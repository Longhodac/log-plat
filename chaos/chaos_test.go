//go:build chaos

package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Longhodac/log-plat/internal/loggen"
	"github.com/Longhodac/log-plat/internal/zeroloss"
)

// Result is what one scenario leaves in results/chaos/<run>/<scenario>.json.
type Result struct {
	Scenario string `json:"scenario"`
	Lines    int    `json:"lines"`
	Rate     int    `json:"lines_per_sec_written"`

	// Where the pipeline stood when the fault started and just before it healed.
	WrittenAtInject int `json:"written_at_inject"`
	IndexedAtInject int `json:"indexed_at_inject"`
	WrittenAtHeal   int `json:"written_at_heal"`
	IndexedAtHeal   int `json:"indexed_at_heal"`
	// Share of the lines written during the fault that were indexed during it.
	// A hard outage should leave this near 0.
	IndexedShareDuringFault float64 `json:"indexed_share_during_fault"`

	FaultSeconds    float64 `json:"fault_seconds"`
	HealSeconds     float64 `json:"heal_seconds"`
	DrainSeconds    float64 `json:"seconds_from_last_write_to_all_indexed"`
	IndexerIndexed  float64 `json:"indexer_indexed_counter_delta"`
	IndexerResetted bool    `json:"indexer_counter_reset_by_restart"`
	// Index operations OpenSearch performed minus the lines written. Above zero
	// means entries were delivered more than once and the document ID collapsed
	// them. Unknown (nil) when OpenSearch restarted and its counter reset.
	RedundantWrites *int64 `json:"redundant_writes_absorbed"`

	Check zeroloss.Report `json:"zero_loss_report"`
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}

func TestChaos(t *testing.T) {
	env := newEnv(t)
	input := filepath.Join(env.root, "data/loghub/HDFS.log")
	if _, err := os.Stat(input); err != nil {
		t.Fatalf("missing %s; run `make data`", input)
	}
	outDir := filepath.Join(env.root, "results", "chaos", env.runID)
	if err := os.MkdirAll(outDir, 0o755); err != nil { //nolint:gosec // reports are committed and meant to be read
		t.Fatal(err)
	}
	t.Logf("run %s; reports go to %s", env.runID, outDir)

	for _, sc := range scenarios() {
		t.Run(sc.name, func(t *testing.T) {
			res := runScenario(t, env, input, sc)
			raw, _ := json.MarshalIndent(res, "", "  ")
			if err := os.WriteFile(filepath.Join(outDir, sc.name+".json"), append(raw, '\n'), 0o644); err != nil { //nolint:gosec // reports are committed and meant to be read
				t.Error(err)
			}
		})
	}
}

func runScenario(t *testing.T, env *Env, input string, sc scenario) Result {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	if err := env.tox.ResetState(); err != nil {
		t.Fatal(err)
	}
	if err := env.waitStackReady(ctx); err != nil {
		t.Fatalf("stack not healthy before the scenario: %v", err)
	}

	lines, rate := envInt("CHAOS_LINES", 60000), envInt("CHAOS_RATE", 3000)
	injectAfter := 4 * time.Second
	name := fmt.Sprintf("chaos-%s-%s.log", sc.name, env.runID)
	hostPath := filepath.Join(env.root, "data/run", name)
	source := sourceDir + "/" + name
	res := Result{Scenario: sc.name, Lines: lines, Rate: rate}

	indexedBefore, _ := env.metric(ctx, "indexer", "logplat_indexer_records_total", `result="indexed"`)
	opsBefore, err := env.indexOps(ctx)
	if err != nil {
		t.Fatalf("index stats: %v", err)
	}

	type genDone struct {
		st  loggen.Stats
		err error
	}
	gen := make(chan genDone, 1)
	genStart := time.Now()
	go func() {
		st, err := loggen.Run(ctx, loggen.Config{In: input, Out: hostPath, Rate: rate, Count: lines})
		gen <- genDone{st, err}
	}()

	time.Sleep(injectAfter)
	res.WrittenAtInject, _ = countLines(hostPath)
	res.IndexedAtInject, _ = env.checker.Count(ctx, source)

	faultStart := time.Now()
	t.Logf("injecting %s (%d lines written, %d indexed)", sc.name, res.WrittenAtInject, res.IndexedAtInject)
	if err := sc.inject(ctx, env); err != nil {
		t.Fatalf("inject: %v", err)
	}
	time.Sleep(sc.hold)

	res.WrittenAtHeal, _ = countLines(hostPath)
	indexed, err := env.checker.Count(ctx, source)
	switch {
	case err == nil:
		res.IndexedAtHeal = indexed
	case sc.osDown:
		res.IndexedAtHeal = res.IndexedAtInject // nothing can be indexed into a dead cluster
	default:
		t.Fatalf("count during fault: %v", err)
	}
	res.FaultSeconds = time.Since(faultStart).Seconds()
	if w := res.WrittenAtHeal - res.WrittenAtInject; w > 0 {
		res.IndexedShareDuringFault = float64(res.IndexedAtHeal-res.IndexedAtInject) / float64(w)
	}
	t.Logf("healing %s (%d written, %d indexed; %.0f%% of the fault-window lines got through)",
		sc.name, res.WrittenAtHeal, res.IndexedAtHeal, 100*res.IndexedShareDuringFault)

	healStart := time.Now()
	if err := sc.heal(ctx, env); err != nil {
		t.Fatalf("heal: %v", err)
	}
	if err := env.waitStackReady(ctx); err != nil {
		t.Fatalf("stack did not recover: %v", err)
	}
	res.HealSeconds = time.Since(healStart).Seconds()

	g := <-gen
	if g.err != nil || g.st.Lines != lines {
		t.Fatalf("loggen wrote %d of %d lines: %v", g.st.Lines, lines, g.err)
	}
	t.Logf("loggen finished %d lines in %s", g.st.Lines, time.Since(genStart).Round(time.Millisecond))
	lastWrite := time.Now()

	if sc.mustStall && res.IndexedShareDuringFault > 0.5 {
		t.Errorf("%s did not stall the pipeline: %.0f%% of fault-window lines were indexed during the fault; the fault may not have taken effect",
			sc.name, 100*res.IndexedShareDuringFault)
	}

	ids, err := zeroloss.ExpectedIDs(hostPath, agentID, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != lines {
		t.Fatalf("source file has %d lines, expected %d", len(ids), lines)
	}
	res.Check, err = env.checker.Check(ctx, hostPath, source, ids, zeroloss.Options{
		Poll: 2 * time.Second, Stall: 2 * time.Minute, Timeout: 10 * time.Minute, Log: t.Logf,
	})
	if err != nil {
		t.Fatalf("checker: %v", err)
	}
	res.DrainSeconds = time.Since(lastWrite).Seconds()

	indexedAfter, _ := env.metric(ctx, "indexer", "logplat_indexer_records_total", `result="indexed"`)
	res.IndexerIndexed = indexedAfter - indexedBefore
	if indexedAfter < indexedBefore {
		res.IndexerResetted, res.IndexerIndexed = true, indexedAfter
	}

	if opsAfter, err := env.indexOps(ctx); err == nil && opsAfter >= opsBefore {
		redundant := opsAfter - opsBefore - int64(lines)
		res.RedundantWrites = &redundant
	}

	if sc.wantRedundant && (res.RedundantWrites == nil || *res.RedundantWrites <= 0) {
		t.Errorf("%s was meant to force redelivery but OpenSearch absorbed no redundant writes; the fault missed its window", sc.name)
	}
	if !res.Check.ZeroLoss {
		t.Errorf("LOSS: %d missing, %d unexpected (sample %v)", res.Check.Missing, res.Check.Unexpected, res.Check.MissingSample)
	}
	redundant := "unknown (opensearch restarted)"
	if res.RedundantWrites != nil {
		redundant = strconv.FormatInt(*res.RedundantWrites, 10)
	}
	t.Logf("zero loss=%v, all %d lines indexed %.1fs after the last write; redundant writes absorbed=%s",
		res.Check.ZeroLoss, lines, res.DrainSeconds, redundant)
	return res
}
