//go:build chaos

// Package chaos breaks a running Compose stack on purpose and checks that no
// log line is lost. It needs the stack from `make chaos-up`.
package chaos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/Longhodac/log-plat/internal/osutil"
	"github.com/Longhodac/log-plat/internal/zeroloss"
)

// Ports published by compose.yaml and compose.chaos.yaml.
const (
	toxiproxyAddr = "localhost:8474"
	openSearchURL = "http://localhost:9200"
	agentID       = "agent-1"
	sourceDir     = "/var/log/ingest" // where the agent container sees data/run
)

var adminPort = map[string]int{"collector": 9101, "indexer": 9102, "agent": 9103}

// Env is the running stack plus the handles tests use to poke it.
type Env struct {
	root    string
	runID   string
	tox     *toxiproxy.Client
	os      *opensearchapi.Client
	checker zeroloss.Checker
}

func newEnv(t *testing.T) *Env {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	osc, err := osutil.New([]string{openSearchURL})
	if err != nil {
		t.Fatal(err)
	}
	e := &Env{
		root:    root,
		runID:   time.Now().UTC().Format("20060102T150405Z"),
		tox:     toxiproxy.NewClient(toxiproxyAddr),
		os:      osc,
		checker: zeroloss.Checker{Client: osc, IndexPrefix: "logs"},
	}
	if _, err := e.tox.Version(); err != nil {
		t.Fatalf("cannot reach Toxiproxy at %s (%v); start the stack with `make chaos-up`", toxiproxyAddr, err)
	}
	return e
}

func (e *Env) compose(ctx context.Context, args ...string) error {
	full := append([]string{"compose", "-f", "compose.yaml", "-f", "compose.chaos.yaml"}, args...)
	cmd := exec.CommandContext(ctx, "docker", full...) //nolint:gosec // arguments come from the test scenarios, not from input
	cmd.Dir = e.root
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("docker %s: %w\n%s", strings.Join(full, " "), err, out)
	}
	return nil
}

// kill sends SIGKILL, so the process gets no chance to shut down cleanly.
func (e *Env) kill(ctx context.Context, svc string) error {
	return e.compose(ctx, "kill", "-s", "SIGKILL", svc)
}

// start restarts a killed container and waits until it reports ready.
func (e *Env) start(ctx context.Context, svc string) error {
	if err := e.compose(ctx, "start", svc); err != nil {
		return err
	}
	return e.waitReady(ctx, svc)
}

func httpOK(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

func (e *Env) ready(ctx context.Context, svc string) bool {
	switch svc {
	case "collector", "indexer", "agent":
		return httpOK(ctx, fmt.Sprintf("http://localhost:%d/readyz", adminPort[svc]))
	case "opensearch":
		return httpOK(ctx, openSearchURL+"/_cluster/health?wait_for_status=yellow&timeout=1s")
	case "kafka":
		out, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.Health.Status}}", "logplat-kafka-1").Output()
		return err == nil && strings.TrimSpace(string(out)) == "healthy"
	case "toxiproxy":
		_, err := e.tox.Version()
		return err == nil
	}
	return false
}

func (e *Env) waitReady(ctx context.Context, svc string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	for !e.ready(ctx, svc) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s did not become ready: %w", svc, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil
}

// waitStackReady blocks until every service is up, so one scenario's damage
// cannot leak into the next.
func (e *Env) waitStackReady(ctx context.Context) error {
	for _, svc := range []string{"toxiproxy", "kafka", "opensearch", "collector", "indexer", "agent"} {
		if err := e.waitReady(ctx, svc); err != nil {
			return err
		}
	}
	return nil
}

// metric sums every series of name on a service's admin port. labelSub, if
// set, keeps only series whose label text contains it.
func (e *Env) metric(ctx context.Context, svc, name, labelSub string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://localhost:%d/metrics", adminPort[svc]), nil)
	if err != nil {
		return 0, err
	}
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var sum float64
	var found bool
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, name) || strings.HasPrefix(line, "#") {
			continue
		}
		rest := line[len(name):]
		if rest == "" || (rest[0] != ' ' && rest[0] != '{') {
			continue // a longer metric name sharing the prefix
		}
		if labelSub != "" && !strings.Contains(rest, labelSub) {
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			return 0, err
		}
		sum += v
		found = true
	}
	if !found {
		return 0, errors.New("metric " + name + " not found")
	}
	return sum, nil
}

// countLines returns how many complete lines the file holds right now.
func countLines(path string) (int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return bytes.Count(raw, []byte{'\n'}), nil
}

// indexOps is how many index operations the logs-* primaries have performed.
// An entry written twice counts twice here but is one document, so the delta
// over a run minus the number of lines is the number of redundant writes
// OpenSearch absorbed. It resets if OpenSearch restarts.
func (e *Env) indexOps(ctx context.Context) (int64, error) {
	var out struct {
		All struct {
			Primaries struct {
				Indexing struct {
					IndexTotal int64 `json:"index_total"`
				} `json:"indexing"`
			} `json:"primaries"`
		} `json:"_all"`
	}
	err := osutil.Do(ctx, e.os, http.MethodGet, "/logs-*/_stats/indexing", nil, &out)
	return out.All.Primaries.Indexing.IndexTotal, err
}

// waitForUnackedWrite returns at a moment when OpenSearch has performed a write
// that the indexer has not yet heard back about. It relies on responses being
// delayed, so a landed write and an unchanged bulk counter mean the indexer is
// still waiting on that request and has committed nothing for it.
func (e *Env) waitForUnackedWrite(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	bulks, err := e.metric(ctx, "indexer", "logplat_indexer_bulk_seconds_count", "")
	if err != nil {
		return err
	}
	ops, err := e.indexOps(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return errors.New("never saw a write land while the indexer was waiting on its response")
		case <-time.After(25 * time.Millisecond):
		}
		now, err := e.indexOps(ctx)
		if err != nil {
			return err
		}
		if now == ops {
			continue
		}
		b, err := e.metric(ctx, "indexer", "logplat_indexer_bulk_seconds_count", "")
		if err != nil {
			return err
		}
		if b == bulks {
			return nil
		}
		bulks, ops = b, now // that write belonged to a request already answered
	}
}
