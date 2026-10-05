// Command zerolosscheck verifies every line of a replayed file reached OpenSearch.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Longhodac/log-plat/internal/osutil"
	"github.com/Longhodac/log-plat/internal/zeroloss"
)

func main() {
	var (
		file    = flag.String("file", "", "the file the agent tailed, as readable from here (required)")
		source  = flag.String("source", "", "the same file's path as the agent sees it (default: -file)")
		agentID = flag.String("agent-id", "", "the tailing agent's AGENT_ID (required)")
		epoch   = flag.Uint("epoch", 0, "file epoch; 0 unless the file was truncated or rotated")
		addrs   = flag.String("opensearch", "http://localhost:9200", "comma-separated OpenSearch addresses")
		prefix  = flag.String("index-prefix", "logs", "index prefix")
		timeout = flag.Duration("timeout", 10*time.Minute, "overall time limit")
		stall   = flag.Duration("stall", time.Minute, "stop waiting when the count stops growing for this long")
		jsonOut = flag.String("json", "", "also write the report here")
	)
	flag.Parse()
	if *file == "" || *agentID == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *source == "" {
		*source = *file
	}
	logf := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, time.Now().Format("15:04:05")+" "+format+"\n", args...)
	}

	ids, err := zeroloss.ExpectedIDs(*file, *agentID, *source, uint32(*epoch))
	if err != nil {
		fail(err)
	}
	logf("expecting %d lines from %s (source %s, agent %s)", len(ids), *file, *source, *agentID)
	osc, err := osutil.New(strings.Split(*addrs, ","))
	if err != nil {
		fail(err)
	}
	c := zeroloss.Checker{Client: osc, IndexPrefix: *prefix}
	r, err := c.Check(context.Background(), *file, *source, ids, zeroloss.Options{
		Poll: 2 * time.Second, Stall: *stall, Timeout: *timeout, Log: logf,
	})
	if err != nil {
		fail(err)
	}
	out, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(out))
	if *jsonOut != "" {
		if err := os.WriteFile(*jsonOut, append(out, '\n'), 0o644); err != nil { //nolint:gosec // reports are committed and meant to be read
			fail(err)
		}
	}
	if !r.ZeroLoss {
		logf("LOSS DETECTED: %d missing, %d unexpected", r.Missing, r.Unexpected)
		os.Exit(1)
	}
	logf("ZERO LOSS: all %d lines indexed; %.0f lines/s from first read to last index", r.Expected, r.LinesPerSec)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "zerolosscheck:", err)
	os.Exit(1)
}
