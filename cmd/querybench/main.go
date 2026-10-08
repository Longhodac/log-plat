// Command querybench measures query API latency under concurrent load.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/Longhodac/log-plat/internal/querybench"
)

func main() {
	var (
		base     = flag.String("url", "http://localhost:8080", "query API base URL")
		key      = flag.String("key", "dev-query-key", "API key")
		workers  = flag.Int("workers", 8, "concurrent clients")
		duration = flag.Duration("duration", 20*time.Second, "measured time")
		warmup   = flag.Duration("warmup", 3*time.Second, "unrecorded warmup")
		targets  = flag.String("targets", "", "newline-separated request paths, or @file to read them from a file (required)")
		offset   = flag.Int("offset", -1, "index in the targets to start from; -1 picks a random one")
		jsonOut  = flag.String("json", "", "also write the result here")
	)
	flag.Parse()
	list := loadTargets(*targets)
	if len(list) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	if *offset < 0 {
		*offset = rand.IntN(len(list)) //nolint:gosec // a benchmark start point, not a secret
	}
	res, err := querybench.Run(context.Background(), querybench.Config{
		BaseURL: *base, APIKey: *key, Targets: list, Workers: *workers, Duration: *duration, Warmup: *warmup, Offset: *offset,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "querybench:", err)
		os.Exit(1)
	}
	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out))
	if *jsonOut != "" {
		if err := os.WriteFile(*jsonOut, append(out, '\n'), 0o644); err != nil { //nolint:gosec // reports are committed and meant to be read
			fmt.Fprintln(os.Stderr, "querybench:", err)
			os.Exit(1)
		}
	}
}

func loadTargets(spec string) []string {
	if path, ok := strings.CutPrefix(spec, "@"); ok {
		raw, err := os.ReadFile(path) //nolint:gosec // path comes from the operator
		if err != nil {
			fmt.Fprintln(os.Stderr, "querybench:", err)
			os.Exit(1)
		}
		spec = string(raw)
	}
	var out []string
	for _, l := range strings.Split(spec, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}
