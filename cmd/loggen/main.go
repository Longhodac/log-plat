// Command loggen replays a log file into an output file at a fixed rate.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Longhodac/log-plat/internal/loggen"
)

func main() {
	var cfg loggen.Config
	flag.StringVar(&cfg.In, "in", "", "input log file to replay (required)")
	flag.StringVar(&cfg.Out, "out", "", "output file to append to (required)")
	flag.IntVar(&cfg.Rate, "rate", 1000, "lines per second; 0 writes as fast as possible")
	flag.IntVar(&cfg.Count, "count", 0, "lines to write, looping the input if needed; 0 means one pass")
	flag.Parse()
	if cfg.In == "" || cfg.Out == "" {
		flag.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	st, err := loggen.Run(ctx, cfg)
	fmt.Fprintln(os.Stderr, st)
	_ = json.NewEncoder(os.Stdout).Encode(st)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loggen:", err)
		os.Exit(1)
	}
}
