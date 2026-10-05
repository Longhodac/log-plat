// Command queryapi serves log search over HTTP.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/envcfg"
	"github.com/Longhodac/log-plat/internal/obs"
	"github.com/Longhodac/log-plat/internal/osutil"
	"github.com/Longhodac/log-plat/internal/query"
)

func main() { obs.Main("query-api", run) }

func run(ctx context.Context, log *slog.Logger) error {
	var env envcfg.Reader
	var (
		listen  = env.String("QUERY_LISTEN_ADDR", ":8080")
		osAddrs = env.List("OPENSEARCH_ADDRS", []string{"http://localhost:9200"})
		prefix  = env.String("INDEX_PREFIX", "logs")
		keySpec = env.Required("QUERY_API_KEYS")
		timeout = env.Duration("QUERY_TIMEOUT", 10*time.Second)
		grace   = env.Duration("SHUTDOWN_GRACE", 10*time.Second)
	)
	if err := env.Err(); err != nil {
		return err
	}
	keys, err := apikey.Parse(keySpec)
	if err != nil {
		return err
	}
	osc, err := osutil.New(osAddrs)
	if err != nil {
		return err
	}
	srv := &query.Server{
		Search:  query.OpenSearch{Client: osc, IndexPrefix: prefix},
		Keys:    keys,
		Ready:   func(ctx context.Context) error { return osutil.Ready(ctx, osc) },
		Log:     log,
		Timeout: timeout,
	}
	log.Info("http listening", "addr", listen)
	return obs.Serve(ctx, &http.Server{
		Addr:              listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      timeout + 5*time.Second,
	}, grace)
}
