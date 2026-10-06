// Command queryapi serves log search over HTTP.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/cache"
	"github.com/Longhodac/log-plat/internal/envcfg"
	"github.com/Longhodac/log-plat/internal/obs"
	"github.com/Longhodac/log-plat/internal/osutil"
	"github.com/Longhodac/log-plat/internal/query"
	"github.com/Longhodac/log-plat/internal/ratelimit"
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

		redisAddr = env.String("REDIS_ADDR", "localhost:6379")
		// Requests per second each API key may make. 0 turns limiting off.
		rate  = env.Float("QUERY_RATE_LIMIT", 0)
		burst = env.Int("QUERY_RATE_BURST", 20)
		// How long a search result may be served from the cache. 0 turns caching off.
		cacheTTL = env.Duration("QUERY_CACHE_TTL", 0)
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
	var searcher query.Searcher = query.OpenSearch{Client: osc, IndexPrefix: prefix}
	var limiter query.RateLimiter
	if rate > 0 || cacheTTL > 0 {
		rdb := cache.NewClient(redisAddr)
		defer rdb.Close()
		if cacheTTL > 0 {
			searcher = &query.Cached{Next: searcher, Cache: cache.Redis{Client: rdb, Prefix: "logplat:qc:"}, TTL: cacheTTL, Log: log}
			log.Info("search cache on", "ttl", cacheTTL, "redis", redisAddr)
		}
		if rate > 0 {
			l, err := ratelimit.New(rdb, "query", rate, burst)
			if err != nil {
				return err
			}
			limiter = l
			log.Info("rate limiting on", "requests_per_second", rate, "burst", burst, "redis", redisAddr)
		}
	}
	srv := &query.Server{
		Search:  searcher,
		Limit:   limiter,
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
