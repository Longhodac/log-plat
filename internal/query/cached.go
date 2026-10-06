package query

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/singleflight"
)

var cacheResults = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "logplat_query_cache_total",
	Help: "Search cache lookups by result: hit, miss, or error (Redis failed and the search ran uncached).",
}, []string{"result"})

// ByteCache stores serialized pages.
type ByteCache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

// Cached answers repeated searches from a cache for up to TTL.
//
// Logs keep arriving, so a cached page can miss lines indexed within the last
// TTL. That staleness is the price of the cache, and the TTL bounds it. The key
// covers every filter, the limit, and the cursor, so a cursor from one filter
// set can never serve another.
type Cached struct {
	Next  Searcher
	Cache ByteCache
	TTL   time.Duration
	Log   *slog.Logger

	inflight singleflight.Group
}

// Key identifies a query's result page.
func Key(q Query) string {
	cursor := ""
	if q.After != nil {
		cursor = strconv.FormatInt(q.After.TimestampMillis, 10) + "/" + q.After.ID
	}
	h := sha256.Sum256([]byte(q.fingerprint() + "\x00" + strconv.Itoa(q.Limit) + "\x00" + cursor))
	return hex.EncodeToString(h[:])
}

// Search implements Searcher. Identical searches that arrive while one is
// running share its result instead of each querying OpenSearch.
func (c *Cached) Search(ctx context.Context, q Query) (Page, error) {
	key := Key(q)
	if raw, ok, err := c.Cache.Get(ctx, key); err != nil {
		cacheResults.WithLabelValues("error").Inc()
		c.Log.Warn("search cache read failed; searching without it", "error", err)
	} else if ok {
		var p Page
		if err := json.Unmarshal(raw, &p); err == nil {
			cacheResults.WithLabelValues("hit").Inc()
			p.Cached = true
			return p, nil
		}
	}

	v, err, _ := c.inflight.Do(key, func() (any, error) {
		// A caller that gives up must not cancel the search other callers share.
		p, err := c.Next.Search(context.WithoutCancel(ctx), q)
		if err != nil {
			return nil, err
		}
		if raw, err := json.Marshal(p); err == nil {
			if err := c.Cache.Set(context.WithoutCancel(ctx), key, raw, c.TTL); err != nil {
				cacheResults.WithLabelValues("error").Inc()
				c.Log.Warn("search cache write failed", "error", err)
			}
		}
		return p, nil
	})
	if err != nil {
		return Page{}, err
	}
	cacheResults.WithLabelValues("miss").Inc()
	return v.(Page), nil
}
