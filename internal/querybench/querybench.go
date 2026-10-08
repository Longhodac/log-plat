// Package querybench measures query API latency under concurrent load.
package querybench

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Config describes one load run.
type Config struct {
	BaseURL  string
	APIKey   string
	Targets  []string      // request paths with query strings; each worker cycles through them
	Workers  int           // concurrent clients
	Duration time.Duration // how long to send
	Warmup   time.Duration // requests in this window are sent but not recorded
	// Offset is where in Targets the run starts. Starting runs at different
	// offsets keeps one run from hitting entries another run left in a cache.
	Offset int
}

// Result summarizes a run. Latencies are in milliseconds.
type Result struct {
	Requests  int         `json:"requests"`
	Seconds   float64     `json:"seconds"`
	RPS       float64     `json:"requests_per_sec"`
	P50       float64     `json:"p50_ms"`
	P90       float64     `json:"p90_ms"`
	P99       float64     `json:"p99_ms"`
	Max       float64     `json:"max_ms"`
	Status    map[int]int `json:"status_counts"`
	CacheHits int         `json:"cache_hit_responses"`
	CacheMiss int         `json:"cache_miss_responses"`
	Errors    int         `json:"transport_errors"`
	BadBodies int         `json:"empty_or_short_bodies"`
	// Latency of responses the API labeled X-Cache HIT and MISS, so cached and
	// uncached work can be told apart inside one run.
	HitP50     float64        `json:"hit_p50_ms,omitempty"`
	MissP50    float64        `json:"miss_p50_ms,omitempty"`
	MissP99    float64        `json:"miss_p99_ms,omitempty"`
	Workers    int            `json:"workers"`
	DistinctQs int            `json:"distinct_queries"`
	Extra      map[string]any `json:"extra,omitempty"`
}

type sample struct {
	ms    float64
	code  int
	hit   bool
	miss  bool
	bytes int
}

// Run sends requests from Workers goroutines for Duration and records latency.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Workers < 1 || len(cfg.Targets) == 0 {
		return Result{}, fmt.Errorf("querybench: need workers >= 1 and at least one target")
	}
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: cfg.Workers, MaxConnsPerHost: cfg.Workers},
	}
	defer client.CloseIdleConnections()

	start := time.Now()
	recordFrom := start.Add(cfg.Warmup)
	end := start.Add(cfg.Warmup + cfg.Duration)
	var (
		mu       sync.Mutex
		samples  []sample
		errCount atomic.Int64
		wg       sync.WaitGroup
	)
	for w := range cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]sample, 0, 4096)
			// Workers stride through the targets, so at any moment they are asking
			// different questions. Stepping by one would have them all ask nearly
			// the same thing at once, which a cache or singleflight collapses.
			for i := w + cfg.Offset; time.Now().Before(end) && ctx.Err() == nil; i += cfg.Workers {
				target := cfg.Targets[i%len(cfg.Targets)]
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.BaseURL+target, nil)
				if err != nil {
					errCount.Add(1)
					continue
				}
				req.Header.Set("X-API-Key", cfg.APIKey)
				t0 := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					errCount.Add(1)
					continue
				}
				n, _ := io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if t0.Before(recordFrom) {
					continue
				}
				xc := resp.Header.Get("X-Cache")
				local = append(local, sample{
					ms: float64(time.Since(t0).Microseconds()) / 1000, code: resp.StatusCode,
					hit: xc == "HIT", miss: xc == "MISS", bytes: int(n),
				})
			}
			mu.Lock()
			samples = append(samples, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(recordFrom).Seconds()

	res := Result{Status: map[int]int{}, Workers: cfg.Workers, DistinctQs: len(cfg.Targets), Errors: int(errCount.Load())}
	lat := make([]float64, 0, len(samples))
	var hitLat, missLat []float64
	for _, s := range samples {
		if s.hit {
			hitLat = append(hitLat, s.ms)
		}
		if s.miss {
			missLat = append(missLat, s.ms)
		}
		res.Status[s.code]++
		if s.hit {
			res.CacheHits++
		}
		if s.miss {
			res.CacheMiss++
		}
		if s.code == http.StatusOK && s.bytes < 20 {
			res.BadBodies++
		}
		lat = append(lat, s.ms)
	}
	res.Requests = len(samples)
	res.Seconds = elapsed
	if elapsed > 0 {
		res.RPS = float64(len(samples)) / elapsed
	}
	slices.Sort(lat)
	slices.Sort(hitLat)
	slices.Sort(missLat)
	res.HitP50, res.MissP50, res.MissP99 = pct(hitLat, 50), pct(missLat, 50), pct(missLat, 99)
	res.P50, res.P90, res.P99 = pct(lat, 50), pct(lat, 90), pct(lat, 99)
	if len(lat) > 0 {
		res.Max = lat[len(lat)-1]
	}
	return res, nil
}

// pct is the nearest-rank percentile of sorted values.
func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(float64(len(sorted))*p/100+0.999999) - 1
	return sorted[min(max(rank, 0), len(sorted)-1)]
}
