package querybench

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPercentileUsesNearestRank(t *testing.T) {
	v := make([]float64, 100)
	for i := range v {
		v[i] = float64(i + 1)
	}
	for p, want := range map[float64]float64{50: 50, 90: 90, 99: 99, 100: 100} {
		if got := pct(v, p); got != want {
			t.Errorf("pct(%v) = %v, want %v", p, got, want)
		}
	}
	if pct(nil, 50) != 0 {
		t.Error("pct of nothing should be 0")
	}
}

func TestRunCountsStatusesCacheHeadersAndSkipsWarmup(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "k" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		time.Sleep(5 * time.Millisecond)
		if n.Add(1)%2 == 0 {
			w.Header().Set("X-Cache", "HIT")
		} else {
			w.Header().Set("X-Cache", "MISS")
		}
		_, _ = w.Write([]byte(`{"logs":[{"id":"abcdefghijklmnop"}]}`))
	}))
	defer srv.Close()

	res, err := Run(context.Background(), Config{
		BaseURL: srv.URL, APIKey: "k", Targets: []string{"/a", "/b"}, Workers: 4,
		Duration: 400 * time.Millisecond, Warmup: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Requests == 0 || res.Status[200] != res.Requests || res.Errors != 0 || res.BadBodies != 0 {
		t.Fatalf("result %+v; want only 200s, no errors, no empty bodies", res)
	}
	if res.CacheHits == 0 || res.CacheMiss == 0 {
		t.Errorf("hits=%d misses=%d, want both counted", res.CacheHits, res.CacheMiss)
	}
	if int(n.Load()) <= res.Requests {
		t.Errorf("server saw %d requests but %d were recorded; warmup requests should be sent and not recorded", n.Load(), res.Requests)
	}
	if res.P50 < 5 || res.P99 < res.P50 {
		t.Errorf("percentiles p50=%v p99=%v look wrong for a 5ms handler", res.P50, res.P99)
	}

	bad, _ := Run(context.Background(), Config{BaseURL: srv.URL, APIKey: "wrong", Targets: []string{"/a"}, Workers: 1, Duration: 100 * time.Millisecond})
	if bad.Status[401] == 0 || bad.Status[200] != 0 {
		t.Errorf("wrong key result %+v, want 401s counted", bad.Status)
	}
}

func TestConcurrentWorkersAskDifferentQuestions(t *testing.T) {
	var mu sync.Mutex
	var order []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		order = append(order, r.URL.Path)
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte(`{"logs":[{"id":"abcdefghijklmnop"}]}`))
	}))
	defer srv.Close()
	var targets []string
	for i := range 400 {
		targets = append(targets, "/q"+strconv.Itoa(i))
	}
	if _, err := Run(context.Background(), Config{
		BaseURL: srv.URL, APIKey: "k", Targets: targets, Workers: 8, Duration: 300 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	seen := map[string]bool{}
	for _, p := range order[:min(len(order), 100)] {
		if seen[p] {
			t.Fatalf("%s was requested twice among the first %d requests; workers overlap instead of striding", p, min(len(order), 100))
		}
		seen[p] = true
	}
}
