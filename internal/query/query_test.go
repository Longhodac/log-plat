package query

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/doc"
)

type fakeSearcher struct {
	got  Query
	page Page
	err  error
}

func (f *fakeSearcher) Search(_ context.Context, q Query) (Page, error) {
	f.got = q
	return f.page, f.err
}

func serve(t *testing.T, s Searcher, target, key string) (*httptest.ResponseRecorder, map[string]APIError) {
	t.Helper()
	keys, _ := apikey.Parse("k:ops")
	srv := &Server{Search: s, Keys: keys, Log: slog.New(slog.DiscardHandler), Timeout: time.Second}
	req := httptest.NewRequest(http.MethodGet, target, nil)
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var body map[string]APIError
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec, body
}

func TestBadParametersGetStructuredErrors(t *testing.T) {
	cases := map[string]string{
		"/v1/logs?level=loud":     "level",
		"/v1/logs?from=yesterday": "from",
		"/v1/logs?from=2008-11-10T00:00:00Z&to=2008-11-09T00:00:00Z": "from",
		"/v1/logs?limit=0":    "limit",
		"/v1/logs?limit=1001": "limit",
		"/v1/logs?cursor=!!!": "cursor",
	}
	for target, field := range cases {
		rec, body := serve(t, &fakeSearcher{}, target, "k")
		if rec.Code != http.StatusBadRequest || body["error"].Code != "invalid_argument" || body["error"].Field != field {
			t.Errorf("%s: %d %+v; want 400 invalid_argument on %s", target, rec.Code, body["error"], field)
		}
	}
}

func TestAuthAndBackendErrors(t *testing.T) {
	rec, body := serve(t, &fakeSearcher{}, "/v1/logs", "")
	if rec.Code != http.StatusUnauthorized || body["error"].Code != "unauthenticated" {
		t.Errorf("no key: %d %+v", rec.Code, body)
	}
	rec, _ = serve(t, &fakeSearcher{}, "/v1/logs", "wrong")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong key: %d", rec.Code)
	}
	rec, body = serve(t, &fakeSearcher{err: errors.New("connection refused")}, "/v1/logs", "k")
	if rec.Code != http.StatusServiceUnavailable || body["error"].Code != "unavailable" {
		t.Errorf("backend down: %d %+v", rec.Code, body)
	}
}

func TestCursorRoundTripsAndIsBoundToFilters(t *testing.T) {
	ts := time.Date(2008, 11, 9, 20, 36, 15, 0, time.UTC)
	f := &fakeSearcher{page: Page{
		Logs: []doc.Doc{{ID: "b", Timestamp: ts}},
		Next: &SortKey{TimestampMillis: ts.UnixMilli(), ID: "b"},
	}}
	rec, _ := serve(t, f, "/v1/logs?service=hdfs&level=INFO&limit=1", "k")
	var resp SearchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.NextCursor == "" {
		t.Fatalf("first page: %d %s", rec.Code, rec.Body)
	}
	if f.got.Level != "info" || f.got.Limit != 1 {
		t.Errorf("parsed query %+v; want normalized level and limit 1", f.got)
	}

	next := "/v1/logs?service=hdfs&level=info&limit=1&cursor=" + url.QueryEscape(resp.NextCursor)
	if rec, _ := serve(t, f, next, "k"); rec.Code != http.StatusOK {
		t.Fatalf("second page: %d %s", rec.Code, rec.Body)
	}
	if f.got.After == nil || *f.got.After != (SortKey{TimestampMillis: ts.UnixMilli(), ID: "b"}) {
		t.Errorf("cursor decoded to %+v", f.got.After)
	}

	other := "/v1/logs?service=apache&cursor=" + url.QueryEscape(resp.NextCursor)
	if rec, body := serve(t, f, other, "k"); rec.Code != http.StatusBadRequest || body["error"].Field != "cursor" {
		t.Errorf("cursor reused with other filters: %d %+v; want 400 on cursor", rec.Code, body)
	}
}

func TestHealthIsUnauthenticated(t *testing.T) {
	if rec, _ := serve(t, &fakeSearcher{}, "/healthz", ""); rec.Code != http.StatusOK {
		t.Errorf("/healthz = %d", rec.Code)
	}
}

type fakeLimiter struct {
	allow bool
	retry time.Duration
	err   error
	keys  []string
}

func (f *fakeLimiter) Allow(_ context.Context, key string, _ int) (bool, time.Duration, error) {
	f.keys = append(f.keys, key)
	return f.allow, f.retry, f.err
}

func serveWith(t *testing.T, s Searcher, lim RateLimiter, target, key string) *httptest.ResponseRecorder {
	t.Helper()
	keys, _ := apikey.Parse("k:ops")
	srv := &Server{Search: s, Limit: lim, Keys: keys, Log: slog.New(slog.DiscardHandler), Timeout: time.Second}
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("X-API-Key", key)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

func TestRateLimitedRequestGets429WithRetryAfterAndSkipsTheSearch(t *testing.T) {
	f := &fakeSearcher{}
	lim := &fakeLimiter{allow: false, retry: 1500 * time.Millisecond}
	rec := serveWith(t, f, lim, "/v1/logs", "k")
	var body map[string]APIError
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if rec.Code != http.StatusTooManyRequests || body["error"].Code != "rate_limited" {
		t.Fatalf("got %d %+v; want 429 rate_limited", rec.Code, body)
	}
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Errorf("Retry-After = %q, want 2 (1.5s rounded up)", got)
	}
	if f.got.Limit != 0 {
		t.Error("a limited request reached the searcher")
	}
	if len(lim.keys) != 1 || lim.keys[0] != "ops" {
		t.Errorf("limiter keyed on %v, want the service name ops and not the API key", lim.keys)
	}
}

func TestUnauthenticatedRequestsDoNotSpendTokens(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	if rec := serveWith(t, &fakeSearcher{}, lim, "/v1/logs", "wrong"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", rec.Code)
	}
	if len(lim.keys) != 0 {
		t.Errorf("limiter was consulted %d times for a bad key", len(lim.keys))
	}
}

func TestLimiterFailureLetsTheRequestThrough(t *testing.T) {
	lim := &fakeLimiter{allow: true, err: errors.New("redis down")}
	if rec := serveWith(t, &fakeSearcher{}, lim, "/v1/logs", "k"); rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200 when only the limiter is broken", rec.Code)
	}
}

type memCache struct {
	mu      sync.Mutex
	m       map[string][]byte
	getErr  error
	setErr  error
	sets    int
	lastTTL time.Duration
}

func (c *memCache) Get(_ context.Context, k string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, false, c.getErr
	}
	v, ok := c.m[k]
	return v, ok, nil
}

func (c *memCache) Set(_ context.Context, k string, v []byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sets++
	c.lastTTL = ttl
	if c.setErr != nil {
		return c.setErr
	}
	if c.m == nil {
		c.m = map[string][]byte{}
	}
	c.m[k] = v
	return nil
}

type countingSearcher struct {
	calls atomic.Int32
	gate  chan struct{}
	page  Page
}

func (s *countingSearcher) Search(context.Context, Query) (Page, error) {
	s.calls.Add(1)
	if s.gate != nil {
		<-s.gate
	}
	return s.page, nil
}

func cachedFor(next Searcher, c ByteCache) *Cached {
	return &Cached{Next: next, Cache: c, TTL: 30 * time.Second, Log: slog.New(slog.DiscardHandler)}
}

func TestRepeatedSearchIsServedFromCache(t *testing.T) {
	ts := time.Date(2008, 11, 9, 20, 36, 15, 0, time.UTC)
	s := &countingSearcher{page: Page{Logs: []doc.Doc{{ID: "a", Timestamp: ts, Message: "m"}}, Next: &SortKey{TimestampMillis: 1, ID: "a"}}}
	mc := &memCache{}
	c := cachedFor(s, mc)
	q := Query{Service: "hdfs", Limit: 10}

	first, err := c.Search(context.Background(), q)
	if err != nil || first.Cached {
		t.Fatalf("first search = cached %v, err %v; want a miss", first.Cached, err)
	}
	second, err := c.Search(context.Background(), q)
	if err != nil || !second.Cached {
		t.Fatalf("second search = cached %v, err %v; want a hit", second.Cached, err)
	}
	if s.calls.Load() != 1 {
		t.Errorf("searcher ran %d times for 2 identical queries, want 1", s.calls.Load())
	}
	if len(second.Logs) != 1 || second.Logs[0].ID != "a" || second.Next == nil || second.Next.ID != "a" {
		t.Errorf("cached page lost data: %+v", second)
	}
	if mc.lastTTL != 30*time.Second {
		t.Errorf("entry stored with TTL %v, want 30s", mc.lastTTL)
	}
}

func TestCacheKeyCoversEveryFilterTheLimitAndTheCursor(t *testing.T) {
	base := Query{Service: "hdfs", Level: "info", Text: "x", Limit: 10}
	seen := map[string]string{Key(base): "base"}
	for name, q := range map[string]Query{
		"service": {Service: "apache", Level: "info", Text: "x", Limit: 10},
		"level":   {Service: "hdfs", Level: "warn", Text: "x", Limit: 10},
		"text":    {Service: "hdfs", Level: "info", Text: "y", Limit: 10},
		"limit":   {Service: "hdfs", Level: "info", Text: "x", Limit: 11},
		"from":    {Service: "hdfs", Level: "info", Text: "x", Limit: 10, From: time.Unix(1, 0)},
		"to":      {Service: "hdfs", Level: "info", Text: "x", Limit: 10, To: time.Unix(1, 0)},
		"cursor":  {Service: "hdfs", Level: "info", Text: "x", Limit: 10, After: &SortKey{TimestampMillis: 5, ID: "z"}},
	} {
		k := Key(q)
		if prev, dup := seen[k]; dup {
			t.Errorf("changing %s gave the same key as %s", name, prev)
		}
		seen[k] = name
	}
	first, second := Key(base), Key(Query{Service: "hdfs", Level: "info", Text: "x", Limit: 10})
	if first != second {
		t.Error("Key is not deterministic")
	}
}

func TestCacheFailuresFallBackToSearching(t *testing.T) {
	s := &countingSearcher{page: Page{Logs: []doc.Doc{{ID: "a"}}}}
	c := cachedFor(s, &memCache{getErr: errors.New("down"), setErr: errors.New("down")})
	p, err := c.Search(context.Background(), Query{Limit: 5})
	if err != nil || len(p.Logs) != 1 || p.Cached {
		t.Fatalf("got %+v, %v; want the search result despite a dead cache", p, err)
	}
}

func TestConcurrentIdenticalSearchesShareOneBackendCall(t *testing.T) {
	s := &countingSearcher{gate: make(chan struct{}), page: Page{Logs: []doc.Doc{{ID: "a"}}}}
	c := cachedFor(s, &memCache{})
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Search(context.Background(), Query{Limit: 5}); err != nil {
				t.Error(err)
			}
		}()
	}
	time.Sleep(200 * time.Millisecond) // let all 20 arrive while the first search is blocked
	close(s.gate)
	wg.Wait()
	if n := s.calls.Load(); n != 1 {
		t.Errorf("20 simultaneous identical searches ran the backend %d times, want 1", n)
	}
}

func TestHandlerReportsCacheHitAndMiss(t *testing.T) {
	s := &countingSearcher{page: Page{Logs: []doc.Doc{{ID: "a"}}}}
	c := cachedFor(s, &memCache{})
	if got := serveWith(t, c, nil, "/v1/logs?service=hdfs", "k").Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("first request X-Cache = %q, want MISS", got)
	}
	if got := serveWith(t, c, nil, "/v1/logs?service=hdfs", "k").Header().Get("X-Cache"); got != "HIT" {
		t.Errorf("second request X-Cache = %q, want HIT", got)
	}
}
