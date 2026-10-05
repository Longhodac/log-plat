package query

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
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
