package alert

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Longhodac/log-plat/internal/backoff"
)

func spike() Alert {
	return Alert{Service: "hdfs", Kind: Firing, Window: time.Minute, Errors: 47, Usual: 2, Samples: []string{"disk /dev/sda failed", "<!channel> look <https://evil.example|here>"}}
}

func slackFor(url string) *Slack {
	return &Slack{URL: url, Client: http.DefaultClient, MaxAttempts: 4, Backoff: backoff.Policy{Base: time.Millisecond, Max: 5 * time.Millisecond}}
}

func TestFormatEscapesLogContentSoItCannotPingOrLink(t *testing.T) {
	text := FormatSlack(spike())
	for _, bad := range []string{"<!channel>", "<https://", "|here>"} {
		if strings.Contains(text, bad) {
			t.Errorf("message contains raw %q, which Slack would act on:\n%s", bad, text)
		}
	}
	for _, want := range []string{"Error spike in `hdfs`", "47 errors in the last 1m0s", "usually about 2", "&lt;!channel&gt;"} {
		if !strings.Contains(text, want) {
			t.Errorf("message lacks %q:\n%s", want, text)
		}
	}
	if r := FormatSlack(Alert{Service: "hdfs", Kind: Resolved, Window: time.Minute, Errors: 3}); !strings.Contains(r, "back to normal") {
		t.Errorf("resolved message = %q", r)
	}
}

func TestSlackPostsJSONText(t *testing.T) {
	var got map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request = %s %s", r.Method, r.Header.Get("Content-Type"))
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &got)
	}))
	defer srv.Close()
	if err := slackFor(srv.URL).Notify(context.Background(), spike()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got["text"], "Error spike in `hdfs`") {
		t.Errorf("payload = %v", got)
	}
}

func TestSlackRetriesServerErrorsAndHonorsRetryAfter(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.WriteHeader(http.StatusInternalServerError)
		case 2:
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	start := time.Now()
	if err := slackFor(srv.URL).Notify(context.Background(), spike()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Errorf("%d requests, want 3 (500, 429, then success)", calls.Load())
	}
	if time.Since(start) < time.Second {
		t.Errorf("finished in %v; the 429 asked for a one second wait", time.Since(start))
	}
}

func TestSlackDoesNotRetryAPermanentRejection(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound) // a revoked webhook
	}))
	defer srv.Close()
	err := slackFor(srv.URL).Notify(context.Background(), spike())
	if err == nil || calls.Load() != 1 {
		t.Errorf("err = %v after %d calls; want one call and an error", err, calls.Load())
	}
}

func TestSlackGivesUpAfterMaxAttemptsWithoutLeakingTheWebhookURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) }))
	url := srv.URL + "/services/T000/B000/SECRETTOKEN"
	err := slackFor(url).Notify(context.Background(), spike())
	if err == nil || strings.Contains(err.Error(), "SECRETTOKEN") {
		t.Errorf("err = %v; want a failure that does not contain the webhook secret", err)
	}
	srv.Close()
	err = slackFor(url).Notify(context.Background(), spike()) // now the connection itself fails
	if err == nil || strings.Contains(err.Error(), "SECRETTOKEN") || strings.Contains(err.Error(), srv.URL) {
		t.Errorf("transport error leaks the URL: %v", err)
	}
}

func TestSlackStopsWhenTheContextEnds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer srv.Close()
	s := slackFor(srv.URL)
	s.MaxAttempts = 100
	s.Backoff = backoff.Policy{Base: time.Second, Max: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_ = s.Notify(ctx, spike())
	if time.Since(start) > 2*time.Second {
		t.Errorf("Notify kept retrying %v after its context ended", time.Since(start))
	}
}
