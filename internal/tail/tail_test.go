package tail

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Longhodac/log-plat/internal/apikey"
)

func ev(id, svc, level, msg string) Event {
	return Event{ID: id, Service: svc, Level: level, Message: msg, Host: "h1", Source: "/f", Timestamp: time.Unix(1, 0).UTC()}
}

func TestFilterMatching(t *testing.T) {
	f, err := ParseFilter(url.Values{"service": {"hdfs, apache"}, "level": {"WARN,error"}, "q": {" Timeout "}, "host": {"h1"}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		ev   Event
		want bool
	}{
		{"all match", ev("1", "hdfs", "error", "Connection TIMEOUT after 5s"), true},
		{"other service", ev("2", "nginx", "error", "timeout"), false},
		{"other level", ev("3", "hdfs", "info", "timeout"), false},
		{"text missing", ev("4", "apache", "warn", "all good"), false},
		{"other host", Event{ID: "5", Service: "hdfs", Level: "error", Message: "timeout", Host: "h2"}, false},
	}
	for _, c := range cases {
		if got := f.Match(c.ev); got != c.want {
			t.Errorf("%s: Match = %v, want %v", c.name, got, c.want)
		}
	}
	if !(Filter{}).Match(ev("6", "anything", "info", "x")) {
		t.Error("an empty filter should match everything")
	}
	if _, err := ParseFilter(url.Values{"level": {"loud"}}); err == nil {
		t.Error("an unknown level was accepted")
	}
}

func TestSlowClientLosesEventsAndFastClientDoesNot(t *testing.T) {
	hub := NewHub(10)
	slow, _ := hub.Subscribe(Filter{}, 3)
	fast, _ := hub.Subscribe(Filter{}, 100)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 50 {
			hub.Publish(ev(string(rune('a'+i%26)), "s", "info", "m"))
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a client that is not reading")
	}
	if len(fast.C) != 50 {
		t.Errorf("fast client buffered %d events, want all 50", len(fast.C))
	}
	if len(slow.C) != 3 || slow.TakeDropped() != 47 {
		t.Errorf("slow client kept %d and lost a different number; want 3 kept and 47 dropped", len(slow.C))
	}
	if slow.TakeDropped() != 0 {
		t.Error("TakeDropped did not reset the counter")
	}
}

func TestHubEnforcesClientLimitAndFreesSlots(t *testing.T) {
	hub := NewHub(2)
	a, _ := hub.Subscribe(Filter{}, 1)
	if _, err := hub.Subscribe(Filter{}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Subscribe(Filter{}, 1); !errors.Is(err, ErrTooManyClients) {
		t.Fatalf("third subscribe err = %v, want ErrTooManyClients", err)
	}
	hub.Unsubscribe(a)
	hub.Unsubscribe(a)
	if _, err := hub.Subscribe(Filter{}, 1); err != nil {
		t.Errorf("a freed slot was not reusable: %v", err)
	}
}

func newServer(t *testing.T, hub *Hub) *httptest.Server {
	t.Helper()
	keys, _ := apikey.Parse("k:ops")
	srv := httptest.NewServer((&Server{Hub: hub, Keys: keys, Log: slog.New(slog.DiscardHandler), Buffer: 2, Heartbeat: 100 * time.Millisecond}).Handler())
	t.Cleanup(srv.Close)
	return srv
}

// connect opens a stream. The response is closed when the test ends.
func connect(t *testing.T, srv *httptest.Server, query string) (*bufio.Reader, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/tail?"+query, nil)
	req.Header.Set("X-API-Key", "k")
	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed in t.Cleanup below
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); resp.Body.Close() })
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	return bufio.NewReader(resp.Body), cancel
}

// readEvent returns the next non-comment SSE frame.
func readEvent(t *testing.T, r *bufio.Reader) (name, id, data string) {
	t.Helper()
	deadline := time.AfterFunc(5*time.Second, func() { t.Error("timed out waiting for an SSE event") })
	defer deadline.Stop()
	for {
		var block []string
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				t.Fatalf("stream ended: %v", err)
			}
			line = strings.TrimRight(line, "\n")
			if line == "" {
				break
			}
			block = append(block, line)
		}
		for _, l := range block {
			switch {
			case strings.HasPrefix(l, "event: "):
				name = strings.TrimPrefix(l, "event: ")
			case strings.HasPrefix(l, "id: "):
				id = strings.TrimPrefix(l, "id: ")
			case strings.HasPrefix(l, "data: "):
				data = strings.TrimPrefix(l, "data: ")
			}
		}
		if name != "" {
			return name, id, data
		}
	}
}

func waitSubs(t *testing.T, hub *Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		hub.mu.RLock()
		n := len(hub.subs)
		hub.mu.RUnlock()
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d subscribers, want %d", n, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStreamDeliversOnlyMatchingEventsAsSSE(t *testing.T) {
	const wantID = "0123456789abcdef0123456789abcdef"
	hub := NewHub(5)
	srv := newServer(t, hub)
	r, _ := connect(t, srv, "service=hdfs&level=error")
	waitSubs(t, hub, 1)

	hub.Publish(ev("skip1", "nginx", "error", "wrong service"))
	hub.Publish(ev("skip2", "hdfs", "info", "wrong level"))
	hub.Publish(ev(wantID, "hdfs", "error", "disk failed"))

	name, id, data := readEvent(t, r)
	var got Event
	if err := json.Unmarshal([]byte(data), &got); err != nil {
		t.Fatal(err)
	}
	if name != "log" || id != wantID || got.Message != "disk failed" || got.Service != "hdfs" {
		t.Errorf("event = %q id %q %+v; want the one matching log", name, id, got)
	}
}

func TestStreamReportsDroppedEventsToASlowReader(t *testing.T) {
	hub := NewHub(5)
	srv := newServer(t, hub) // buffer of 2 events per client
	r, _ := connect(t, srv, "")
	waitSubs(t, hub, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 200 {
			hub.Publish(ev("e"+string(rune('0'+i%10)), "s", "info", "burst"))
		}
	}()
	wg.Wait()

	sawDropped := false
	for range 6 {
		name, _, data := readEvent(t, r)
		if name == "dropped" {
			sawDropped = strings.Contains(data, `"dropped"`)
			break
		}
	}
	if !sawDropped {
		t.Error("a reader that fell behind was never told that events were dropped")
	}
}

func TestStreamAuthValidationAndCleanup(t *testing.T) {
	hub := NewHub(1)
	srv := newServer(t, hub)

	for name, tc := range map[string]struct {
		key, query string
		want       int
	}{
		"no key":    {"", "", http.StatusUnauthorized},
		"wrong key": {"nope", "", http.StatusUnauthorized},
		"bad level": {"k", "level=loud", http.StatusBadRequest},
	} {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/tail?"+tc.query, nil)
		if tc.key != "" {
			req.Header.Set("X-API-Key", tc.key)
		}
		resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed on the next lines
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s: status %d, want %d", name, resp.StatusCode, tc.want)
		}
	}
	waitSubs(t, hub, 0)

	_, cancel := connect(t, srv, "")
	waitSubs(t, hub, 1)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/tail", nil)
	req.Header.Set("X-API-Key", "k")
	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed on the next line
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Errorf("at the client limit: status %d Retry-After %q; want 503 with Retry-After", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	cancel()
	waitSubs(t, hub, 0) // a disconnected client's slot is released
}

func TestAForgedIDCannotInjectFieldsIntoTheStream(t *testing.T) {
	hub := NewHub(5)
	srv := newServer(t, hub)
	r, _ := connect(t, srv, "")
	waitSubs(t, hub, 1)

	hub.Publish(ev("0123456789abcdef0123456789abcdef\nevent: dropped\ndata: {\"dropped\":999999}", "s", "info", "forged id"))
	hub.Publish(ev("0123456789abcdef0123456789abcdef", "s", "info", "genuine"))

	name, id, data := readEvent(t, r)
	if name != "log" || id != "" || !strings.Contains(data, "forged id") {
		t.Fatalf("forged entry = %q id %q data %q; want a plain log event with no id", name, id, data)
	}
	name, id, _ = readEvent(t, r)
	if name != "log" || id != "0123456789abcdef0123456789abcdef" {
		t.Errorf("a well-formed ID should be sent: got event %q id %q", name, id)
	}
}
