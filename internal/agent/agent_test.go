package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/collector"
	"github.com/Longhodac/log-plat/internal/zeroloss"
)

// memPublisher records published IDs. failFirst publishes fail before any succeed.
type memPublisher struct {
	mu        sync.Mutex
	ids       map[string]int
	entries   []*logplatv1.LogEntry
	failFirst atomic.Int32
}

func (p *memPublisher) Publish(_ context.Context, entries []*logplatv1.LogEntry) func() error {
	if p.failFirst.Add(-1) >= 0 {
		return func() error { return errors.New("injected kafka failure") }
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range entries {
		p.ids[e.GetId()]++
		p.entries = append(p.entries, e)
	}
	return func() error { return nil }
}

func (p *memPublisher) snapshot() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int, len(p.ids))
	for k, v := range p.ids {
		out[k] = v
	}
	return out
}

// harness is a collector on an in-memory listener that can be stopped and
// restarted to simulate outages.
type harness struct {
	t   *testing.T
	pub *memPublisher
	lis *bufconn.Listener
	srv *grpc.Server
	mu  sync.Mutex
}

func newHarness(t *testing.T) *harness {
	h := &harness{t: t, pub: &memPublisher{ids: map[string]int{}}, lis: bufconn.Listen(4 << 20)}
	t.Cleanup(func() { h.stop() })
	return h
}

func (h *harness) start() {
	h.mu.Lock()
	defer h.mu.Unlock()
	keys, _ := apikey.Parse("k:svc")
	h.srv = grpc.NewServer(grpc.StreamInterceptor(collector.StreamAuth(keys, APIKeyHeader)))
	logplatv1.RegisterIngestServiceServer(h.srv, &collector.Server{Pub: h.pub, MaxInflight: 4, Log: slog.New(slog.DiscardHandler), Now: time.Now})
	lis := h.lis
	go func() { _ = h.srv.Serve(lis) }()
}

func (h *harness) stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.srv != nil {
		h.srv.Stop()
		h.srv = nil
		h.lis = bufconn.Listen(4 << 20)
	}
}

func (h *harness) client() logplatv1.IngestServiceClient {
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			h.mu.Lock()
			lis := h.lis
			h.mu.Unlock()
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { conn.Close() })
	return logplatv1.NewIngestServiceClient(conn)
}

func testConfig(stateDir string, paths ...string) Config {
	return Config{
		AgentID: "agent-test", Host: "h", Paths: paths, StateDir: stateDir,
		BatchMaxEntries: 50, BatchLinger: 10 * time.Millisecond,
		PollInterval: 5 * time.Millisecond, ScanInterval: 20 * time.Millisecond,
		ReadSize: 1024, DrainTimeout: 2 * time.Second,
		Backoff: backoff.Policy{Base: 5 * time.Millisecond, Max: 50 * time.Millisecond},
	}
}

func writeLines(t *testing.T, path string, from, to int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i := from; i < to; i++ {
		fmt.Fprintf(f, "081109 203615 %d INFO dfs.DataNode: line %d\n", i, i)
	}
}

// expectedIDs is what the zero-loss checker will look for.
func expectedIDs(t *testing.T, agentID, path string, epoch uint32) map[string]bool {
	t.Helper()
	ids, err := zeroloss.ExpectedIDs(path, agentID, path, epoch)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = true
	}
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func containsAll(got map[string]int, want map[string]bool) bool {
	for id := range want {
		if got[id] == 0 {
			return false
		}
	}
	return true
}

func runAgent(t *testing.T, cfg Config, client logplatv1.IngestServiceClient) (stop func() error) {
	t.Helper()
	a, err := New(cfg, client, "k", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- a.Run(ctx) }()
	return func() error { cancel(); return <-errc }
}

func TestDeliversEveryLineWithExpectedIDs(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	writeLines(t, logPath, 0, 500)
	h := newHarness(t)
	h.start()
	stop := runAgent(t, testConfig(filepath.Join(dir, "state"), filepath.Join(dir, "*.log")), h.client())

	writeLines(t, logPath, 500, 1000)
	want := expectedIDs(t, "agent-test", logPath, 0)
	waitFor(t, "all 1000 lines", func() bool { return containsAll(h.pub.snapshot(), want) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := h.pub.snapshot(); len(got) != len(want) {
		t.Errorf("published %d distinct IDs, want exactly %d", len(got), len(want))
	}
}

func TestSpoolsThroughOutageAndKafkaFailures(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	h := newHarness(t)
	h.pub.failFirst.Store(3)
	stop := runAgent(t, testConfig(filepath.Join(dir, "state"), logPath), h.client())

	writeLines(t, logPath, 0, 400)
	spoolDir := filepath.Join(dir, "state", "spool")
	waitFor(t, "lines spooled while collector is down", func() bool {
		segs, _ := filepath.Glob(filepath.Join(spoolDir, "*.seg"))
		for _, s := range segs {
			if fi, err := os.Stat(s); err == nil && fi.Size() > 0 {
				return true
			}
		}
		return false
	})
	if n := len(h.pub.snapshot()); n != 0 {
		t.Fatalf("%d entries published with the collector down", n)
	}

	h.start()
	want := expectedIDs(t, "agent-test", logPath, 0)
	waitFor(t, "delivery after collector recovers", func() bool { return containsAll(h.pub.snapshot(), want) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestRestartResumesWithoutLoss(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	state := filepath.Join(dir, "state")
	h := newHarness(t)
	h.start()

	writeLines(t, logPath, 0, 300)
	stop := runAgent(t, testConfig(state, logPath), h.client())
	waitFor(t, "first lines", func() bool { return len(h.pub.snapshot()) > 0 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	writeLines(t, logPath, 300, 600)
	stop = runAgent(t, testConfig(state, logPath), h.client())
	want := expectedIDs(t, "agent-test", logPath, 0)
	waitFor(t, "all lines after restart", func() bool { return containsAll(h.pub.snapshot(), want) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := h.pub.snapshot(); len(got) != len(want) {
		t.Errorf("published %d distinct IDs, want %d", len(got), len(want))
	}
}

func TestTruncationStartsNewEpoch(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	h := newHarness(t)
	h.start()
	writeLines(t, logPath, 0, 50)
	stop := runAgent(t, testConfig(filepath.Join(dir, "state"), logPath), h.client())
	first := expectedIDs(t, "agent-test", logPath, 0)
	waitFor(t, "epoch 0 lines", func() bool { return containsAll(h.pub.snapshot(), first) })

	if err := os.Truncate(logPath, 0); err != nil {
		t.Fatal(err)
	}
	writeLines(t, logPath, 0, 10)
	second := expectedIDs(t, "agent-test", logPath, 1)
	waitFor(t, "epoch 1 lines", func() bool { return containsAll(h.pub.snapshot(), second) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}
