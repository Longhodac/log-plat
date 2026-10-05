package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/logid"
)

// gatedPublisher holds every write until release is closed, then reports err.
type gatedPublisher struct {
	release chan struct{}
	err     error
	mu      sync.Mutex
	got     []*logplatv1.LogEntry
}

func (p *gatedPublisher) Publish(_ context.Context, entries []*logplatv1.LogEntry) func() error {
	p.mu.Lock()
	p.got = append(p.got, entries...)
	p.mu.Unlock()
	return func() error {
		<-p.release
		return p.err
	}
}

func startServer(t *testing.T, pub Publisher) logplatv1.IngestServiceClient {
	t.Helper()
	keys, err := apikey.Parse("good-key:billing")
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.StreamInterceptor(StreamAuth(keys, "x-api-key")))
	logplatv1.RegisterIngestServiceServer(srv, &Server{Pub: pub, MaxInflight: 4, Log: slog.New(slog.DiscardHandler), Now: time.Now})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return logplatv1.NewIngestServiceClient(conn)
}

func entry(offset int64) *logplatv1.LogEntry {
	now := timestamppb.Now()
	return &logplatv1.LogEntry{
		Id: logid.New("a", "/f", 0, offset), Message: "hello", Timestamp: now, ObservedAt: now,
		Level: logplatv1.Level_LEVEL_INFO, Source: "/f", Service: "spoofed",
	}
}

func open(t *testing.T, c logplatv1.IngestServiceClient, key string) logplatv1.IngestService_IngestClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	s, err := c.Ingest(metadata.AppendToOutgoingContext(ctx, "x-api-key", key))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAckWaitsForPublishAndStampsService(t *testing.T) {
	pub := &gatedPublisher{release: make(chan struct{})}
	s := open(t, startServer(t, pub), "good-key")
	bad := entry(1)
	bad.Message = ""
	if err := s.Send(&logplatv1.IngestRequest{Seq: 7, Entries: []*logplatv1.LogEntry{entry(0), bad}}); err != nil {
		t.Fatal(err)
	}

	acked := make(chan *logplatv1.IngestResponse, 1)
	go func() {
		a, _ := s.Recv()
		acked <- a
	}()
	select {
	case <-acked:
		t.Fatal("ack arrived before the publish completed")
	case <-time.After(200 * time.Millisecond):
	}
	close(pub.release)
	a := <-acked
	if a.GetSeq() != 7 || a.GetAccepted() != 1 || len(a.GetRejected()) != 1 || a.GetRejected()[0].GetReason() != "empty message" {
		t.Fatalf("ack = %v; want seq 7, 1 accepted, 1 rejected for empty message", a)
	}
	if got := pub.got[0].GetService(); got != "billing" {
		t.Errorf("published service %q, want the key's service, not the agent's claim", got)
	}
}

func TestPublishFailureEndsStreamUnavailable(t *testing.T) {
	pub := &gatedPublisher{release: make(chan struct{}), err: errors.New("broker down")}
	close(pub.release)
	s := open(t, startServer(t, pub), "good-key")
	if err := s.Send(&logplatv1.IngestRequest{Seq: 1, Entries: []*logplatv1.LogEntry{entry(0)}}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Recv()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Recv error = %v, want Unavailable so the agent retries", err)
	}
}

func TestAuthRejectsUnknownKeys(t *testing.T) {
	pub := &gatedPublisher{release: make(chan struct{})}
	client := startServer(t, pub)
	for _, key := range []string{"", "wrong-key"} {
		s := open(t, client, key)
		_ = s.Send(&logplatv1.IngestRequest{Seq: 1, Entries: []*logplatv1.LogEntry{entry(0)}})
		if _, err := s.Recv(); status.Code(err) != codes.Unauthenticated {
			t.Errorf("key %q: Recv error = %v, want Unauthenticated", key, err)
		}
	}
	if len(pub.got) != 0 {
		t.Errorf("unauthenticated streams published %d entries", len(pub.got))
	}
}

func TestAcksFollowArrivalOrderAndCleanCloseReturnsEOF(t *testing.T) {
	pub := &gatedPublisher{release: make(chan struct{})}
	close(pub.release)
	s := open(t, startServer(t, pub), "good-key")
	for seq := uint64(1); seq <= 10; seq++ {
		if err := s.Send(&logplatv1.IngestRequest{Seq: seq, Entries: []*logplatv1.LogEntry{entry(int64(seq))}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 10; seq++ {
		a, err := s.Recv()
		if err != nil || a.GetSeq() != seq {
			t.Fatalf("ack %d = %v, %v", seq, a, err)
		}
	}
	if _, err := s.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("after last ack Recv = %v, want EOF", err)
	}
}
