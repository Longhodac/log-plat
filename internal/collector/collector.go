// Package collector receives batches from agents, validates them, and
// publishes them to Kafka. A batch is acknowledged only after Kafka has
// acknowledged every accepted entry in it.
package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
)

// Publisher writes entries durably. Publish must not block on the write
// itself: it starts the write and returns wait, which blocks until every
// entry is acknowledged or one fails. Publish may block for backpressure.
type Publisher interface {
	Publish(ctx context.Context, entries []*logplatv1.LogEntry) (wait func() error)
}

// Server implements IngestService.
type Server struct {
	logplatv1.UnimplementedIngestServiceServer
	Pub Publisher
	// MaxInflight bounds batches per stream that are publishing but not yet acked.
	MaxInflight int
	Log         *slog.Logger
	Now         func() time.Time
}

type pending struct {
	ack      *logplatv1.IngestResponse
	wait     func() error
	received time.Time
}

// Ingest pipelines each stream: a receive goroutine validates batches and
// starts their Kafka writes, while this goroutine waits for those writes in
// arrival order and sends acks. If any write fails the stream ends with
// Unavailable; the agent reconnects and resends everything it has not seen
// acked, and OpenSearch absorbs the resulting duplicates by log ID.
func (s *Server) Ingest(stream logplatv1.IngestService_IngestServer) error {
	ctx := stream.Context()
	svc, ok := ServiceFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "no authenticated service")
	}
	streamsActive.Inc()
	defer streamsActive.Dec()

	queue := make(chan pending, s.MaxInflight)
	recvErr := make(chan error, 1)
	go func() {
		defer close(queue)
		for {
			req, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			p := s.start(ctx, svc, req)
			select {
			case queue <- p:
			case <-ctx.Done():
				return
			}
		}
	}()

	for p := range queue {
		if err := p.wait(); err != nil {
			publishErrors.Inc()
			s.Log.Error("kafka publish failed; closing stream", "service", svc, "seq", p.ack.GetSeq(), "error", err)
			return status.Error(codes.Unavailable, "publish to kafka failed; retry")
		}
		entriesTotal.WithLabelValues(svc, "published").Add(float64(p.ack.GetAccepted()))
		if err := stream.Send(p.ack); err != nil {
			return err
		}
		batchSeconds.Observe(s.Now().Sub(p.received).Seconds())
	}
	select {
	case err := <-recvErr:
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	default:
		return ctx.Err()
	}
}

// start validates a batch, stamps the authenticated service on each valid
// entry, and begins publishing them.
func (s *Server) start(ctx context.Context, svc string, req *logplatv1.IngestRequest) pending {
	now := s.Now()
	ack := &logplatv1.IngestResponse{Seq: req.GetSeq()}
	valid := make([]*logplatv1.LogEntry, 0, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		if reason := Validate(e, now); reason != "" {
			ack.Rejected = append(ack.Rejected, &logplatv1.Rejection{Id: e.GetId(), Reason: reason})
			entriesTotal.WithLabelValues(svc, "rejected").Inc()
			continue
		}
		e.Service = svc
		valid = append(valid, e)
	}
	ack.Accepted = uint32(len(valid))
	batchesTotal.Inc()

	wait := func() error { return nil }
	if len(valid) > 0 {
		wait = s.Pub.Publish(ctx, valid)
	}
	return pending{ack: ack, wait: wait, received: now}
}
