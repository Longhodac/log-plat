package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/spool"
)

// APIKeyHeader carries the agent's API key on the gRPC stream.
const APIKeyHeader = "x-api-key"

// Sender streams spooled batches to the collector and advances the spool
// cursor as acks arrive.
type Sender struct {
	Client  logplatv1.IngestServiceClient
	APIKey  string
	Spool   *spool.Spool
	Window  int // batches in flight per stream
	Backoff backoff.Policy
	Log     *slog.Logger
}

type flight struct {
	seq   uint64
	next  spool.Pos
	sent  time.Time
	acked bool
}

// errStreamClosed reports a collector that ended the stream without an error.
var errStreamClosed = errors.New("collector closed the stream")

// Run sends until ctx is done, or until drained is closed and every spooled
// record has been acknowledged.
func (s *Sender) Run(ctx context.Context, drained <-chan struct{}) error {
	attempt := 0
	for {
		progressed, err := s.session(ctx, drained)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		sendErrors.Inc()
		if progressed {
			attempt = 0
		}
		s.Log.Warn("collector stream failed; backing off", "error", err, "attempt", attempt)
		if err := s.Backoff.Sleep(ctx, attempt); err != nil {
			return err
		}
		attempt++
	}
}

// session runs one stream. Every record not acknowledged when it ends is
// resent by the next session, starting from the spool's ack cursor.
func (s *Sender) session(ctx context.Context, drained <-chan struct{}) (progressed bool, err error) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := s.Client.Ingest(metadata.AppendToOutgoingContext(sctx, APIKeyHeader, s.APIKey))
	if err != nil {
		return false, err
	}

	acks := make(chan *logplatv1.IngestResponse, s.Window)
	recvErr := make(chan error, 1)
	go func() {
		for {
			ack, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case acks <- ack:
			case <-sctx.Done():
				return
			}
		}
	}()

	r := s.Spool.NewReader(s.Spool.Acked())
	defer r.Close()
	var inflight []flight
	var seq uint64
	for {
		for len(inflight) < s.Window {
			rec, ok, err := r.Next()
			if err != nil {
				return progressed, err
			}
			if !ok {
				break
			}
			seq++
			req := &logplatv1.IngestRequest{}
			if err := proto.Unmarshal(rec.Payload, req); err != nil {
				// CRC passed, so this is a bug rather than disk damage.
				// Skipping beats wedging the agent on one record forever.
				corruptRecords.Inc()
				s.Log.Error("skipping undecodable spool record", "error", err, "next", rec.Next)
				inflight = append(inflight, flight{seq: seq, next: rec.Next, acked: true})
				continue
			}
			req.Seq = seq
			if err := stream.Send(req); err != nil {
				if errors.Is(err, io.EOF) {
					return progressed, <-recvErr
				}
				return progressed, err
			}
			batchesSent.Inc()
			inflight = append(inflight, flight{seq: seq, next: rec.Next, sent: time.Now()})
		}

		if len(inflight) == 0 && isClosed(drained) {
			_ = stream.CloseSend()
			return progressed, nil
		}
		if done := s.retire(&inflight); done {
			progressed = true
			continue
		}

		select {
		case ack := <-acks:
			if len(inflight) == 0 || ack.GetSeq() < inflight[0].seq || ack.GetSeq() > inflight[len(inflight)-1].seq {
				s.Log.Warn("ack for unknown batch", "seq", ack.GetSeq())
				continue
			}
			f := &inflight[ack.GetSeq()-inflight[0].seq]
			if !f.acked {
				f.acked = true
				ackSeconds.Observe(time.Since(f.sent).Seconds())
				entriesAcked.Add(float64(ack.GetAccepted()))
				entriesRejected.Add(float64(len(ack.GetRejected())))
				for _, rj := range ack.GetRejected() {
					s.Log.Warn("collector rejected entry", "id", rj.GetId(), "reason", rj.GetReason())
				}
			}
			if s.retire(&inflight) {
				progressed = true
			}
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				err = errStreamClosed
			}
			return progressed, err
		case <-s.Spool.Notify():
		case <-whenOpen(drained, len(inflight) == 0):
			// Drain requested while idle: loop back to re-check for records.
		case <-ctx.Done():
			return progressed, ctx.Err()
		}
	}
}

// retire pops the acknowledged prefix of inflight and advances the spool
// cursor past it. Acks may arrive out of order; the cursor only moves over a
// contiguous acknowledged run, so it never skips an unacked batch.
func (s *Sender) retire(inflight *[]flight) bool {
	n := 0
	for n < len(*inflight) && (*inflight)[n].acked {
		n++
	}
	if n == 0 {
		return false
	}
	next := (*inflight)[n-1].next
	*inflight = (*inflight)[n:]
	if err := s.Spool.Ack(next); err != nil {
		s.Log.Error("spool ack failed", "error", err)
	}
	return true
}

func isClosed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// whenOpen returns c only when cond holds, so a select can wait on it conditionally.
func whenOpen(c <-chan struct{}, cond bool) <-chan struct{} {
	if cond {
		return c
	}
	return nil
}
