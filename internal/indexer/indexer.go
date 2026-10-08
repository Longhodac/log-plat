// Package indexer consumes log records from Kafka and bulk-indexes them into
// OpenSearch.
//
// Delivery is at-least-once: offsets are committed only after every record in
// a poll has been either indexed or written to the dead-letter topic. A crash
// before the commit redelivers those records, and because each document's
// _id is the log ID the redelivery overwrites rather than duplicates.
package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/collector"
	"github.com/Longhodac/log-plat/internal/doc"
)

// Config controls an indexer.
type Config struct {
	IndexPrefix string
	DLQTopic    string
	MaxPoll     int
	// BulkWorkers is how many _bulk requests one poll sends at once. A poll's
	// documents are split into that many chunks. Offsets are still committed
	// once, after every chunk is done, so the delivery guarantee is unchanged.
	// Zero or one sends a single request.
	BulkWorkers int
	Backoff     backoff.Policy
	// BulkTimeout bounds one _bulk request. A request that hangs on a dead
	// connection is abandoned and retried instead of wedging the indexer.
	// Zero means no limit.
	BulkTimeout time.Duration
	// ShutdownGrace is how long an in-progress batch may keep indexing and
	// committing after shutdown starts.
	ShutdownGrace time.Duration
}

// Bulk sends one _bulk body and returns the per-item results in request order.
type Bulk interface {
	Bulk(ctx context.Context, body []byte) ([]ItemResult, error)
}

// ItemResult is one item of a _bulk response.
type ItemResult struct {
	Status int
	Error  string
}

// Indexer runs the consume, index, commit loop.
type Indexer struct {
	Kafka *kgo.Client
	OS    Bulk
	Cfg   Config
	Log   *slog.Logger
	Now   func() time.Time
}

// Run polls until ctx is done. The batch in progress at shutdown gets
// ShutdownGrace to finish and commit; if it cannot, it is not committed and
// will be redelivered.
func (ix *Indexer) Run(ctx context.Context) error {
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	stop := context.AfterFunc(ctx, func() { time.AfterFunc(ix.Cfg.ShutdownGrace, cancelWork) })
	defer stop()
	// With BlockRebalanceOnPoll, closing the client blocks until the last
	// poll's rebalance hold is released.
	defer ix.Kafka.AllowRebalance()

	for {
		pollStart := time.Now()
		fetches := ix.Kafka.PollRecords(ctx, ix.Cfg.MaxPoll)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				ix.Log.Error("fetch error", "topic", topic, "partition", p, "error", err)
			}
		})
		recs := fetches.Records()
		if len(recs) == 0 {
			ix.Kafka.AllowRebalance()
			continue
		}
		phaseSeconds.WithLabelValues("poll").Observe(time.Since(pollStart).Seconds())
		processStart := time.Now()
		if err := ix.process(workCtx, recs); err != nil {
			if workCtx.Err() != nil {
				ix.Log.Warn("shutdown grace expired; uncommitted records will be redelivered", "records", len(recs))
				return nil
			}
			return err
		}
		phaseSeconds.WithLabelValues("process").Observe(time.Since(processStart).Seconds())
		commitStart := time.Now()
		err := ix.Kafka.CommitRecords(workCtx, recs...)
		phaseSeconds.WithLabelValues("commit").Observe(time.Since(commitStart).Seconds())
		if err != nil {
			// The records are in OpenSearch; a failed commit only means they
			// will be redelivered and overwritten in place.
			commitErrors.Inc()
			ix.Log.Warn("offset commit failed", "error", err)
		}
		ix.Log.Debug("batch committed", "records", len(recs))
		ix.Kafka.AllowRebalance()
	}
}

// pendingDoc is a decoded record awaiting indexing.
type pendingDoc struct {
	rec  *kgo.Record
	line []byte // bulk action + source, newline-terminated
	obs  time.Time
}

func (ix *Indexer) process(ctx context.Context, recs []*kgo.Record) error {
	now := ix.Now()
	var docs []pendingDoc
	var dead []*kgo.Record
	for _, r := range recs {
		kafkaDwell.Observe(max(now.Sub(r.Timestamp), 0).Seconds())
		line, obs, reason := ix.encode(r, now)
		if reason != "" {
			dead = append(dead, deadLetter(ix.Cfg.DLQTopic, r, reason))
			continue
		}
		docs = append(docs, pendingDoc{rec: r, line: line, obs: obs})
	}

	permanent, err := ix.indexParallel(ctx, docs)
	if err != nil {
		return err
	}
	dead = append(dead, permanent...)
	if len(dead) > 0 {
		if err := ix.Kafka.ProduceSync(ctx, dead...).FirstErr(); err != nil {
			return fmt.Errorf("write dead letters: %w", err)
		}
		recordsTotal.WithLabelValues("dead_lettered").Add(float64(len(dead)))
	}
	return nil
}

// indexParallel splits docs into BulkWorkers chunks and indexes them at once.
// Each chunk retries independently, and the call returns only when every chunk
// has either been indexed or dead-lettered, so the caller can commit after it.
// Concurrent writes are safe because every document has a fixed _id.
func (ix *Indexer) indexParallel(ctx context.Context, docs []pendingDoc) ([]*kgo.Record, error) {
	workers := max(ix.Cfg.BulkWorkers, 1)
	if workers == 1 || len(docs) < 2 {
		return ix.index(ctx, docs)
	}
	size := (len(docs) + workers - 1) / workers
	var (
		g    errgroup.Group
		mu   sync.Mutex
		dead []*kgo.Record
	)
	for start := 0; start < len(docs); start += size {
		chunk := docs[start:min(start+size, len(docs))]
		g.Go(func() error {
			d, err := ix.index(ctx, chunk)
			mu.Lock()
			dead = append(dead, d...)
			mu.Unlock()
			return err
		})
	}
	return dead, g.Wait()
}

// encode turns a Kafka record into a bulk index line, or a reason it cannot be one.
func (ix *Indexer) encode(r *kgo.Record, now time.Time) ([]byte, time.Time, string) {
	var e logplatv1.LogEntry
	if err := proto.Unmarshal(r.Value, &e); err != nil {
		return nil, time.Time{}, "unparseable: " + err.Error()
	}
	// The collector already validated, but anything can be written to the
	// topic; re-check so garbage that happens to decode is dead-lettered.
	if reason := collector.Validate(&e, now); reason != "" {
		return nil, time.Time{}, "invalid: " + reason
	}
	if e.GetService() == "" {
		return nil, time.Time{}, "invalid: missing service"
	}
	d := doc.FromEntry(&e, now)
	action, _ := json.Marshal(map[string]any{"index": map[string]string{
		"_index": doc.IndexName(ix.Cfg.IndexPrefix, d.Timestamp),
		"_id":    d.ID,
	}})
	src, err := json.Marshal(d)
	if err != nil {
		return nil, time.Time{}, "unencodable: " + err.Error()
	}
	line := make([]byte, 0, len(action)+len(src)+2)
	line = append(append(append(append(line, action...), '\n'), src...), '\n')
	return line, d.ObservedAt, ""
}

// index writes docs, retrying transient failures until each item succeeds or
// fails permanently. Permanent failures come back as dead letters.
func (ix *Indexer) index(ctx context.Context, docs []pendingDoc) ([]*kgo.Record, error) {
	var dead []*kgo.Record
	for attempt := 0; len(docs) > 0; attempt++ {
		if attempt > 0 {
			bulkRetries.Inc()
			if err := ix.Cfg.Backoff.Sleep(ctx, attempt-1); err != nil {
				return nil, err
			}
		}
		var body bytes.Buffer
		for _, d := range docs {
			body.Write(d.line)
		}
		start := time.Now()
		bulkCtx, cancel := ctx, context.CancelFunc(func() {})
		if ix.Cfg.BulkTimeout > 0 {
			bulkCtx, cancel = context.WithTimeout(ctx, ix.Cfg.BulkTimeout)
		}
		results, err := ix.OS.Bulk(bulkCtx, body.Bytes())
		cancel()
		bulkSeconds.Observe(time.Since(start).Seconds())
		if err == nil && len(results) != len(docs) {
			err = fmt.Errorf("bulk returned %d items for %d docs", len(results), len(docs))
		}
		if err != nil {
			ix.Log.Warn("bulk request failed; retrying", "error", err, "docs", len(docs), "attempt", attempt)
			continue
		}
		var retry []pendingDoc
		indexedAt := ix.Now()
		for i, res := range results {
			switch classify(res.Status) {
			case outcomeOK:
				recordsTotal.WithLabelValues("indexed").Inc()
				e2eSeconds.Observe(indexedAt.Sub(docs[i].obs).Seconds())
			case outcomeRetry:
				retry = append(retry, docs[i])
			case outcomeDead:
				dead = append(dead, deadLetter(ix.Cfg.DLQTopic, docs[i].rec, "rejected by opensearch: HTTP "+strconv.Itoa(res.Status)+": "+res.Error))
			}
		}
		if len(retry) > 0 {
			ix.Log.Warn("bulk items failed transiently; retrying", "count", len(retry), "attempt", attempt)
		}
		docs = retry
	}
	return dead, nil
}

type outcome int

const (
	outcomeOK outcome = iota
	outcomeRetry
	outcomeDead
)

// classify maps a bulk item status to what the indexer does with it. 429 and
// 5xx are load or availability problems that clear up; other 4xx (mapping
// conflicts, malformed documents) will fail the same way forever.
func classify(status int) outcome {
	switch {
	case status >= 200 && status < 300:
		return outcomeOK
	case status == 429 || status >= 500:
		return outcomeRetry
	default:
		return outcomeDead
	}
}

func deadLetter(topic string, r *kgo.Record, reason string) *kgo.Record {
	return &kgo.Record{
		Topic: topic,
		Key:   r.Key,
		Value: r.Value,
		Headers: []kgo.RecordHeader{
			{Key: "error", Value: []byte(reason)},
			{Key: "source_topic", Value: []byte(r.Topic)},
			{Key: "source_partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
			{Key: "source_offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
		},
	}
}

// OpenSearchBulk adapts the official client to Bulk.
type OpenSearchBulk struct {
	Client *opensearchapi.Client
}

// Bulk implements Bulk.
func (b OpenSearchBulk) Bulk(ctx context.Context, body []byte) ([]ItemResult, error) {
	resp, err := b.Client.Bulk(ctx, opensearchapi.BulkReq{Body: bytes.NewReader(body)})
	if err != nil {
		return nil, err
	}
	out := make([]ItemResult, len(resp.Items))
	for i, item := range resp.Items {
		for _, it := range item {
			out[i].Status = it.Status
			if it.Error != nil {
				out[i].Error = it.Error.Type + ": " + it.Error.Reason
			}
		}
	}
	return out, nil
}
