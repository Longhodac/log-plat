package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/logid"
)

// scriptedBulk answers each call with the next status list and records the
// IDs it was sent.
type scriptedBulk struct {
	script [][]int
	calls  [][]string
}

func (b *scriptedBulk) Bulk(_ context.Context, body []byte) ([]ItemResult, error) {
	var ids []string
	for _, line := range bytes.Split(bytes.TrimSpace(body), []byte("\n")) {
		var action struct {
			Index struct {
				ID string `json:"_id"`
			} `json:"index"`
		}
		if json.Unmarshal(line, &action) == nil && action.Index.ID != "" {
			ids = append(ids, action.Index.ID)
		}
	}
	b.calls = append(b.calls, ids)
	statuses := b.script[0]
	b.script = b.script[1:]
	out := make([]ItemResult, len(statuses))
	for i, s := range statuses {
		out[i] = ItemResult{Status: s, Error: "x"}
	}
	return out, nil
}

// hangingBulk blocks its first call until the context ends, like a request
// stuck on a dead connection, then answers normally.
type hangingBulk struct{ calls int }

func (b *hangingBulk) Bulk(ctx context.Context, body []byte) ([]ItemResult, error) {
	b.calls++
	if b.calls == 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	n := bytes.Count(body, []byte("\n")) / 2
	out := make([]ItemResult, n)
	for i := range out {
		out[i].Status = 201
	}
	return out, nil
}

func newIndexer(b Bulk) *Indexer {
	return &Indexer{
		OS:  b,
		Cfg: Config{IndexPrefix: "logs", DLQTopic: "dlq", Backoff: backoff.Policy{Base: time.Millisecond, Max: time.Millisecond}},
		Log: slog.New(slog.DiscardHandler),
		Now: time.Now,
	}
}

func record(t testing.TB, offset int64) *kgo.Record {
	t.Helper()
	ts := timestamppb.New(time.Date(2008, 11, 9, 20, 36, 15, 0, time.UTC))
	v, err := proto.Marshal(&logplatv1.LogEntry{
		Id: logid.New("a", "/f", 0, offset), Service: "hdfs", Message: "m", Timestamp: ts, ObservedAt: ts,
		Level: logplatv1.Level_LEVEL_INFO, Source: "/f", Offset: offset,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "logs", Partition: 2, Offset: offset, Value: v}
}

func TestIndexRetriesOnlyTransientItemsAndDeadLettersPermanentOnes(t *testing.T) {
	b := &scriptedBulk{script: [][]int{{429, 201, 400, 503}, {200, 201}}}
	ix := newIndexer(b)
	var docs []pendingDoc
	for i := range 4 {
		line, obs, reason := ix.encode(record(t, int64(i)), time.Now())
		if reason != "" {
			t.Fatal(reason)
		}
		docs = append(docs, pendingDoc{rec: record(t, int64(i)), line: line, obs: obs})
	}

	dead, err := ix.index(context.Background(), docs)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.calls) != 2 {
		t.Fatalf("%d bulk calls, want 2", len(b.calls))
	}
	retried := b.calls[1]
	if len(retried) != 2 || retried[0] != b.calls[0][0] || retried[1] != b.calls[0][3] {
		t.Errorf("second call sent %v, want only the 429 and 503 items %v", retried, []string{b.calls[0][0], b.calls[0][3]})
	}
	if len(dead) != 1 || dead[0].Topic != "dlq" {
		t.Fatalf("got %d dead letters, want 1 on dlq", len(dead))
	}
	headers := map[string]string{}
	for _, h := range dead[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["source_offset"] != "2" || !strings.Contains(headers["error"], "HTTP 400") {
		t.Errorf("dead letter headers %v, want source_offset 2 and an HTTP 400 error", headers)
	}
}

func TestEncodeUsesLogIDAndDailyIndex(t *testing.T) {
	ix := newIndexer(nil)
	line, _, reason := ix.encode(record(t, 9), time.Now())
	if reason != "" {
		t.Fatal(reason)
	}
	want := `{"index":{"_id":"` + logid.New("a", "/f", 0, 9) + `","_index":"logs-2008.11.09"}}`
	if got, _, _ := strings.Cut(string(line), "\n"); got != want {
		t.Errorf("action line = %s, want %s", got, want)
	}
}

func TestEncodeRejectsGarbage(t *testing.T) {
	ix := newIndexer(nil)
	noService := record(t, 1)
	var e logplatv1.LogEntry
	_ = proto.Unmarshal(noService.Value, &e)
	e.Service = ""
	noService.Value, _ = proto.Marshal(&e)

	for name, r := range map[string]*kgo.Record{
		"not protobuf":    {Value: []byte{0xff, 0xff, 0xff}},
		"decodes, empty":  {Value: []byte{}},
		"missing service": noService,
	} {
		if _, _, reason := ix.encode(r, time.Now()); reason == "" {
			t.Errorf("%s: encoded, want a dead-letter reason", name)
		}
	}
}

func BenchmarkEncode(b *testing.B) {
	ix := newIndexer(nil)
	r := record(b, 1)
	now := time.Now()
	for b.Loop() {
		if _, _, reason := ix.encode(r, now); reason != "" {
			b.Fatal(reason)
		}
	}
}

func TestHungBulkRequestIsAbandonedAndRetried(t *testing.T) {
	b := &hangingBulk{}
	ix := newIndexer(b)
	ix.Cfg.BulkTimeout = 50 * time.Millisecond
	line, obs, reason := ix.encode(record(t, 1), time.Now())
	if reason != "" {
		t.Fatal(reason)
	}

	done := make(chan error, 1)
	go func() {
		dead, err := ix.index(context.Background(), []pendingDoc{{rec: record(t, 1), line: line, obs: obs}})
		if err == nil && len(dead) != 0 {
			err = errors.New("unexpected dead letters")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("index() is still blocked on the hung request; BulkTimeout did not apply")
	}
	if b.calls != 2 {
		t.Errorf("%d bulk calls, want 2 (the hung one, then the retry)", b.calls)
	}
}
