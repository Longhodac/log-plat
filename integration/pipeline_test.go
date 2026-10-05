//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/agent"
	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/collector"
	"github.com/Longhodac/log-plat/internal/indexer"
	"github.com/Longhodac/log-plat/internal/kafkautil"
	"github.com/Longhodac/log-plat/internal/query"
	"github.com/Longhodac/log-plat/internal/zeroloss"
)

var fastBackoff = backoff.Policy{Base: 50 * time.Millisecond, Max: time.Second}

func startCollector(t *testing.T, ctx context.Context, topic string) string {
	t.Helper()
	cl, err := kgo.NewClient(append(collector.ProducerOpts(30*time.Second), kgo.SeedBrokers(brokers...))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	if err := kafkautil.EnsureTopics(ctx, cl, kafkautil.Topic{Name: topic, Partitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	keys, _ := apikey.Parse("it-key:hdfs")
	srv := grpc.NewServer(grpc.StreamInterceptor(collector.StreamAuth(keys, agent.APIKeyHeader)))
	logplatv1.RegisterIngestServiceServer(srv, &collector.Server{
		Pub: &collector.KafkaPublisher{Client: cl, Topic: topic}, MaxInflight: 8, Log: discard(), Now: time.Now,
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func startIndexer(t *testing.T, ctx context.Context, topic, prefix string) *kgo.Client {
	t.Helper()
	osc := openSearch(t, prefix)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(topic+"-indexer"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := kafkautil.EnsureTopics(ctx, cl, kafkautil.Topic{Name: topic + "-dlq", Partitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	ix := &indexer.Indexer{
		Kafka: cl, OS: indexer.OpenSearchBulk{Client: osc},
		Cfg: indexer.Config{IndexPrefix: prefix, DLQTopic: topic + "-dlq", MaxPoll: 1000, Backoff: fastBackoff, ShutdownGrace: 5 * time.Second},
		Log: discard(), Now: time.Now,
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = ix.Run(ctx) }()
	t.Cleanup(func() { <-done; cl.Close() })
	return cl
}

func writeHDFS(t *testing.T, path string, n int) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i := range n {
		level := "INFO"
		switch {
		case i%50 == 0:
			level = "ERROR"
		case i%10 == 0:
			level = "WARN"
		}
		// Three lines share each second, so pagination must break
		// timestamp ties by ID to avoid skipping or repeating documents.
		sec := i / 3
		fmt.Fprintf(f, "081109 %02d%02d%02d %d %s dfs.DataNode$PacketResponder: block blk_%d terminating\n",
			20+sec/3600, sec/60%60, sec%60, 100+i, level, i)
	}
}

func TestPipelineEndToEndZeroLossAndQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	name := uniq(t)
	prefix := "logs-" + name

	addr := startCollector(t, ctx, name)
	startIndexer(t, ctx, name, prefix)

	dir := t.TempDir()
	logPath := filepath.Join(dir, "hdfs.log")
	const lines = 5000
	writeHDFS(t, logPath, lines)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	a, err := agent.New(agent.Config{
		AgentID: "it-agent", Host: "it", Paths: []string{logPath}, StateDir: filepath.Join(dir, "state"),
		BatchMaxEntries: 500, BatchLinger: 20 * time.Millisecond, PollInterval: 20 * time.Millisecond, Backoff: fastBackoff,
	}, logplatv1.NewIngestServiceClient(conn), "it-key", discard())
	if err != nil {
		t.Fatal(err)
	}
	agentCtx, stopAgent := context.WithCancel(ctx)
	agentDone := make(chan error, 1)
	go func() { agentDone <- a.Run(agentCtx) }()
	defer func() { stopAgent(); <-agentDone }()

	osc := openSearch(t, prefix)
	ids, err := zeroloss.ExpectedIDs(logPath, "it-agent", logPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	report, err := zeroloss.Checker{Client: osc, IndexPrefix: prefix}.Check(ctx, logPath, logPath, ids, zeroloss.Options{
		Poll: 500 * time.Millisecond, Stall: 30 * time.Second, Timeout: 2 * time.Minute, Log: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.ZeroLoss || report.Expected != lines {
		t.Fatalf("zero-loss report: %+v", report)
	}

	keys, _ := apikey.Parse("q:ops")
	api := httptest.NewServer((&query.Server{
		Search: query.OpenSearch{Client: osc, IndexPrefix: prefix}, Keys: keys, Log: discard(), Timeout: 10 * time.Second,
	}).Handler())
	defer api.Close()

	get := func(params url.Values) query.SearchResponse {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, api.URL+"/v1/logs?"+params.Encode(), nil)
		req.Header.Set("X-API-Key", "q")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: HTTP %d", params.Encode(), resp.StatusCode)
		}
		var out query.SearchResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	t.Run("cursor pagination visits every document once in order", func(t *testing.T) {
		seen := map[string]bool{}
		params := url.Values{"service": {"hdfs"}, "limit": {"700"}}
		var prev *query.SortKey
		for pages := 0; ; pages++ {
			if pages > lines/700+2 {
				t.Fatal("pagination did not terminate")
			}
			page := get(params)
			for _, d := range page.Logs {
				if seen[d.ID] {
					t.Fatalf("document %s returned twice", d.ID)
				}
				seen[d.ID] = true
				key := query.SortKey{TimestampMillis: d.Timestamp.UnixMilli(), ID: d.ID}
				if prev != nil && (key.TimestampMillis > prev.TimestampMillis || (key.TimestampMillis == prev.TimestampMillis && key.ID > prev.ID)) {
					t.Fatalf("out of order: %+v after %+v", key, *prev)
				}
				prev = &key
			}
			if page.NextCursor == "" {
				break
			}
			params.Set("cursor", page.NextCursor)
		}
		if len(seen) != lines {
			t.Errorf("paginated over %d documents, want %d", len(seen), lines)
		}
	})

	t.Run("filters by level, text, and time range", func(t *testing.T) {
		if n := len(get(url.Values{"level": {"error"}, "limit": {"1000"}}).Logs); n != lines/50 {
			t.Errorf("level=error matched %d, want %d", n, lines/50)
		}
		if got := get(url.Values{"q": {"blk_4242"}}).Logs; len(got) != 1 || !strings.Contains(got[0].Message, "blk_4242 ") {
			t.Errorf("q=blk_4242 matched %d docs, want the one line", len(got))
		}
		from := time.Date(2008, 11, 9, 20, 0, 0, 0, time.UTC)
		window := url.Values{"from": {from.Format(time.RFC3339)}, "to": {from.Add(time.Minute).Format(time.RFC3339)}, "limit": {"1000"}}
		if n := len(get(window).Logs); n != 180 {
			t.Errorf("one-minute window matched %d, want 180 (lines 0-179)", n)
		}
	})
}
