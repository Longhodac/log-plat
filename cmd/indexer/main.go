// Command indexer consumes logs from Kafka and bulk-indexes them into OpenSearch.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/doc"
	"github.com/Longhodac/log-plat/internal/envcfg"
	"github.com/Longhodac/log-plat/internal/indexer"
	"github.com/Longhodac/log-plat/internal/kafkautil"
	"github.com/Longhodac/log-plat/internal/obs"
	"github.com/Longhodac/log-plat/internal/osutil"
)

func main() { obs.Main("indexer", run) }

func run(ctx context.Context, log *slog.Logger) error {
	var env envcfg.Reader
	var (
		admin       = env.String("ADMIN_ADDR", ":9100")
		brokers     = env.List("KAFKA_BROKERS", []string{"localhost:9094"})
		topic       = env.String("KAFKA_TOPIC", "logs")
		group       = env.String("KAFKA_GROUP", "indexer")
		dlq         = env.String("KAFKA_DLQ_TOPIC", "logs-dlq")
		replication = env.Int("KAFKA_REPLICATION_FACTOR", 1)
		osAddrs     = env.List("OPENSEARCH_ADDRS", []string{"http://localhost:9200"})
		prefix      = env.String("INDEX_PREFIX", "logs")
		shards      = env.Int("INDEX_SHARDS", 1)
		replicas    = env.Int("INDEX_REPLICAS", 0)
		refresh     = env.String("INDEX_REFRESH_INTERVAL", "5s")
		maxPoll     = env.Int("INDEXER_MAX_POLL_RECORDS", 5000)
		grace       = env.Duration("SHUTDOWN_GRACE", 20*time.Second)
	)
	if err := env.Err(); err != nil {
		return err
	}

	osc, err := osutil.New(osAddrs)
	if err != nil {
		return err
	}
	tmpl, err := doc.Template(prefix, shards, replicas, refresh)
	if err != nil {
		return err
	}
	if err := backoff.Default.Retry(ctx, func() error {
		return osutil.PutTemplate(ctx, osc, doc.TemplateName(prefix), tmpl)
	}, func(attempt int, err error) { log.Warn("waiting for opensearch", "attempt", attempt, "error", err) }); err != nil {
		return err
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Offsets are committed by hand after OpenSearch confirms the writes,
		// and a rebalance cannot revoke partitions mid-batch.
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := backoff.Default.Retry(ctx, func() error {
		return kafkautil.EnsureTopics(ctx, cl, kafkautil.Topic{Name: dlq, Partitions: 1, ReplicationFactor: int16(replication)})
	}, func(attempt int, err error) { log.Warn("waiting for kafka", "attempt", attempt, "error", err) }); err != nil {
		return err
	}

	ix := &indexer.Indexer{
		Kafka: cl,
		OS:    indexer.OpenSearchBulk{Client: osc},
		Cfg:   indexer.Config{IndexPrefix: prefix, DLQTopic: dlq, MaxPoll: maxPoll, Backoff: backoff.Default, ShutdownGrace: grace},
		Log:   log,
		Now:   time.Now,
	}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return ix.Run(gctx) })
	g.Go(func() error {
		indexer.ReportLag(gctx, kadm.NewClient(cl), group, 10*time.Second, log)
		return nil
	})
	g.Go(func() error {
		ready := func(ctx context.Context) error {
			return errors.Join(kafkautil.Ready(ctx, cl), osutil.Ready(ctx, osc))
		}
		return obs.Serve(gctx, &http.Server{Addr: admin, Handler: obs.AdminHandler(ready), ReadHeaderTimeout: 5 * time.Second}, 5*time.Second)
	})
	return g.Wait()
}
