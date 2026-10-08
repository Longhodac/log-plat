package indexer

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
)

var (
	recordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "logplat_indexer_records_total",
		Help: "Records by outcome: indexed or dead_lettered.",
	}, []string{"result"})
	bulkSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "logplat_indexer_bulk_seconds",
		Help:    "Latency of one _bulk request.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	})
	bulkRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_indexer_bulk_retries_total",
		Help: "Bulk attempts that retried failed items.",
	})
	commitErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_indexer_commit_errors_total",
		Help: "Offset commits that failed.",
	})
	e2eSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "logplat_indexer_end_to_end_seconds",
		Help:    "Time from the agent reading a line to it being indexed.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
	})
	phaseSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "logplat_indexer_phase_seconds",
		Help:    "Time per poll cycle spent in each phase: poll (waiting for records), process (encode and bulk write), commit.",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.5, 24),
	}, []string{"phase"})
	kafkaDwell = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "logplat_indexer_kafka_dwell_seconds",
		Help:    "Time from the collector producing a record to the indexer polling it. Uses the Kafka record timestamp.",
		Buckets: prometheus.ExponentialBuckets(0.005, 1.4, 30),
	})
	consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logplat_indexer_consumer_lag",
		Help: "Records between the group's committed offset and the partition end.",
	}, []string{"topic", "partition"})
)

// ReportLag publishes the consumer group's lag every interval until ctx is done.
func ReportLag(ctx context.Context, adm *kadm.Client, group string, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		lags, err := adm.Lag(ctx, group)
		if err != nil {
			log.Warn("lag query failed", "error", err)
			continue
		}
		lags.Each(func(l kadm.DescribedGroupLag) {
			for topic, parts := range l.Lag {
				for p, m := range parts {
					consumerLag.WithLabelValues(topic, strconv.Itoa(int(p))).Set(float64(m.Lag))
				}
			}
		})
	}
}
