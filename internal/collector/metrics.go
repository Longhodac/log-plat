package collector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	entriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "logplat_collector_entries_total",
		Help: "Entries by outcome: published (Kafka acked) or rejected (failed validation).",
	}, []string{"service", "result"})
	batchesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_collector_batches_total",
		Help: "Batches received.",
	})
	batchSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "logplat_collector_batch_seconds",
		Help:    "Time from receiving a batch to sending its ack.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	})
	publishSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "logplat_collector_publish_seconds",
		Help:    "Time for Kafka to acknowledge every record in a batch.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	})
	publishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_collector_publish_errors_total",
		Help: "Batches whose Kafka write failed.",
	})
	agentDwell = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "logplat_collector_agent_dwell_seconds",
		Help:    "Time from the agent reading a line to the collector receiving its batch: tailer poll, batching, spool, and the send.",
		Buckets: prometheus.ExponentialBuckets(0.005, 1.4, 30),
	})
	throttleSeconds = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "logplat_collector_throttle_seconds_total",
		Help: "Seconds batches spent waiting for a service's rate limit.",
	}, []string{"service"})
	authFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_collector_auth_failures_total",
		Help: "Streams rejected for a missing or unknown API key.",
	})
	streamsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "logplat_collector_streams_active",
		Help: "Open agent streams.",
	})
)
