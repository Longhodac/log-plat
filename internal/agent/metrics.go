package agent

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	linesRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "logplat_agent_lines_read_total",
		Help: "Lines read from tailed files.",
	}, []string{"source"})
	batchesSpooled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_agent_batches_spooled_total",
		Help: "Batches durably appended to the spool.",
	})
	spoolAppendSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "logplat_agent_spool_append_seconds",
		Help:    "Time to append and fsync one batch.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	})
	spoolFullWaits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_agent_spool_full_total",
		Help: "Times the spool was full and reading paused.",
	})
	spoolBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "logplat_agent_spool_bytes",
		Help: "Bytes held in spool segments, including acked bytes not yet reclaimed.",
	})
	batchesSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_agent_batches_sent_total",
		Help: "Batches sent to the collector, including resends.",
	})
	entriesAcked = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_agent_entries_acked_total",
		Help: "Entries the collector confirmed as written to Kafka.",
	})
	entriesRejected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_agent_entries_rejected_total",
		Help: "Entries the collector rejected as invalid.",
	})
	ackSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "logplat_agent_ack_seconds",
		Help:    "Time from sending a batch to receiving its ack.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	})
	sendErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_agent_stream_errors_total",
		Help: "Collector streams that ended with an error.",
	})
	corruptRecords = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_agent_corrupt_records_total",
		Help: "Spool records that failed to decode and were skipped.",
	})
)
