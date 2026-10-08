package tail

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	clients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "logplat_tail_clients",
		Help: "Connected live-tail clients.",
	})
	consumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_tail_records_consumed_total",
		Help: "Records read from Kafka by the tail service.",
	})
	delivered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_tail_events_delivered_total",
		Help: "Events placed in a client's buffer.",
	})
	dropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_tail_events_dropped_total",
		Help: "Events a slow client lost because its buffer was full.",
	})
	deliverySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "logplat_tail_delivery_seconds",
		Help:    "Time from the agent reading a line to the tail service handing it to the hub.",
		Buckets: prometheus.ExponentialBuckets(0.005, 1.4, 30),
	})
	decodeErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logplat_tail_decode_errors_total",
		Help: "Kafka records that were not valid log entries.",
	})
)
