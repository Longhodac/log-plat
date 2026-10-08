package alert

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/doc"
)

var (
	alertsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "logplat_alerter_alerts_total",
		Help: "Alerts by kind (firing or resolved) and outcome (sent, failed, or dropped when the send queue was full).",
	}, []string{"kind", "outcome"})
	errorsObserved = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "logplat_alerter_errors_observed_total",
		Help: "Error and fatal entries counted, by service.",
	}, []string{"service"})
	notifySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "logplat_alerter_notify_seconds",
		Help:    "Time to deliver one alert, including retries.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	})
	trackedServices = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "logplat_alerter_services_tracked",
		Help: "Services with recent errors that the detector is tracking.",
	})
)

// NewClient joins the alerter consumer group on topic.
//
// Records are keyed by service, so one service's entries all sit in one
// partition and therefore reach exactly one alerter instance. Counting errors
// per service needs no coordination between instances. The group resumes from
// its committed offsets after a restart. Entries older than the detector's
// look-back are ignored, so catching up on a backlog raises no stale alerts.
func NewClient(brokers []string, topic, group string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
}

// Consume feeds the detector until ctx ends.
func Consume(ctx context.Context, cl *kgo.Client, det *Detector, log *slog.Logger) {
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Warn("fetch error", "topic", topic, "partition", p, "error", err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			var e logplatv1.LogEntry
			if proto.Unmarshal(r.Value, &e) != nil {
				return
			}
			level := doc.LevelName(e.GetLevel())
			if level != "error" && level != "fatal" {
				return
			}
			at := e.GetObservedAt().AsTime()
			if !e.GetObservedAt().IsValid() {
				at = time.Now()
			}
			errorsObserved.WithLabelValues(e.GetService()).Inc()
			det.Observe(e.GetService(), level, at, e.GetMessage())
		})
	}
}

// Loop evaluates the detector every interval and sends what it finds. Sending
// happens on its own goroutine so a slow webhook cannot stall detection.
func Loop(ctx context.Context, det *Detector, n Notifier, every time.Duration, log *slog.Logger) {
	queue := make(chan Alert, 64)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for a := range queue {
			start := time.Now()
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
			err := n.Notify(sctx, a)
			cancel()
			notifySeconds.Observe(time.Since(start).Seconds())
			if err != nil {
				alertsTotal.WithLabelValues(string(a.Kind), "failed").Inc()
				log.Error("alert delivery failed", "kind", a.Kind, "service", a.Service, "error", err)
				continue
			}
			alertsTotal.WithLabelValues(string(a.Kind), "sent").Inc()
		}
	}()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			close(queue)
			<-done // let an alert already queued finish sending
			return
		case <-t.C:
			for _, a := range det.Evaluate() {
				select {
				case queue <- a:
				default:
					alertsTotal.WithLabelValues(string(a.Kind), "dropped").Inc()
					log.Error("alert queue full; dropping", "kind", a.Kind, "service", a.Service)
				}
			}
			trackedServices.Set(float64(det.Services()))
		}
	}
}
