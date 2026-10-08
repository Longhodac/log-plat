// Command alerter watches the log stream for error spikes and notifies Slack.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Longhodac/log-plat/internal/alert"
	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/envcfg"
	"github.com/Longhodac/log-plat/internal/kafkautil"
	"github.com/Longhodac/log-plat/internal/obs"
)

func main() { obs.Main("alerter", run) }

func run(ctx context.Context, log *slog.Logger) error {
	var env envcfg.Reader
	cfg := alert.DefaultConfig()
	var (
		admin   = env.String("ADMIN_ADDR", ":9100")
		brokers = env.List("KAFKA_BROKERS", []string{"localhost:9094"})
		topic   = env.String("KAFKA_TOPIC", "logs")
		group   = env.String("ALERTER_GROUP", "alerter")
		// Empty means alerts are only written to the log.
		webhook = env.String("SLACK_WEBHOOK_URL", "")
		every   = env.Duration("ALERT_EVAL_INTERVAL", 5*time.Second)
		grace   = env.Duration("SHUTDOWN_GRACE", 10*time.Second)
	)
	cfg.Window = env.Duration("ALERT_WINDOW", cfg.Window)
	cfg.Bucket = env.Duration("ALERT_BUCKET", cfg.Bucket)
	cfg.Baseline = env.Duration("ALERT_BASELINE", cfg.Baseline)
	cfg.MinErrors = env.Int("ALERT_MIN_ERRORS", cfg.MinErrors)
	cfg.Ratio = env.Float("ALERT_RATIO", cfg.Ratio)
	cfg.Cooldown = env.Duration("ALERT_COOLDOWN", cfg.Cooldown)
	cfg.Samples = env.Int("ALERT_SAMPLES", cfg.Samples)
	if err := env.Err(); err != nil {
		return err
	}

	var notifier alert.Notifier = alert.LogNotifier{Log: log}
	if webhook != "" {
		notifier = &alert.Slack{URL: webhook, Client: &http.Client{Timeout: 10 * time.Second}, MaxAttempts: 5, Backoff: backoff.Policy{Base: time.Second, Max: 30 * time.Second}}
		log.Info("slack notifications on")
	} else {
		log.Warn("SLACK_WEBHOOK_URL is not set; alerts go to the log only")
	}

	cl, err := alert.NewClient(brokers, topic, group)
	if err != nil {
		return err
	}
	defer cl.Close()
	det := alert.NewDetector(cfg, time.Now)
	log.Info("alerting", "window", cfg.Window, "baseline", cfg.Baseline, "min_errors", cfg.MinErrors, "ratio", cfg.Ratio, "cooldown", cfg.Cooldown)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { alert.Consume(gctx, cl, det, log); return nil })
	g.Go(func() error { alert.Loop(gctx, det, notifier, every, log); return nil })
	g.Go(func() error {
		return obs.Serve(gctx, &http.Server{
			Addr:              admin,
			Handler:           obs.AdminHandler(func(ctx context.Context) error { return kafkautil.Ready(ctx, cl) }),
			ReadHeaderTimeout: 5 * time.Second,
		}, grace)
	})
	return g.Wait()
}
