// Command tail streams live log entries to clients over Server-Sent Events.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/envcfg"
	"github.com/Longhodac/log-plat/internal/kafkautil"
	"github.com/Longhodac/log-plat/internal/obs"
	"github.com/Longhodac/log-plat/internal/tail"
)

func main() { obs.Main("tail", run) }

func run(ctx context.Context, log *slog.Logger) error {
	var env envcfg.Reader
	var (
		listen     = env.String("TAIL_LISTEN_ADDR", ":8082")
		brokers    = env.List("KAFKA_BROKERS", []string{"localhost:9094"})
		topic      = env.String("KAFKA_TOPIC", "logs")
		keySpec    = env.Required("TAIL_API_KEYS")
		maxClients = env.Int("TAIL_MAX_CLIENTS", 100)
		buffer     = env.Int("TAIL_CLIENT_BUFFER", 5000)
		heartbeat  = env.Duration("TAIL_HEARTBEAT", 15*time.Second)
		grace      = env.Duration("SHUTDOWN_GRACE", 10*time.Second)
	)
	if err := env.Err(); err != nil {
		return err
	}
	keys, err := apikey.Parse(keySpec)
	if err != nil {
		return err
	}
	cl, err := tail.NewClient(brokers, topic)
	if err != nil {
		return err
	}
	defer cl.Close()

	hub := tail.NewHub(maxClients)
	srv := &tail.Server{
		Hub: hub, Keys: keys, Log: log, Buffer: buffer, Heartbeat: heartbeat,
		Ready: func(ctx context.Context) error { return kafkautil.Ready(ctx, cl) },
	}
	// Streams never finish on their own, so they must be told to stop when
	// shutdown starts or the server would wait out the whole grace period.
	streams, stopStreams := context.WithCancel(context.Background())
	defer stopStreams()
	httpSrv := &http.Server{
		Addr:              listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(_ net.Listener) context.Context { return streams },
	}
	httpSrv.RegisterOnShutdown(stopStreams)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		tail.Consume(gctx, cl, hub, log)
		return nil
	})
	g.Go(func() error {
		log.Info("tail listening", "addr", listen, "max_clients", maxClients)
		return obs.Serve(gctx, httpSrv, grace)
	})
	return g.Wait()
}
