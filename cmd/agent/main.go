// Command agent tails log files and ships them to the collector.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/agent"
	"github.com/Longhodac/log-plat/internal/envcfg"
	"github.com/Longhodac/log-plat/internal/obs"
)

func main() { obs.Main("agent", run) }

func run(ctx context.Context, log *slog.Logger) error {
	host, _ := os.Hostname()
	var env envcfg.Reader
	cfg := agent.Config{
		AgentID:           env.String("AGENT_ID", host),
		Host:              host,
		Paths:             env.List("AGENT_PATHS", nil),
		StateDir:          env.String("AGENT_STATE_DIR", "/var/lib/logplat-agent"),
		SpoolMaxBytes:     int64(env.Int("AGENT_SPOOL_MAX_BYTES", 1<<30)),
		SpoolSegmentBytes: int64(env.Int("AGENT_SPOOL_SEGMENT_BYTES", 16<<20)),
		BatchMaxEntries:   env.Int("AGENT_BATCH_MAX_ENTRIES", 1000),
		BatchMaxBytes:     env.Int("AGENT_BATCH_MAX_BYTES", 1<<20),
		BatchLinger:       env.Duration("AGENT_BATCH_LINGER", 200*time.Millisecond),
		Window:            env.Int("AGENT_WINDOW", 8),
		PollInterval:      env.Duration("AGENT_POLL_INTERVAL", 100*time.Millisecond),
		DrainTimeout:      env.Duration("AGENT_DRAIN_TIMEOUT", 10*time.Second),
	}
	addr := env.String("COLLECTOR_ADDR", "localhost:7070")
	key := env.Required("AGENT_API_KEY")
	admin := env.String("ADMIN_ADDR", ":9100")
	if err := env.Err(); err != nil {
		return err
	}

	// Plaintext is for local development only; production would add TLS here.
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(16<<20)),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	a, err := agent.New(cfg, logplatv1.NewIngestServiceClient(conn), key, log)
	if err != nil {
		return err
	}
	log.Info("agent configured", "agent_id", cfg.AgentID, "paths", cfg.Paths, "collector", addr)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return a.Run(gctx) })
	g.Go(func() error {
		return obs.Serve(gctx, &http.Server{Addr: admin, Handler: obs.AdminHandler(nil), ReadHeaderTimeout: 5 * time.Second}, 5*time.Second)
	})
	return g.Wait()
}
