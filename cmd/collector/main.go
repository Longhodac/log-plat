// Command collector accepts agent streams over gRPC and publishes to Kafka.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/agent"
	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/cache"
	"github.com/Longhodac/log-plat/internal/collector"
	"github.com/Longhodac/log-plat/internal/envcfg"
	"github.com/Longhodac/log-plat/internal/kafkautil"
	"github.com/Longhodac/log-plat/internal/obs"
	"github.com/Longhodac/log-plat/internal/ratelimit"
)

func main() { obs.Main("collector", run) }

func run(ctx context.Context, log *slog.Logger) error {
	var env envcfg.Reader
	var (
		listen      = env.String("COLLECTOR_LISTEN_ADDR", ":7070")
		admin       = env.String("ADMIN_ADDR", ":9100")
		brokers     = env.List("KAFKA_BROKERS", []string{"localhost:9094"})
		topic       = env.String("KAFKA_TOPIC", "logs")
		partitions  = env.Int("KAFKA_PARTITIONS", 6)
		replication = env.Int("KAFKA_REPLICATION_FACTOR", 1)
		keySpec     = env.Required("COLLECTOR_API_KEYS")
		maxInflight = env.Int("COLLECTOR_MAX_INFLIGHT", 16)
		delivery    = env.Duration("KAFKA_DELIVERY_TIMEOUT", 30*time.Second)
		grace       = env.Duration("SHUTDOWN_GRACE", 20*time.Second)
		redisAddr   = env.String("REDIS_ADDR", "localhost:6379")
		// Entries per second each service may publish. 0 turns limiting off.
		rate = env.Float("COLLECTOR_RATE_LIMIT", 0)
		// Bucket size, which is also the largest burst. It should be at least a
		// full agent batch (1000 entries by default) so one batch can pass whole.
		burst = env.Int("COLLECTOR_RATE_BURST", 5000)
	)
	if err := env.Err(); err != nil {
		return err
	}
	keys, err := apikey.Parse(keySpec)
	if err != nil {
		return err
	}

	cl, err := kgo.NewClient(append(collector.ProducerOpts(delivery), kgo.SeedBrokers(brokers...))...)
	if err != nil {
		return err
	}
	defer cl.Close()
	err = backoff.Default.Retry(ctx, func() error {
		return kafkautil.EnsureTopics(ctx, cl, kafkautil.Topic{
			Name: topic, Partitions: int32(partitions), ReplicationFactor: int16(replication),
			Configs: map[string]*string{"min.insync.replicas": kafkautil.Ptr("1")},
		})
	}, func(attempt int, err error) { log.Warn("waiting for kafka", "attempt", attempt, "error", err) })
	if err != nil {
		return err
	}

	srv := grpc.NewServer(
		grpc.StreamInterceptor(collector.StreamAuth(keys, agent.APIKeyHeader)),
		grpc.MaxRecvMsgSize(16<<20),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	)
	cs := &collector.Server{
		Pub: &collector.KafkaPublisher{Client: cl, Topic: topic}, MaxInflight: maxInflight, Log: log, Now: time.Now,
	}
	if rate > 0 {
		rdb := cache.NewClient(redisAddr)
		defer rdb.Close()
		if cs.Limit, err = ratelimit.New(rdb, "collector", rate, burst); err != nil {
			return err
		}
		log.Info("rate limiting on", "entries_per_second", rate, "burst", burst, "redis", redisAddr)
	}
	logplatv1.RegisterIngestServiceServer(srv, cs)
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("grpc listening", "addr", listen)
		return srv.Serve(lis)
	})
	g.Go(func() error {
		<-gctx.Done()
		// GracefulStop lets in-flight batches finish publishing and acking.
		stopped := make(chan struct{})
		go func() { srv.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(grace):
			log.Warn("graceful stop timed out; closing streams")
			srv.Stop()
		}
		return nil
	})
	g.Go(func() error {
		return obs.Serve(gctx, &http.Server{
			Addr:              admin,
			Handler:           obs.AdminHandler(func(ctx context.Context) error { return kafkautil.Ready(ctx, cl) }),
			ReadHeaderTimeout: 5 * time.Second,
		}, grace)
	})
	err = g.Wait()
	flushCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	if ferr := cl.Flush(flushCtx); ferr != nil {
		log.Warn("kafka flush at shutdown", "error", ferr)
	}
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}
