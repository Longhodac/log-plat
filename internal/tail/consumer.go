package tail

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
)

// NewClient returns a Kafka client that reads every partition of topic from
// its current end. It joins no consumer group and commits nothing, so every
// tail instance sees every record and none interferes with the indexer.
func NewClient(brokers []string, topic string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.MetadataMinAge(time.Second),
	)
}

// Consume publishes every record to hub until ctx ends.
func Consume(ctx context.Context, cl *kgo.Client, hub *Hub, log *slog.Logger) {
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
		now := time.Now()
		fetches.EachRecord(func(r *kgo.Record) {
			consumed.Inc()
			var e logplatv1.LogEntry
			if err := proto.Unmarshal(r.Value, &e); err != nil {
				decodeErrors.Inc()
				return
			}
			ev := FromEntry(&e)
			if !ev.ObservedAt.IsZero() {
				deliverySeconds.Observe(max(now.Sub(ev.ObservedAt), 0).Seconds())
			}
			hub.Publish(ev)
		})
	}
}
