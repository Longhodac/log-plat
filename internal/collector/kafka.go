package collector

import (
	"context"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
)

// KafkaPublisher writes each entry as one record keyed by service, so the
// default murmur2 partitioner keeps a service's logs in one partition.
type KafkaPublisher struct {
	Client *kgo.Client
	Topic  string
}

// ProducerOpts are the durability settings the collector's ack depends on:
// acks from all in-sync replicas and the idempotent producer, so the
// client's own retries cannot write a record twice.
func ProducerOpts(deliveryTimeout time.Duration) []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.MaxBufferedRecords(100_000),
	}
}

// Publish implements Publisher.
func (p *KafkaPublisher) Publish(ctx context.Context, entries []*logplatv1.LogEntry) func() error {
	start := time.Now()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	wg.Add(len(entries))
	for _, e := range entries {
		val, err := proto.Marshal(e)
		if err != nil {
			fail(err)
			wg.Done()
			continue
		}
		rec := &kgo.Record{Topic: p.Topic, Key: []byte(e.GetService()), Value: val}
		p.Client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err != nil {
				fail(err)
			}
			wg.Done()
		})
	}
	return func() error {
		wg.Wait()
		publishSeconds.Observe(time.Since(start).Seconds())
		return firstErr
	}
}
