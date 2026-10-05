// Package kafkautil holds Kafka admin helpers shared by the services.
package kafkautil

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic describes a topic the service needs to exist.
type Topic struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]*string
}

// EnsureTopics creates any missing topics. Existing topics are left as they
// are, so running it on every start is safe.
func EnsureTopics(ctx context.Context, cl *kgo.Client, topics ...Topic) error {
	adm := kadm.NewClient(cl)
	for _, t := range topics {
		res, err := adm.CreateTopic(ctx, t.Partitions, t.ReplicationFactor, t.Configs, t.Name)
		if err == nil {
			err = res.Err
		}
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", t.Name, err)
		}
	}
	return nil
}

// Ready returns nil when at least one broker answers a metadata request.
func Ready(ctx context.Context, cl *kgo.Client) error {
	_, err := kadm.NewClient(cl).BrokerMetadata(ctx)
	return err
}

// Ptr returns a pointer to s, for topic configs.
func Ptr(s string) *string { return &s }
