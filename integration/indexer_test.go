//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/kafkautil"
	"github.com/Longhodac/log-plat/internal/logid"
	"github.com/Longhodac/log-plat/internal/zeroloss"
)

// Redelivery is the normal failure mode of at-least-once delivery: the same
// entry reaches Kafka twice (agent resend) or is consumed twice (crash before
// commit). Either way OpenSearch must end up with one document per ID.
func TestRedeliveredRecordsCollapseAndGarbageIsDeadLettered(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	name := uniq(t)
	prefix := "logs-" + name

	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	if err := kafkautil.EnsureTopics(ctx, prod, kafkautil.Topic{Name: name, Partitions: 2, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}

	const n = 300
	ts := timestamppb.New(time.Date(2008, 11, 9, 20, 36, 15, 0, time.UTC))
	var values [][]byte
	var ids []string
	for i := range n {
		e := &logplatv1.LogEntry{
			Id: logid.New("redeliver", "/src", 0, int64(i)), Service: "hdfs", Message: "m", Timestamp: ts, ObservedAt: timestamppb.Now(),
			Level: logplatv1.Level_LEVEL_INFO, Source: "/src", Offset: int64(i),
		}
		v, _ := proto.Marshal(e)
		values = append(values, v)
		ids = append(ids, e.Id)
	}
	garbage := []byte("not a protobuf \xff\xff")
	// The client owns a record once produced, so each delivery is a new one.
	var all []*kgo.Record
	for _, v := range append(append(append([][]byte{}, values...), garbage), values...) {
		all = append(all, &kgo.Record{Topic: name, Key: []byte("hdfs"), Value: v})
	}
	if err := prod.ProduceSync(ctx, all...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	consumer := startIndexer(t, ctx, name, prefix)
	osc := openSearch(t, prefix)
	report, err := zeroloss.Checker{Client: osc, IndexPrefix: prefix}.Check(ctx, "", "/src", ids, zeroloss.Options{
		Poll: 500 * time.Millisecond, Stall: 20 * time.Second, Timeout: time.Minute, Log: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.ZeroLoss || report.Indexed != n {
		t.Fatalf("%d records produced twice: report %+v; want exactly %d documents", n, report, n)
	}

	dlq, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(name+"-dlq"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer dlq.Close()
	pollCtx, pollCancel := context.WithTimeout(ctx, 30*time.Second)
	defer pollCancel()
	dead := dlq.PollFetches(pollCtx).Records()
	if len(dead) != 1 || string(dead[0].Value) != string(garbage) {
		t.Fatalf("dead-letter topic holds %d records, want the 1 garbage record", len(dead))
	}

	adm := kadm.NewClient(consumer)
	eventually(t, 30*time.Second, "offsets committed past every record", func() (bool, error) {
		lags, err := adm.Lag(ctx, name+"-indexer")
		if err != nil {
			return false, err
		}
		total := int64(0)
		lags.Each(func(l kadm.DescribedGroupLag) { total += l.Lag.Total() })
		return total == 0, nil
	})
}
