//go:build integration

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/alert"
	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/kafkautil"
	"github.com/Longhodac/log-plat/internal/logid"
	"github.com/Longhodac/log-plat/internal/tail"
)

func producer(t *testing.T, ctx context.Context, topic string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	if err := kafkautil.EnsureTopics(ctx, cl, kafkautil.Topic{Name: topic, Partitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	return cl
}

var seq int64

func send(t *testing.T, ctx context.Context, cl *kgo.Client, topic, service string, level logplatv1.Level, msg string) string {
	t.Helper()
	seq++
	now := timestamppb.Now()
	id := logid.New("it", "/src/"+service, 0, seq)
	v, err := proto.Marshal(&logplatv1.LogEntry{
		Id: id, Service: service, Message: msg, Timestamp: now, ObservedAt: now, Level: level, Source: "/src", Host: "h1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(service), Value: v}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestLiveTailDeliversMatchingKafkaRecordsToAClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	topic := uniq(t)
	prod := producer(t, ctx, topic)

	cl, err := tail.NewClient(brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	hub := tail.NewHub(10)
	go tail.Consume(ctx, cl, hub, discard())

	// The consumer starts at the end of the topic, so wait until it is really
	// reading before sending anything that matters.
	probe, err := hub.Subscribe(tail.Filter{Services: map[string]bool{"probe": true}}, 10)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 60*time.Second, "the tail consumer to start reading", func() (bool, error) {
		send(t, ctx, prod, topic, "probe", logplatv1.Level_LEVEL_INFO, "ready?")
		select {
		case <-probe.C:
			return true, nil
		case <-time.After(300 * time.Millisecond):
			return false, nil
		}
	})
	hub.Unsubscribe(probe)

	keys, _ := apikey.Parse("k:ops")
	srv := httptest.NewServer((&tail.Server{Hub: hub, Keys: keys, Log: discard(), Buffer: 100, Heartbeat: time.Second}).Handler())
	defer srv.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/tail?service=hdfs&level=error&q=DISK", nil)
	req.Header.Set("X-API-Key", "k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	if line, _ := r.ReadString('\n'); !strings.HasPrefix(line, ": connected") {
		t.Fatalf("first line = %q, want the connected comment", line)
	}

	want := map[string]bool{}
	for i := range 10 {
		want[send(t, ctx, prod, topic, "hdfs", logplatv1.Level_LEVEL_ERROR, fmt.Sprintf("disk %d failed", i))] = true
		send(t, ctx, prod, topic, "hdfs", logplatv1.Level_LEVEL_INFO, "disk is fine")
		send(t, ctx, prod, topic, "apache", logplatv1.Level_LEVEL_ERROR, "disk full on another service")
		send(t, ctx, prod, topic, "hdfs", logplatv1.Level_LEVEL_ERROR, "network flap")
	}

	got := map[string]bool{}
	events := make(chan tail.Event)
	go func() {
		defer close(events)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			if data, ok := strings.CutPrefix(strings.TrimSpace(line), "data: "); ok {
				var ev tail.Event
				if json.Unmarshal([]byte(data), &ev) == nil {
					events <- ev
				}
			}
		}
	}()
	deadline := time.After(20 * time.Second)
	for len(got) < len(want) {
		select {
		case ev := <-events:
			if !want[ev.ID] {
				t.Fatalf("received %+v, which does not match service=hdfs, level=error, q=disk", ev)
			}
			got[ev.ID] = true
		case <-deadline:
			t.Fatalf("received %d of %d matching events", len(got), len(want))
		}
	}
	select {
	case ev := <-events:
		t.Errorf("an extra event arrived: %+v", ev)
	case <-time.After(1500 * time.Millisecond):
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 0))
}

type slackCatcher struct {
	mu   sync.Mutex
	msgs []string
}

func (c *slackCatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var p map[string]string
	_ = json.NewDecoder(r.Body).Decode(&p)
	c.mu.Lock()
	c.msgs = append(c.msgs, p["text"])
	c.mu.Unlock()
}

func (c *slackCatcher) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.msgs...)
}

func TestAlerterSendsOneSlackMessageForASpikeAndOneWhenItEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	topic := uniq(t)
	prod := producer(t, ctx, topic)

	slack := &slackCatcher{}
	hook := httptest.NewServer(slack)
	defer hook.Close()

	cfg := alert.Config{Window: 4 * time.Second, Bucket: 500 * time.Millisecond, Baseline: 20 * time.Second, MinErrors: 10, Ratio: 3, Cooldown: 2 * time.Second, Samples: 2}
	det := alert.NewDetector(cfg, time.Now)
	cl, err := alert.NewClient(brokers, topic, topic+"-alerter")
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	go alert.Consume(ctx, cl, det, discard())
	notifier := &alert.Slack{URL: hook.URL, Client: http.DefaultClient, MaxAttempts: 3, Backoff: backoff.Policy{Base: 50 * time.Millisecond, Max: time.Second}}
	go alert.Loop(ctx, det, notifier, 500*time.Millisecond, discard())

	eventually(t, 90*time.Second, "the alerter to join its group and read", func() (bool, error) {
		send(t, ctx, prod, topic, "probe", logplatv1.Level_LEVEL_ERROR, "probe")
		time.Sleep(300 * time.Millisecond)
		return det.Services() > 0, nil
	})

	// Information and warnings, however many, are not errors.
	for range 200 {
		send(t, ctx, prod, topic, "hdfs", logplatv1.Level_LEVEL_WARN, "warn only")
	}
	for i := range 40 {
		send(t, ctx, prod, topic, "hdfs", logplatv1.Level_LEVEL_ERROR, fmt.Sprintf("replica %d <!channel> lost", i))
	}
	eventually(t, 20*time.Second, "a spike alert in Slack", func() (bool, error) {
		return len(slack.snapshot()) >= 1, nil
	})
	first := slack.snapshot()[0]
	if !strings.Contains(first, "Error spike in `hdfs`") || strings.Contains(first, "<!channel>") {
		t.Errorf("first message = %q; want a hdfs spike with the log text escaped", first)
	}

	// The spike goes on for several seconds. That must not send a second message.
	for range 6 {
		for range 8 {
			send(t, ctx, prod, topic, "hdfs", logplatv1.Level_LEVEL_ERROR, "still failing")
		}
		time.Sleep(500 * time.Millisecond)
	}
	if n := len(slack.snapshot()); n != 1 {
		t.Fatalf("%d messages during one sustained spike, want 1: %q", n, slack.snapshot())
	}

	eventually(t, 30*time.Second, "the all-clear message", func() (bool, error) {
		return len(slack.snapshot()) >= 2, nil
	})
	time.Sleep(2 * time.Second)
	msgs := slack.snapshot()
	if len(msgs) != 2 || !strings.Contains(msgs[1], "back to normal") {
		t.Errorf("messages = %q; want exactly a spike alert then an all-clear", msgs)
	}
}
