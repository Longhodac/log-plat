//go:build chaos

package chaos

import (
	"context"
	"errors"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
)

// scenario is one way to hurt the stack. inject starts the fault and heal ends
// it. mustStall means the fault is a hard outage, so the test also asserts that
// indexing fell far behind while it lasted.
type scenario struct {
	name      string
	mustStall bool
	// wantRedundant means the scenario is built to make a component deliver
	// entries twice, so it also asserts that OpenSearch absorbed some.
	wantRedundant bool
	osDown        bool // OpenSearch itself is unreachable, so counting during the fault is impossible
	hold          time.Duration
	inject        func(ctx context.Context, e *Env) error
	heal          func(ctx context.Context, e *Env) error
}

func killService(svc string) (inject, heal func(context.Context, *Env) error) {
	return func(ctx context.Context, e *Env) error { return e.kill(ctx, svc) },
		func(ctx context.Context, e *Env) error { return e.start(ctx, svc) }
}

func proxyDown(name string) (inject, heal func(context.Context, *Env) error) {
	toggle := func(enable bool) func(context.Context, *Env) error {
		return func(_ context.Context, e *Env) error {
			p, err := e.tox.Proxy(name)
			if err != nil {
				return err
			}
			if enable {
				return p.Enable()
			}
			return p.Disable()
		}
	}
	return toggle(false), toggle(true)
}

// proxyToxic adds a toxic to both directions of a proxy and removes it on heal.
func proxyToxic(proxy, kind string, attrs toxiproxy.Attributes) (inject, heal func(context.Context, *Env) error) {
	return proxyToxicOn(proxy, kind, []string{"upstream", "downstream"}, attrs)
}

// proxyToxicOn limits the toxic to the given directions. Upstream is client to
// server and downstream is server to client.
func proxyToxicOn(proxy, kind string, streams []string, attrs toxiproxy.Attributes) (inject, heal func(context.Context, *Env) error) {
	name := func(stream string) string { return kind + "-" + stream }
	return func(_ context.Context, e *Env) error {
			p, err := e.tox.Proxy(proxy)
			if err != nil {
				return err
			}
			for _, stream := range streams {
				if _, err := p.AddToxic(name(stream), kind, stream, 1.0, attrs); err != nil {
					return err
				}
			}
			return nil
		}, func(_ context.Context, e *Env) error {
			p, err := e.tox.Proxy(proxy)
			if err != nil {
				return err
			}
			for _, stream := range streams {
				if err := p.RemoveToxic(name(stream)); err != nil {
					return err
				}
			}
			return nil
		}
}

// crashLoop kills and restarts a service several times in a row. The restart
// happens inside inject, so heal has nothing left to do.
func crashLoop(svc string, times int) (inject, heal func(context.Context, *Env) error) {
	return func(ctx context.Context, e *Env) error {
		for range times {
			if err := e.kill(ctx, svc); err != nil {
				return err
			}
			time.Sleep(time.Second)
			if err := e.start(ctx, svc); err != nil {
				return err
			}
			time.Sleep(time.Second)
		}
		return nil
	}, func(context.Context, *Env) error { return nil }
}

func scenarios() []scenario {
	var all []scenario
	add := func(name string, mustStall bool, inject, heal func(context.Context, *Env) error) {
		all = append(all, scenario{name: name, mustStall: mustStall, hold: 8 * time.Second, inject: inject, heal: heal})
	}

	// Process crashes: SIGKILL, no graceful shutdown.
	for _, svc := range []string{"collector", "kafka", "indexer", "agent"} {
		i, h := killService(svc)
		add("kill-"+svc, true, i, h)
	}
	i, h := killService("opensearch")
	all = append(all, scenario{name: "kill-opensearch", mustStall: true, osDown: true, hold: 8 * time.Second, inject: i, heal: h})

	// Network outages: the connection is refused until healed.
	for _, hop := range []string{"collector", "kafka"} {
		i, h := proxyDown(hop)
		add("net-"+hop+"-down", true, i, h)
	}
	i, h = proxyDown("opensearch")
	add("net-opensearch-down", true, i, h)

	// Degraded networks: data still flows, badly.
	i, h = proxyToxic("collector", "latency", toxiproxy.Attributes{"latency": 800, "jitter": 400})
	add("net-collector-latency", false, i, h)
	i, h = proxyToxic("collector", "reset_peer", toxiproxy.Attributes{"timeout": 300})
	add("net-collector-resets", false, i, h)
	i, h = proxyToxic("kafka", "bandwidth", toxiproxy.Attributes{"rate": 50})
	add("net-kafka-bandwidth", false, i, h)
	i, h = proxyToxic("opensearch", "timeout", toxiproxy.Attributes{"timeout": 0})
	add("net-opensearch-blackhole", true, i, h)

	// Faults that make a component deliver the same entries twice. Killing a
	// process or cutting a link between a write and its acknowledgement forces
	// a resend, and the duplicates must collapse in OpenSearch.
	i, h = ackLoss()
	all = append(all, scenario{name: "net-collector-ack-loss", wantRedundant: true, hold: 8 * time.Second, inject: i, heal: h})
	all = append(all, killIndexerAfterWriteBeforeCommit())
	i, h = crashLoop("collector", 3)
	all = append(all, scenario{name: "crash-loop-collector", hold: time.Second, inject: i, heal: h})
	i, h = crashLoop("indexer", 3)
	all = append(all, scenario{name: "crash-loop-indexer", hold: time.Second, inject: i, heal: h})

	// Several failures in a row, healed together.
	all = append(all, scenario{
		name: "kill-everything-in-sequence", mustStall: true, hold: 6 * time.Second,
		inject: func(ctx context.Context, e *Env) error {
			for _, svc := range []string{"collector", "kafka", "indexer"} {
				if err := e.kill(ctx, svc); err != nil {
					return err
				}
				time.Sleep(2 * time.Second)
			}
			return nil
		},
		heal: func(ctx context.Context, e *Env) error {
			for _, svc := range []string{"kafka", "collector", "indexer"} {
				if err := e.start(ctx, svc); err != nil {
					return err
				}
			}
			return nil
		},
	})
	return all
}

// killIndexerAfterWriteBeforeCommit delays OpenSearch's responses by three
// seconds. Each bulk request is indexed at once, but the indexer waits for the
// reply before it commits offsets. The scenario watches for a write that has
// landed in OpenSearch while the indexer has not yet seen its response, and
// kills the indexer at that moment. Kafka must then redeliver the written but
// uncommitted records, and the document IDs must absorb the repeat writes.
func killIndexerAfterWriteBeforeCommit() scenario {
	slow, unslow := proxyToxicOn("opensearch", "latency", []string{"downstream"}, toxiproxy.Attributes{"latency": 3000})
	return scenario{
		name: "kill-indexer-after-write-before-commit", wantRedundant: true, hold: time.Second,
		inject: func(ctx context.Context, e *Env) error {
			if err := slow(ctx, e); err != nil {
				return err
			}
			if err := e.waitForUnackedWrite(ctx, 30*time.Second); err != nil {
				return err
			}
			if err := e.kill(ctx, "indexer"); err != nil {
				return err
			}
			if err := unslow(ctx, e); err != nil {
				return err
			}
			return e.start(ctx, "indexer")
		},
		heal: func(context.Context, *Env) error { return nil },
	}
}

// ackLoss cuts the agent's connection to the collector at a moment when the
// collector has published entries to Kafka that the agent has not yet seen
// acknowledged. Those entries are in Kafka, the agent still holds them, and it
// must send them again, so OpenSearch has to absorb the repeats.
//
// A fixed-time reset hit that window in only about half of the runs, and adding
// a delay to the acks did not fix it, so the scenario watches the two counters
// for the window instead. Delaying acks by 300 ms keeps the window open long
// enough to catch.
func ackLoss() (inject, heal func(context.Context, *Env) error) {
	slow, unslow := proxyToxicOn("collector", "latency", []string{"downstream"}, toxiproxy.Attributes{"latency": 300})
	return func(ctx context.Context, e *Env) error {
			gap := func() (float64, error) {
				pub, err := e.metric(ctx, "collector", "logplat_collector_entries_total", `result="published"`)
				if err != nil {
					return 0, err
				}
				acked, err := e.metric(ctx, "agent", "logplat_agent_entries_acked_total", "")
				return pub - acked, err
			}
			base, err := gap()
			if err != nil {
				return err
			}
			if err := slow(ctx, e); err != nil {
				return err
			}
			deadline := time.Now().Add(30 * time.Second)
			for {
				g, err := gap()
				if err != nil {
					return err
				}
				if g > base {
					break
				}
				if time.Now().After(deadline) {
					return errors.New("never saw published entries waiting for their ack")
				}
				time.Sleep(10 * time.Millisecond)
			}
			p, err := e.tox.Proxy("collector")
			if err != nil {
				return err
			}
			if err := p.Disable(); err != nil {
				return err
			}
			time.Sleep(300 * time.Millisecond)
			return p.Enable()
		}, func(ctx context.Context, e *Env) error {
			return unslow(ctx, e)
		}
}
