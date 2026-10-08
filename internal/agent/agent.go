// Package agent tails log files, spools batches durably, and streams them to
// the collector.
//
// Data flows tailer -> batcher -> spool -> sender. The batcher is the only
// spool writer and the only registry writer; the sender is the only spool
// reader. A line is acknowledged to nobody until the collector confirms Kafka
// has it, and nothing is deleted from the spool before that.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/spool"
)

// Config controls one agent.
type Config struct {
	AgentID string
	Host    string
	Paths   []string // glob patterns
	// Format is how each line is read. Empty means plain text.
	Format Format
	// DockerLabel, with the docker-json format, limits the agent to containers
	// that carry this "key=value" label. Without it the agent would tail every
	// container on the host, including this platform's own, and feed their logs
	// back into itself.
	DockerLabel string

	StateDir          string
	SpoolMaxBytes     int64
	SpoolSegmentBytes int64

	BatchMaxEntries int
	BatchMaxBytes   int
	BatchLinger     time.Duration

	Window       int
	PollInterval time.Duration
	ScanInterval time.Duration
	ReadSize     int
	DrainTimeout time.Duration
	Backoff      backoff.Policy
}

// Defaults fills unset fields.
func (c *Config) Defaults() {
	set := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	setD := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	if c.Format == "" {
		c.Format = FormatText
	}
	set(&c.BatchMaxEntries, 1000)
	set(&c.BatchMaxBytes, 1<<20)
	set(&c.Window, 8)
	set(&c.ReadSize, 64<<10)
	setD(&c.BatchLinger, 200*time.Millisecond)
	setD(&c.PollInterval, 100*time.Millisecond)
	setD(&c.ScanInterval, 2*time.Second)
	setD(&c.DrainTimeout, 10*time.Second)
	if c.Backoff == (backoff.Policy{}) {
		c.Backoff = backoff.Default
	}
}

// Validate reports configuration that cannot work.
func (c *Config) Validate() error {
	var errs []error
	if c.AgentID == "" {
		errs = append(errs, errors.New("agent ID is required"))
	}
	if len(c.Paths) == 0 {
		errs = append(errs, errors.New("at least one path is required"))
	}
	for _, p := range c.Paths {
		if _, err := filepath.Match(p, ""); err != nil {
			errs = append(errs, fmt.Errorf("bad glob %q: %w", p, err))
		}
	}
	if c.StateDir == "" {
		errs = append(errs, errors.New("state dir is required"))
	}
	return errors.Join(errs...)
}

// Agent owns the spool and registry for its state dir.
type Agent struct {
	cfg    Config
	log    *slog.Logger
	spool  *spool.Spool
	reg    *Registry
	sender *Sender
	now    func() time.Time
}

// New opens the agent's state. client is the collector connection.
func New(cfg Config, client logplatv1.IngestServiceClient, apiKey string, log *slog.Logger) (*Agent, error) {
	cfg.Defaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	sp, err := spool.Open(filepath.Join(cfg.StateDir, "spool"), spool.Options{
		SegmentBytes: cfg.SpoolSegmentBytes,
		MaxBytes:     cfg.SpoolMaxBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("open spool: %w", err)
	}
	reg, err := LoadRegistry(filepath.Join(cfg.StateDir, "registry.json"))
	if err != nil {
		sp.Close()
		return nil, fmt.Errorf("load registry: %w", err)
	}
	return &Agent{
		cfg: cfg, log: log, spool: sp, reg: reg, now: time.Now,
		sender: &Sender{Client: client, APIKey: apiKey, Spool: sp, Window: cfg.Window, Backoff: cfg.Backoff, Log: log},
	}, nil
}

// Run tails and ships until ctx is cancelled. On cancellation it stops
// reading, spools what it already read, then keeps sending for up to
// DrainTimeout so a clean shutdown leaves as little as possible on disk.
func (a *Agent) Run(ctx context.Context) error {
	chunks := make(chan chunk, 64)
	var tailers sync.WaitGroup
	go func() {
		a.discover(ctx, chunks, &tailers)
		tailers.Wait()
		close(chunks)
	}()

	batcherDone := make(chan struct{})
	var batchErr error
	go func() {
		defer close(batcherDone)
		batchErr = a.batch(ctx, chunks)
	}()

	sendCtx, cancelSend := context.WithCancel(context.Background())
	defer cancelSend()
	go func() {
		<-batcherDone
		select {
		case <-time.After(a.cfg.DrainTimeout):
			a.log.Warn("drain timeout; unacked batches stay spooled for next start")
		case <-sendCtx.Done():
		}
		cancelSend()
	}()

	stopHousekeeping := a.housekeep()
	sendErr := a.sender.Run(sendCtx, batcherDone)
	stopHousekeeping()
	<-batcherDone
	if errors.Is(sendErr, context.Canceled) {
		sendErr = nil
	}
	return errors.Join(batchErr, sendErr, a.reg.Save(), a.spool.Close())
}

// housekeep periodically persists cursors and publishes gauges.
func (a *Agent) housekeep() (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if err := a.spool.SaveCursor(); err != nil {
					a.log.Error("save spool cursor", "error", err)
				}
				if err := a.reg.Save(); err != nil {
					a.log.Error("save registry", "error", err)
				}
				spoolBytes.Set(float64(a.spool.Bytes()))
			}
		}
	}()
	return func() { close(done); wg.Wait() }
}

// discover starts a tailer for every path matching the globs, rescanning so
// new files are picked up. A path gets at most one tailer at a time.
func (a *Agent) discover(ctx context.Context, out chan<- chunk, wg *sync.WaitGroup) {
	var mu sync.Mutex
	active := map[string]bool{}
	skipped := map[string]bool{} // container logs that failed the label check; labels never change
	scan := func() {
		for _, pattern := range a.cfg.Paths {
			matches, _ := filepath.Glob(pattern)
			for _, path := range matches {
				mu.Lock()
				if active[path] || skipped[path] {
					mu.Unlock()
					continue
				}
				if a.cfg.Format == FormatDockerJSON && !matchesLabel(path, a.cfg.DockerLabel) {
					skipped[path] = true
					mu.Unlock()
					continue
				}
				active[path] = true
				mu.Unlock()

				start, _ := a.reg.Get(path)
				host := a.cfg.Host
				if a.cfg.Format == FormatDockerJSON {
					host = containerName(path)
				}
				t := &tailer{
					path: path, agentID: a.cfg.AgentID, host: host, start: start, out: out, format: a.cfg.Format,
					poll: a.cfg.PollInterval, readSize: a.cfg.ReadSize, log: a.log, now: a.now,
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := t.run(ctx)
					if err != nil && !errors.Is(err, context.Canceled) {
						a.log.Error("tailer stopped", "path", path, "error", err)
					}
					mu.Lock()
					delete(active, path)
					mu.Unlock()
				}()
				a.log.Info("tailing file", "path", path, "offset", start.Offset, "epoch", start.Epoch)
			}
		}
	}
	scan()
	t := time.NewTicker(a.cfg.ScanInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			scan()
		}
	}
}

// batch groups chunks into spool records. It returns when chunks is closed
// and everything it received has been spooled (or the spool stayed full past
// shutdown, in which case the registry was never advanced for those lines and
// they are re-read on the next start).
func (a *Agent) batch(ctx context.Context, chunks <-chan chunk) error {
	var (
		entries []*logplatv1.LogEntry
		size    int
		states  = map[string]FileState{}
		timer   = time.NewTimer(a.cfg.BatchLinger)
	)
	timer.Stop()

	flush := func() error {
		if len(entries) == 0 && len(states) == 0 {
			return nil
		}
		if len(entries) > 0 {
			payload, err := proto.Marshal(&logplatv1.IngestRequest{Entries: entries})
			if err != nil {
				return fmt.Errorf("marshal batch: %w", err)
			}
			if err := a.append(ctx, payload); err != nil {
				return err
			}
		}
		for src, st := range states {
			a.reg.Set(src, st)
		}
		entries, size, states = nil, 0, map[string]FileState{}
		timer.Stop()
		return nil
	}

	for {
		select {
		case c, ok := <-chunks:
			if !ok {
				return flush()
			}
			if len(entries) == 0 {
				timer.Reset(a.cfg.BatchLinger)
			}
			entries = append(entries, c.entries...)
			size += c.bytes
			states[c.source] = c.state
			if len(entries) >= a.cfg.BatchMaxEntries || size >= a.cfg.BatchMaxBytes {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// append writes one record, waiting out a full spool. During shutdown it
// gives up instead; those lines were never recorded in the registry.
func (a *Agent) append(ctx context.Context, payload []byte) error {
	for {
		start := time.Now()
		_, err := a.spool.Append(payload)
		if err == nil {
			spoolAppendSeconds.Observe(time.Since(start).Seconds())
			batchesSpooled.Inc()
			return nil
		}
		if !errors.Is(err, spool.ErrFull) {
			return fmt.Errorf("spool append: %w", err)
		}
		spoolFullWaits.Inc()
		select {
		case <-ctx.Done():
			return fmt.Errorf("spool full at shutdown; unspooled lines will be re-read: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
