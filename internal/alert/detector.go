// Package alert finds error spikes in the log stream and tells people about them.
package alert

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// Config sets what counts as a spike.
type Config struct {
	// Window is how far back "right now" looks.
	Window time.Duration
	// Bucket is the counting resolution inside the window and the baseline.
	Bucket time.Duration
	// Baseline is how much history before the window defines "normal".
	Baseline time.Duration
	// MinErrors is the fewest errors in one window that can ever be a spike. It
	// stops a service that normally logs nothing from alerting on one or two.
	MinErrors int
	// Ratio is how many times the usual window total the current one must reach.
	Ratio float64
	// Cooldown is the shortest time between two spike alerts for one service.
	Cooldown time.Duration
	// Samples is how many recent error messages an alert carries. 0 omits them.
	Samples int
}

// DefaultConfig is one minute of errors against the previous ten.
func DefaultConfig() Config {
	return Config{Window: time.Minute, Bucket: time.Second, Baseline: 10 * time.Minute, MinErrors: 20, Ratio: 5, Cooldown: 2 * time.Minute, Samples: 3}
}

// Kind is what happened to a service.
type Kind string

const (
	// Firing means a spike started. It is sent once, not on every evaluation.
	Firing Kind = "firing"
	// Resolved means errors fell back below the threshold.
	Resolved Kind = "resolved"
)

// Alert is one notification.
type Alert struct {
	Service string
	Kind    Kind
	At      time.Time
	Window  time.Duration
	// Errors is the count in the last Window.
	Errors int
	// Usual is the typical count per Window over the baseline period.
	Usual   float64
	Samples []string
}

type state struct {
	buckets   map[int64]int
	recent    []string
	firing    bool
	lastFired time.Time
}

// Detector counts error and fatal entries per service and reports spikes.
//
// It works from the entry's own observed time, so replaying a file does not
// look like a burst of old errors, and a backlog from before the baseline
// period is ignored. Time passes only when Evaluate is called with the current
// clock, so tests drive it with a fake one.
type Detector struct {
	cfg Config
	now func() time.Time

	mu   sync.Mutex
	svcs map[string]*state
}

// NewDetector returns a Detector. now is the clock, normally time.Now.
func NewDetector(cfg Config, now func() time.Time) *Detector {
	return &Detector{cfg: cfg, now: now, svcs: map[string]*state{}}
}

func (d *Detector) bucketOf(t time.Time) int64 { return t.UnixNano() / int64(d.cfg.Bucket) }

func (d *Detector) windowBuckets() int64   { return int64(d.cfg.Window / d.cfg.Bucket) }
func (d *Detector) baselineBuckets() int64 { return int64(d.cfg.Baseline / d.cfg.Bucket) }

// Observe records an entry. Only the "error" and "fatal" levels count.
func (d *Detector) Observe(service, level string, at time.Time, message string) {
	if level != "error" && level != "fatal" {
		return
	}
	now := d.now()
	if at.After(now) {
		at = now // a skewed clock must not park errors in the future
	}
	cur := d.bucketOf(now)
	b := d.bucketOf(at)
	if b <= cur-d.windowBuckets()-d.baselineBuckets() {
		return // older than anything the detector looks at
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.svcs[service]
	if s == nil {
		s = &state{buckets: map[int64]int{}}
		d.svcs[service] = s
	}
	s.buckets[b]++
	if d.cfg.Samples > 0 {
		if len(message) > 200 {
			message = message[:200] + "..."
		}
		s.recent = append(s.recent, message)
		if len(s.recent) > d.cfg.Samples {
			s.recent = s.recent[len(s.recent)-d.cfg.Samples:]
		}
	}
}

// Evaluate returns the alerts due now, in service-name order.
func (d *Detector) Evaluate() []Alert {
	now := d.now()
	cur := d.bucketOf(now)
	win, base := d.windowBuckets(), d.baselineBuckets()

	d.mu.Lock()
	defer d.mu.Unlock()
	var out []Alert
	for name, s := range d.svcs {
		var inWindow, inBaseline int
		for b, n := range s.buckets {
			switch {
			case b <= cur-win-base:
				delete(s.buckets, b)
			case b <= cur-win:
				inBaseline += n
			case b <= cur:
				inWindow += n
			}
		}
		usual := float64(inBaseline) * float64(win) / float64(base)

		switch {
		case !s.firing:
			spike := inWindow >= d.cfg.MinErrors && float64(inWindow) >= d.cfg.Ratio*max(usual, 1)
			if spike && (s.lastFired.IsZero() || now.Sub(s.lastFired) >= d.cfg.Cooldown) {
				s.firing, s.lastFired = true, now
				out = append(out, Alert{Service: name, Kind: Firing, At: now, Window: d.cfg.Window, Errors: inWindow, Usual: usual, Samples: append([]string(nil), s.recent...)})
			}
		case float64(inWindow)*2 < float64(d.cfg.MinErrors):
			s.firing = false
			out = append(out, Alert{Service: name, Kind: Resolved, At: now, Window: d.cfg.Window, Errors: inWindow, Usual: usual})
		}
		if len(s.buckets) == 0 && !s.firing {
			delete(d.svcs, name)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Service < out[j].Service })
	return out
}

// Services is how many services the detector currently tracks.
func (d *Detector) Services() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.svcs)
}

// escapeSlack neutralizes the characters Slack treats as markup, so a log line
// containing <!channel> or a link cannot ping anyone or hide a URL.
func escapeSlack(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
