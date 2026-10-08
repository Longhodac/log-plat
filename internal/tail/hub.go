// Package tail streams log entries to connected clients as they arrive.
//
// It is best-effort by design: a client sees entries from the moment it
// connects, entries are not stored for later, and a client that cannot keep up
// loses entries (and is told how many) instead of slowing everyone else down.
// History is the search API's job.
package tail

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logplatv1 "github.com/Longhodac/log-plat/gen/logplat/v1"
	"github.com/Longhodac/log-plat/internal/doc"
)

// Event is one entry as clients see it.
type Event struct {
	ID         string    `json:"id"`
	Service    string    `json:"service"`
	Timestamp  time.Time `json:"timestamp"`
	ObservedAt time.Time `json:"observed_at"`
	Level      string    `json:"level"`
	Message    string    `json:"message"`
	Host       string    `json:"host,omitempty"`
	Source     string    `json:"source"`
}

// FromEntry converts a record read from Kafka.
func FromEntry(e *logplatv1.LogEntry) Event {
	return Event{
		ID: e.GetId(), Service: e.GetService(),
		Timestamp: e.GetTimestamp().AsTime().UTC(), ObservedAt: e.GetObservedAt().AsTime().UTC(),
		Level: doc.LevelName(e.GetLevel()), Message: e.GetMessage(), Host: e.GetHost(), Source: e.GetSource(),
	}
}

// Filter selects which events a client receives. An empty set matches all.
type Filter struct {
	Services map[string]bool
	Hosts    map[string]bool
	Levels   map[string]bool
	Text     string // lowercase; matches if the message contains it
}

// Match reports whether ev passes the filter.
func (f Filter) Match(ev Event) bool {
	if len(f.Services) > 0 && !f.Services[ev.Service] {
		return false
	}
	if len(f.Hosts) > 0 && !f.Hosts[ev.Host] {
		return false
	}
	if len(f.Levels) > 0 && !f.Levels[ev.Level] {
		return false
	}
	return f.Text == "" || strings.Contains(strings.ToLower(ev.Message), f.Text)
}

// ErrTooManyClients means the hub is at its connection limit.
var ErrTooManyClients = errors.New("tail: too many clients")

// Sub is one client's feed.
type Sub struct {
	C       <-chan Event
	c       chan Event
	filter  Filter
	dropped atomic.Uint64
}

// TakeDropped returns how many matching events were lost since the last call.
func (s *Sub) TakeDropped() uint64 { return s.dropped.Swap(0) }

// Hub fans events out to subscribers.
type Hub struct {
	mu   sync.RWMutex
	subs map[*Sub]struct{}
	max  int
}

// NewHub returns a Hub that accepts at most max subscribers.
func NewHub(max int) *Hub { return &Hub{subs: map[*Sub]struct{}{}, max: max} }

// Subscribe registers a client with a buffer of buf events.
func (h *Hub) Subscribe(f Filter, buf int) (*Sub, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.subs) >= h.max {
		return nil, ErrTooManyClients
	}
	c := make(chan Event, buf)
	s := &Sub{C: c, c: c, filter: f}
	h.subs[s] = struct{}{}
	clients.Set(float64(len(h.subs)))
	return s, nil
}

// Unsubscribe removes a client. It is safe to call twice.
func (h *Hub) Unsubscribe(s *Sub) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subs, s)
	clients.Set(float64(len(h.subs)))
}

// Publish offers ev to every matching client without ever blocking: a client
// whose buffer is full loses the event and has it counted.
func (h *Hub) Publish(ev Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs {
		if !s.filter.Match(ev) {
			continue
		}
		select {
		case s.c <- ev:
			delivered.Inc()
		default:
			s.dropped.Add(1)
			dropped.Inc()
		}
	}
}
