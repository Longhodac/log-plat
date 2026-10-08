package tail

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/doc"
	"github.com/Longhodac/log-plat/internal/logid"
	"github.com/Longhodac/log-plat/internal/obs"
)

// ParseFilter reads service, host, level (each comma-separated) and q from a query string.
func ParseFilter(v url.Values) (Filter, error) {
	f := Filter{Text: strings.ToLower(strings.TrimSpace(v.Get("q")))}
	set := func(key string) map[string]bool {
		var m map[string]bool
		for _, s := range strings.Split(v.Get(key), ",") {
			if s = strings.TrimSpace(s); s != "" {
				if m == nil {
					m = map[string]bool{}
				}
				m[s] = true
			}
		}
		return m
	}
	f.Services, f.Hosts = set("service"), set("host")
	for lv := range set("level") {
		if _, ok := doc.ParseLevelName(lv); !ok {
			return f, fmt.Errorf("level %q: must be one of trace, debug, info, warn, error, fatal, unknown", lv)
		}
		if f.Levels == nil {
			f.Levels = map[string]bool{}
		}
		f.Levels[strings.ToLower(lv)] = true
	}
	if len(f.Text) > 512 {
		return f, fmt.Errorf("q must be at most 512 characters")
	}
	return f, nil
}

// maxUnflushed bounds how many events are written between flushes.
const maxUnflushed = 64

// Server serves GET /v1/tail as a Server-Sent Events stream.
type Server struct {
	Hub       *Hub
	Keys      *apikey.Store
	Ready     obs.ReadyFunc
	Log       *slog.Logger
	Buffer    int           // events buffered per client
	Heartbeat time.Duration // keepalive comment interval
}

// Handler routes the stream, health, readiness, and metrics.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tail", s.tail)
	mux.Handle("/", obs.AdminHandler(s.Ready))
	return mux
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	obs.WriteJSON(w, status, map[string]apiError{"error": {Code: code, Message: msg}})
}

func (s *Server) tail(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("X-API-Key")
	if key == "" {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "missing X-API-Key header")
		return
	}
	if _, ok := s.Keys.Service(key); !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "unknown API key")
		return
	}
	f, err := ParseFilter(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	rc := http.NewResponseController(w)
	sub, err := s.Hub.Subscribe(f, s.Buffer)
	if err != nil {
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "too_many_clients", "the tail service is at its client limit")
		return
	}
	defer s.Hub.Unsubscribe(sub)

	// A stream outlives the server's normal write deadline.
	_ = rc.SetWriteDeadline(time.Time{})
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": connected\n\n")
	if err := rc.Flush(); err != nil {
		return
	}

	beat := time.NewTicker(s.Heartbeat)
	defer beat.Stop()
	unflushed := 0
	for {
		select {
		case <-r.Context().Done():
			return
		case <-beat.C:
			if n := sub.TakeDropped(); n > 0 {
				fmt.Fprintf(w, "event: dropped\ndata: {\"dropped\":%d}\n\n", n) //nolint:gosec // an integer
			} else {
				fmt.Fprint(w, ": keepalive\n\n")
			}
		case ev := <-sub.C:
			if n := sub.TakeDropped(); n > 0 {
				fmt.Fprintf(w, "event: dropped\ndata: {\"dropped\":%d}\n\n", n) //nolint:gosec // an integer
			}
			raw, _ := json.Marshal(ev)
			// The tail reads Kafka without the collector's validation, so an ID
			// is only written when it is a well-formed log ID. A newline in a
			// crafted one would otherwise inject fields into the stream. The data
			// line is JSON, which escapes newlines.
			if logid.Valid(ev.ID) {
				fmt.Fprintf(w, "id: %s\n", ev.ID) //nolint:gosec // validated above; the stream is text/event-stream, not HTML
			}
			fmt.Fprintf(w, "event: log\ndata: %s\n\n", raw) //nolint:gosec // JSON-encoded, served as text/event-stream
			// Flush once the burst waiting in the buffer has been written, but
			// never let a constant stream postpone a flush for long.
			if unflushed++; len(sub.C) > 0 && unflushed < maxUnflushed {
				continue
			}
		}
		unflushed = 0
		if err := rc.Flush(); err != nil {
			return
		}
	}
}
