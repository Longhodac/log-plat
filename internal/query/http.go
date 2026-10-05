package query

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Longhodac/log-plat/internal/apikey"
	"github.com/Longhodac/log-plat/internal/doc"
	"github.com/Longhodac/log-plat/internal/obs"
)

var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "logplat_query_requests_total",
		Help: "HTTP requests by route and status code.",
	}, []string{"route", "code"})
	requestSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "logplat_query_request_seconds",
		Help:    "HTTP request latency by route.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"route"})
)

// APIError is the body of every non-2xx response.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

func writeError(w http.ResponseWriter, status int, e APIError) {
	obs.WriteJSON(w, status, map[string]APIError{"error": e})
}

// SearchResponse is the body of GET /v1/logs.
type SearchResponse struct {
	Logs       []doc.Doc `json:"logs"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

// Server is the query API.
type Server struct {
	Search  Searcher
	Keys    *apikey.Store
	Ready   obs.ReadyFunc
	Log     *slog.Logger
	Timeout time.Duration
}

// Handler routes the API, health, readiness, and metrics.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/logs", s.instrument("/v1/logs", s.auth(http.HandlerFunc(s.searchLogs))))
	mux.Handle("/", obs.AdminHandler(s.Ready))
	return mux
}

func (s *Server) searchLogs(w http.ResponseWriter, r *http.Request) {
	q, err := Parse(r.URL.Query())
	if err != nil {
		var fe *FieldError
		if errors.As(err, &fe) {
			writeError(w, http.StatusBadRequest, APIError{Code: "invalid_argument", Message: fe.Error(), Field: fe.Field})
			return
		}
		writeError(w, http.StatusBadRequest, APIError{Code: "invalid_argument", Message: err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.Timeout)
	defer cancel()
	page, err := s.Search.Search(ctx, q)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			writeError(w, http.StatusGatewayTimeout, APIError{Code: "deadline_exceeded", Message: "search timed out"})
			return
		}
		s.Log.Error("search failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, APIError{Code: "unavailable", Message: "search backend unavailable"})
		return
	}
	resp := SearchResponse{Logs: page.Logs}
	if page.Next != nil {
		resp.NextCursor = EncodeCursor(*page.Next, q)
	}
	obs.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if key == "" {
			writeError(w, http.StatusUnauthorized, APIError{Code: "unauthenticated", Message: "missing X-API-Key header"})
			return
		}
		if _, ok := s.Keys.Service(key); !ok {
			writeError(w, http.StatusUnauthorized, APIError{Code: "unauthenticated", Message: "unknown API key"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		requestsTotal.WithLabelValues(route, strconv.Itoa(rec.code)).Inc()
		requestSeconds.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}
