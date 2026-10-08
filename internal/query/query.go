// Package query serves log search over HTTP.
package query

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Longhodac/log-plat/internal/doc"
)

const (
	DefaultLimit = 100
	MaxLimit     = 1000
	maxTextLen   = 512
	maxService   = 128
)

// Query is a validated search request.
type Query struct {
	Service string
	Host    string
	Level   string
	Text    string
	From    time.Time // inclusive; zero means unbounded
	To      time.Time // exclusive; zero means unbounded
	Limit   int
	After   *SortKey
}

// SortKey is the position of a document in (timestamp desc, id desc) order.
// Ties on timestamp are broken by ID, so the order is total and a page
// boundary can never skip or repeat a document.
type SortKey struct {
	TimestampMillis int64  `json:"t"`
	ID              string `json:"i"`
}

// Page is one page of results.
type Page struct {
	Logs []doc.Doc
	Next *SortKey
	// Cached is set when the page came from the cache. It is not stored.
	Cached bool `json:"-"`
}

// FieldError is a client mistake in one parameter.
type FieldError struct {
	Field  string
	Reason string
}

func (e *FieldError) Error() string { return e.Field + ": " + e.Reason }

// Parse validates query parameters. Every check happens here so the search
// code can trust a Query.
func Parse(v url.Values) (Query, error) {
	q := Query{
		Service: strings.TrimSpace(v.Get("service")),
		Host:    strings.TrimSpace(v.Get("host")),
		Text:    strings.TrimSpace(v.Get("q")),
		Limit:   DefaultLimit,
	}
	if len(q.Service) > maxService {
		return q, &FieldError{"service", fmt.Sprintf("must be at most %d characters", maxService)}
	}
	if len(q.Host) > maxService {
		return q, &FieldError{"host", fmt.Sprintf("must be at most %d characters", maxService)}
	}
	if len(q.Text) > maxTextLen {
		return q, &FieldError{"q", fmt.Sprintf("must be at most %d characters", maxTextLen)}
	}
	if lv := strings.TrimSpace(v.Get("level")); lv != "" {
		if _, ok := doc.ParseLevelName(lv); !ok {
			return q, &FieldError{"level", "must be one of trace, debug, info, warn, error, fatal, unknown"}
		}
		q.Level = strings.ToLower(lv)
	}
	for _, f := range []struct {
		name string
		dst  *time.Time
	}{{"from", &q.From}, {"to", &q.To}} {
		if s := v.Get(f.name); s != "" {
			t, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				return q, &FieldError{f.name, "must be an RFC 3339 timestamp, e.g. 2008-11-09T20:36:15Z"}
			}
			*f.dst = t.UTC()
		}
	}
	if !q.From.IsZero() && !q.To.IsZero() && !q.From.Before(q.To) {
		return q, &FieldError{"from", "must be before to"}
	}
	if s := v.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > MaxLimit {
			return q, &FieldError{"limit", fmt.Sprintf("must be an integer from 1 to %d", MaxLimit)}
		}
		q.Limit = n
	}
	if s := v.Get("cursor"); s != "" {
		key, err := DecodeCursor(s, q)
		if err != nil {
			return q, &FieldError{"cursor", err.Error()}
		}
		q.After = &key
	}
	return q, nil
}

type cursor struct {
	SortKey
	Filter string `json:"f"`
}

// fingerprint identifies the filters a cursor was issued for. Reusing a
// cursor with different filters would silently return the wrong page.
func (q Query) fingerprint() string {
	h := sha256.Sum256([]byte(strings.Join([]string{
		q.Service, q.Host, q.Level, q.Text, q.From.Format(time.RFC3339Nano), q.To.Format(time.RFC3339Nano),
	}, "\x00")))
	return hex.EncodeToString(h[:8])
}

// EncodeCursor returns an opaque cursor for the page after key.
func EncodeCursor(key SortKey, q Query) string {
	raw, _ := json.Marshal(cursor{SortKey: key, Filter: q.fingerprint()})
	return base64.RawURLEncoding.EncodeToString(raw)
}

// DecodeCursor parses a cursor and checks it was issued for q's filters.
func DecodeCursor(s string, q Query) (SortKey, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return SortKey{}, errors.New("is malformed")
	}
	var c cursor
	if err := json.Unmarshal(raw, &c); err != nil || c.ID == "" {
		return SortKey{}, errors.New("is malformed")
	}
	if c.Filter != q.fingerprint() {
		return SortKey{}, errors.New("was issued for different filters")
	}
	return c.SortKey, nil
}
