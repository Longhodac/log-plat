// Package zeroloss verifies that every line written to a file reached
// OpenSearch.
//
// It frames the file the same way the agent does and derives each line's ID
// independently, so it checks the pipeline against the source file rather
// than against anything the pipeline itself recorded.
package zeroloss

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/Longhodac/log-plat/internal/doc"
	"github.com/Longhodac/log-plat/internal/lineio"
	"github.com/Longhodac/log-plat/internal/logid"
	"github.com/Longhodac/log-plat/internal/osutil"
)

// ExpectedIDs returns the ID of every line in file, as an agent with agentID
// tailing it at path source in the given epoch would assign them.
func ExpectedIDs(file, agentID, source string, epoch uint32) ([]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ids []string
	fr := lineio.NewFramer(0, lineio.DefaultMaxLen)
	emit := func(l lineio.Line) { ids = append(ids, logid.New(agentID, source, epoch, l.Offset)) }
	buf := make([]byte, 1<<20)
	for {
		n, err := f.Read(buf)
		fr.Feed(buf[:n], emit)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	// A trailing partial line is not counted: the agent will not ship it
	// until its newline arrives.
	return ids, nil
}

// Checker queries OpenSearch.
type Checker struct {
	Client      *opensearchapi.Client
	IndexPrefix string
}

func (c Checker) pattern() string { return "/" + doc.Pattern(c.IndexPrefix) }

func (c Checker) refresh(ctx context.Context) error {
	return osutil.Do(ctx, c.Client, http.MethodPost, c.pattern()+"/_refresh?allow_no_indices=true", nil, nil)
}

// Count returns how many documents came from source.
func (c Checker) Count(ctx context.Context, source string) (int, error) {
	if err := c.refresh(ctx); err != nil {
		return 0, err
	}
	body, _ := json.Marshal(map[string]any{"query": map[string]any{"term": map[string]any{"source": source}}})
	var out struct {
		Count int `json:"count"`
	}
	err := osutil.Do(ctx, c.Client, http.MethodPost, c.pattern()+"/_count?allow_no_indices=true", body, &out)
	return out.Count, err
}

// Missing returns the IDs not present in any index.
func (c Checker) Missing(ctx context.Context, ids []string) ([]string, error) {
	if err := c.refresh(ctx); err != nil {
		return nil, err
	}
	const chunk = 5000
	var missing []string
	for start := 0; start < len(ids); start += chunk {
		part := ids[start:min(start+chunk, len(ids))]
		body, _ := json.Marshal(map[string]any{
			"size":    len(part),
			"_source": false,
			"query":   map[string]any{"ids": map[string]any{"values": part}},
		})
		var out struct {
			Hits struct {
				Hits []struct {
					ID string `json:"_id"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err := osutil.Do(ctx, c.Client, http.MethodPost, c.pattern()+"/_search?allow_no_indices=true", body, &out); err != nil {
			return nil, err
		}
		found := make(map[string]bool, len(out.Hits.Hits))
		for _, h := range out.Hits.Hits {
			found[h.ID] = true
		}
		for _, id := range part {
			if !found[id] {
				missing = append(missing, id)
			}
		}
	}
	return missing, nil
}

// Span returns the earliest observed_at and latest indexed_at for source.
func (c Checker) Span(ctx context.Context, source string) (first, last time.Time, err error) {
	body, _ := json.Marshal(map[string]any{
		"size":  0,
		"query": map[string]any{"term": map[string]any{"source": source}},
		"aggs": map[string]any{
			"first_observed": map[string]any{"min": map[string]any{"field": "observed_at"}},
			"last_indexed":   map[string]any{"max": map[string]any{"field": "indexed_at"}},
		},
	})
	var out struct {
		Aggregations struct {
			First struct {
				Value *float64 `json:"value"`
			} `json:"first_observed"`
			Last struct {
				Value *float64 `json:"value"`
			} `json:"last_indexed"`
		} `json:"aggregations"`
	}
	if err := osutil.Do(ctx, c.Client, http.MethodPost, c.pattern()+"/_search?allow_no_indices=true", body, &out); err != nil {
		return first, last, err
	}
	if out.Aggregations.First.Value == nil || out.Aggregations.Last.Value == nil {
		return first, last, errors.New("no documents for source")
	}
	return time.UnixMilli(int64(*out.Aggregations.First.Value)).UTC(), time.UnixMilli(int64(*out.Aggregations.Last.Value)).UTC(), nil
}

// Report is the checker's verdict.
type Report struct {
	File          string    `json:"file"`
	Source        string    `json:"source"`
	Expected      int       `json:"expected"`
	Indexed       int       `json:"indexed_docs_for_source"`
	Missing       int       `json:"missing"`
	Unexpected    int       `json:"unexpected"`
	MissingSample []string  `json:"missing_sample,omitempty"`
	FirstObserved time.Time `json:"first_observed_at"`
	LastIndexed   time.Time `json:"last_indexed_at"`
	Seconds       float64   `json:"ingest_seconds"`
	LinesPerSec   float64   `json:"ingest_lines_per_sec"`
	ZeroLoss      bool      `json:"zero_loss"`
}

// Options control waiting for the pipeline to drain.
type Options struct {
	Poll    time.Duration
	Stall   time.Duration // give up when the count stops growing for this long
	Timeout time.Duration
	Log     func(format string, args ...any)
}

// Check waits for the source's document count to reach len(ids) (or stall),
// then verifies each ID is present.
func (c Checker) Check(ctx context.Context, file, source string, ids []string, o Options) (Report, error) {
	r := Report{File: file, Source: source, Expected: len(ids)}
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()

	lastCount, lastChange := -1, time.Now()
wait:
	for {
		n, err := c.Count(ctx, source)
		if err != nil {
			return r, fmt.Errorf("count: %w", err)
		}
		r.Indexed = n
		if n != lastCount {
			o.Log("indexed %d/%d", n, len(ids))
			lastCount, lastChange = n, time.Now()
		}
		if n >= len(ids) || time.Since(lastChange) > o.Stall {
			break
		}
		select {
		case <-ctx.Done():
			o.Log("timed out waiting; checking what arrived")
			break wait
		case <-time.After(o.Poll):
		}
	}
	missing, err := c.Missing(context.WithoutCancel(ctx), ids)
	if err != nil {
		return r, fmt.Errorf("missing: %w", err)
	}
	r.Missing = len(missing)
	r.MissingSample = missing[:min(10, len(missing))]
	// Doc IDs are unique, so documents beyond the found set are lines that
	// were not in the file: a framing disagreement or a stray writer.
	r.Unexpected = r.Indexed - (len(ids) - len(missing))
	r.ZeroLoss = r.Missing == 0 && r.Unexpected == 0
	if r.Indexed > 0 {
		first, last, err := c.Span(context.WithoutCancel(ctx), source)
		if err != nil {
			return r, fmt.Errorf("span: %w", err)
		}
		r.FirstObserved, r.LastIndexed = first, last
		r.Seconds = last.Sub(first).Seconds()
		if r.Seconds > 0 {
			r.LinesPerSec = float64(r.Indexed-r.Unexpected) / r.Seconds
		}
	}
	return r, nil
}
