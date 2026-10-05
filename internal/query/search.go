package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/Longhodac/log-plat/internal/doc"
)

// Searcher runs a validated query.
type Searcher interface {
	Search(ctx context.Context, q Query) (Page, error)
}

// OpenSearch searches every daily index under IndexPrefix.
type OpenSearch struct {
	Client      *opensearchapi.Client
	IndexPrefix string
}

// Body builds the search request. Filters run in filter context (cached, no
// scoring) because results are ordered by time, not relevance.
func Body(q Query) ([]byte, error) {
	var filters []any
	if q.Service != "" {
		filters = append(filters, map[string]any{"term": map[string]any{"service": q.Service}})
	}
	if q.Level != "" {
		filters = append(filters, map[string]any{"term": map[string]any{"level": q.Level}})
	}
	if !q.From.IsZero() || !q.To.IsZero() {
		r := map[string]any{"format": "strict_date_optional_time"}
		if !q.From.IsZero() {
			r["gte"] = q.From.Format(time.RFC3339Nano)
		}
		if !q.To.IsZero() {
			r["lt"] = q.To.Format(time.RFC3339Nano)
		}
		filters = append(filters, map[string]any{"range": map[string]any{"timestamp": r}})
	}
	if q.Text != "" {
		filters = append(filters, map[string]any{"match": map[string]any{
			"message": map[string]any{"query": q.Text, "operator": "and"},
		}})
	}
	body := map[string]any{
		"size":             q.Limit + 1,
		"track_total_hits": false,
		"query":            map[string]any{"bool": map[string]any{"filter": filters}},
		"sort": []any{
			map[string]any{"timestamp": map[string]any{"order": "desc"}},
			map[string]any{"id": map[string]any{"order": "desc"}},
		},
	}
	if q.After != nil {
		body["search_after"] = []any{q.After.TimestampMillis, q.After.ID}
	}
	return json.Marshal(body)
}

// Search implements Searcher. It fetches one extra hit to learn whether a
// next page exists without a second round trip.
func (s OpenSearch) Search(ctx context.Context, q Query) (Page, error) {
	body, err := Body(q)
	if err != nil {
		return Page{}, err
	}
	resp, err := s.Client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{doc.Pattern(s.IndexPrefix)},
		Body:    bytes.NewReader(body),
	})
	if err != nil {
		return Page{}, err
	}
	hits := resp.Hits.Hits
	page := Page{Logs: make([]doc.Doc, 0, min(len(hits), q.Limit))}
	for i, h := range hits {
		if i == q.Limit {
			last := page.Logs[len(page.Logs)-1]
			page.Next = &SortKey{TimestampMillis: last.Timestamp.UnixMilli(), ID: last.ID}
			break
		}
		var d doc.Doc
		if err := json.Unmarshal(h.Source, &d); err != nil {
			return Page{}, fmt.Errorf("decode hit %s: %w", h.ID, err)
		}
		page.Logs = append(page.Logs, d)
	}
	return page, nil
}
