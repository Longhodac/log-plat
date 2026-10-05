// Package osutil wires the official OpenSearch client and the few raw calls
// the services share.
package osutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// New returns a client for addrs. Local development runs OpenSearch with the
// security plugin disabled, so no credentials are configured here.
func New(addrs []string) (*opensearchapi.Client, error) {
	return opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: addrs},
	})
}

// Do sends a raw request and decodes a 2xx JSON response into out (if non-nil).
func Do(ctx context.Context, c *opensearchapi.Client, method, path string, body []byte, out any) error {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Client.Stream(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return &StatusError{Code: resp.StatusCode, Body: truncate(string(raw), 512)}
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// StatusError is a non-2xx response.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("opensearch: HTTP %d: %s", e.Code, e.Body)
}

// PutTemplate creates or replaces an index template. PUT is idempotent, so
// every indexer replica can run it at startup.
func PutTemplate(ctx context.Context, c *opensearchapi.Client, name string, body []byte) error {
	return Do(ctx, c, http.MethodPut, "/_index_template/"+name, body, nil)
}

// Ready returns nil when the cluster answers and is not red.
func Ready(ctx context.Context, c *opensearchapi.Client) error {
	var h struct {
		Status string `json:"status"`
	}
	if err := Do(ctx, c, http.MethodGet, "/_cluster/health", nil, &h); err != nil {
		return err
	}
	if strings.EqualFold(h.Status, "red") {
		return fmt.Errorf("opensearch cluster status is red")
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
