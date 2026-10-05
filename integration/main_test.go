//go:build integration

// Package integration runs the pipeline against real Kafka and OpenSearch
// containers. Run with: go test -tags=integration ./integration/...
package integration

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcopensearch "github.com/testcontainers/testcontainers-go/modules/opensearch"

	"github.com/Longhodac/log-plat/internal/backoff"
	"github.com/Longhodac/log-plat/internal/doc"
	"github.com/Longhodac/log-plat/internal/osutil"
)

const (
	kafkaImage      = "confluentinc/confluent-local:8.2.4"
	opensearchImage = "opensearchproject/opensearch:3.9.0"
)

var (
	brokers []string
	osAddr  string
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	code, err := withContainers(ctx, m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func withContainers(ctx context.Context, m *testing.M) (int, error) {
	kc, err := tckafka.Run(ctx, kafkaImage, tckafka.WithClusterID("logplat-test"))
	if kc != nil {
		defer func() { _ = testcontainers.TerminateContainer(kc) }()
	}
	if err != nil {
		return 0, fmt.Errorf("kafka: %w", err)
	}
	if brokers, err = kc.Brokers(ctx); err != nil {
		return 0, err
	}

	oc, err := tcopensearch.Run(ctx, opensearchImage,
		testcontainers.WithEnv(map[string]string{"OPENSEARCH_JAVA_OPTS": "-Xms512m -Xmx512m"}))
	if oc != nil {
		defer func() { _ = testcontainers.TerminateContainer(oc) }()
	}
	if err != nil {
		return 0, fmt.Errorf("opensearch: %w", err)
	}
	if osAddr, err = oc.Address(ctx); err != nil {
		return 0, err
	}
	return m.Run(), nil
}

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// uniq namespaces topics, groups, and indices so tests sharing containers
// cannot see each other's data.
func uniq(t *testing.T) string {
	return strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name())) + fmt.Sprintf("-%d", time.Now().UnixNano()%1e6)
}

func openSearch(t *testing.T, prefix string) *opensearchapi.Client {
	t.Helper()
	c, err := osutil.New([]string{osAddr})
	if err != nil {
		t.Fatal(err)
	}
	tmpl, err := doc.Template(prefix, 1, 0, "1s")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err = backoff.Default.Retry(ctx, func() error {
		return osutil.PutTemplate(ctx, c, doc.TemplateName(prefix), tmpl)
	}, func(int, error) {})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s (last error: %v)", timeout, what, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
