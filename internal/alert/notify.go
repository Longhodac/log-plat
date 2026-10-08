package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Longhodac/log-plat/internal/backoff"
)

// Notifier delivers an alert somewhere.
type Notifier interface {
	Notify(ctx context.Context, a Alert) error
}

// FormatSlack renders an alert as Slack mrkdwn.
func FormatSlack(a Alert) string {
	svc := escapeSlack(a.Service)
	window := a.Window.Round(time.Second)
	if a.Kind == Resolved {
		return fmt.Sprintf(":white_check_mark: *Errors back to normal in `%s`* (%d in the last %s)", svc, a.Errors, window)
	}
	var b strings.Builder
	fmt.Fprintf(&b, ":rotating_light: *Error spike in `%s`*\n%d errors in the last %s (usually about %.0f).", svc, a.Errors, window, a.Usual)
	if len(a.Samples) > 0 {
		b.WriteString("\nMost recent:")
		for _, s := range a.Samples {
			fmt.Fprintf(&b, "\n• `%s`", strings.ReplaceAll(escapeSlack(s), "`", "'"))
		}
	}
	return b.String()
}

// LogNotifier writes alerts to the log. It is what runs when no webhook is set.
type LogNotifier struct{ Log *slog.Logger }

// Notify implements Notifier.
func (n LogNotifier) Notify(_ context.Context, a Alert) error {
	n.Log.Warn("alert", "kind", a.Kind, "service", a.Service, "errors", a.Errors, "usual", a.Usual, "message", FormatSlack(a))
	return nil
}

// Slack posts alerts to an incoming webhook.
type Slack struct {
	URL         string
	Client      *http.Client
	MaxAttempts int
	Backoff     backoff.Policy
}

// ErrPermanent marks a failure that retrying cannot fix, such as a revoked webhook.
var ErrPermanent = errors.New("slack rejected the message")

// Notify posts the alert. It retries network errors, 5xx, and 429 (waiting out
// Retry-After), and gives up at once on any other 4xx. Errors never include
// the webhook URL, because the URL is a secret.
func (s *Slack) Notify(ctx context.Context, a Alert) error {
	body, _ := json.Marshal(map[string]string{"text": FormatSlack(a)})
	attempts := max(s.MaxAttempts, 1)
	var last error
	for i := range attempts {
		wait, err := s.post(ctx, body)
		if err == nil {
			return nil
		}
		last = err
		if errors.Is(err, ErrPermanent) || i == attempts-1 {
			break
		}
		if wait <= 0 {
			wait = s.Backoff.Delay(i)
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return last
}

func (s *Slack) post(ctx context.Context, body []byte) (retryAfter time.Duration, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("%w: bad webhook URL", ErrPermanent)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.Client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err // url.Error carries the full URL
		}
		return 0, fmt.Errorf("slack request failed: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	switch {
	case resp.StatusCode/100 == 2:
		return 0, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		secs, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
		return time.Duration(min(max(secs, 1), 30)) * time.Second, fmt.Errorf("slack rate limited (HTTP 429)")
	case resp.StatusCode >= 500:
		return 0, fmt.Errorf("slack server error (HTTP %d)", resp.StatusCode)
	}
	return 0, fmt.Errorf("%w (HTTP %d)", ErrPermanent, resp.StatusCode)
}
