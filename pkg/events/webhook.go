package events

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/fystack/multichain-indexer/pkg/common/logger"
)

const (
	webhookTimeout    = 10 * time.Second
	webhookBatch      = 100
	webhookMinBackoff = time.Second
	webhookMaxBackoff = time.Minute
)

type outboxRow struct {
	id      int64
	topic   string
	key     *string
	payload []byte
}

// dispatch delivers outbox rows in insertion order. On failure it retries the
// same row with backoff, forever: dropping a deposit event is worse than stalling.
func (e *emitter) dispatch(ctx context.Context) {
	defer close(e.done)
	client := &http.Client{Timeout: webhookTimeout}
	backoff := webhookMinBackoff

	for {
		n, err := e.flush(ctx, client)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Error("Webhook delivery failed, retrying", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, webhookMaxBackoff)
			continue
		}
		backoff = webhookMinBackoff
		if n == webhookBatch {
			continue // probably more pending
		}
		select {
		case <-ctx.Done():
			return
		case <-e.wake:
		}
	}
}

// flush delivers up to webhookBatch rows and returns how many were delivered.
func (e *emitter) flush(ctx context.Context, client *http.Client) (int, error) {
	rows, err := e.db.QueryContext(ctx,
		`SELECT id, topic, key, payload FROM outbox ORDER BY id LIMIT ?`, webhookBatch)
	if err != nil {
		return 0, err
	}
	var batch []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.id, &r.topic, &r.key, &r.payload); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for i, r := range batch {
		if err := e.post(ctx, client, r); err != nil {
			return i, err
		}
		if _, err := e.db.ExecContext(ctx, `DELETE FROM outbox WHERE id = ?`, r.id); err != nil {
			return i, err
		}
	}
	return len(batch), nil
}

func (e *emitter) post(ctx context.Context, client *http.Client, r outboxRow) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.webhook.URL, bytes.NewReader(r.payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-Type", r.topic)
	if r.key != nil {
		req.Header.Set("Idempotency-Key", *r.key) // consumers dedupe on this
	}
	if e.webhook.Secret != "" {
		mac := hmac.New(sha256.New, []byte(e.webhook.Secret))
		mac.Write(r.payload)
		req.Header.Set("X-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("webhook returned %s for outbox id %d", resp.Status, r.id)
	}
	return nil
}
