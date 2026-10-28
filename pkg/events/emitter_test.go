package events

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/types"
	"github.com/fystack/multichain-indexer/pkg/infra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmitter_DeliversInOrderRetriesAndSigns(t *testing.T) {
	db, err := infra.OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	defer db.Close()

	var (
		mu       sync.Mutex
		calls    int
		received []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 { // first attempt fails: the same event must be retried
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		mac := hmac.New(sha256.New, []byte("s3cret"))
		mac.Write(body)
		assert.Equal(t, "sha256="+hex.EncodeToString(mac.Sum(nil)), r.Header.Get("X-Signature-256"))
		assert.Equal(t, TopicTransfer, r.Header.Get("X-Event-Type"))
		assert.NotEmpty(t, r.Header.Get("Idempotency-Key"))
		var tx types.Transaction
		assert.NoError(t, tx.UnmarshalBinary(body))
		received = append(received, tx.TxHash)
	}))
	defer srv.Close()

	e := NewEmitter(db, config.WebhookConfig{URL: srv.URL, Secret: "s3cret"})
	defer e.Close()

	tx1 := types.Transaction{TxHash: "a", NetworkId: "n", TransferIndex: "0"}
	tx2 := types.Transaction{TxHash: "b", NetworkId: "n", TransferIndex: "0"}
	require.NoError(t, e.EmitTransaction("eth", &tx1))
	require.NoError(t, e.EmitTransaction("eth", &tx1)) // duplicate while pending is ignored
	require.NoError(t, e.EmitTransaction("eth", &tx2))

	require.Eventually(t, func() bool {
		var n int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&n))
		return n == 0
	}, 5*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"a", "b"}, received)
}

func TestEmitter_KeepsEventsWithoutWebhook(t *testing.T) {
	db, err := infra.OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	defer db.Close()

	e := NewEmitter(db, config.WebhookConfig{})
	require.NoError(t, e.EmitUTXO("btc", &types.UTXOEvent{TxHash: "u"}))
	e.Close()
	e.Close() // idempotent: the manager and main both close it

	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE topic = ?`, TopicUTXO).Scan(&n))
	require.Equal(t, 1, n)
}
