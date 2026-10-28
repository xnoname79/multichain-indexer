package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/logger"
	"github.com/fystack/multichain-indexer/pkg/common/types"
)

const (
	TopicTransfer = "transfer"
	TopicUTXO     = "utxo"
	TopicEvent    = "event"
)

type IndexerEvent struct {
	Type      string `json:"type"`
	Chain     string `json:"chain"`
	Data      any    `json:"data"`
	Timestamp int64  `json:"timestamp"`
}

type Emitter interface {
	EmitBlock(chain string, block *types.Block) error
	EmitTransaction(chain string, tx *types.Transaction) error
	EmitUTXO(chain string, utxo *types.UTXOEvent) error
	EmitError(chain string, err error) error
	Emit(event IndexerEvent) error
	Close()
}

// emitter persists events to the SQLite outbox, then wakes the webhook
// dispatcher over a channel. An event is only removed once delivered, so a
// crash or a down webhook never loses it.
type emitter struct {
	db      *sql.DB
	webhook config.WebhookConfig
	wake    chan struct{}
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
}

func NewEmitter(db *sql.DB, webhook config.WebhookConfig) Emitter {
	ctx, cancel := context.WithCancel(context.Background())
	e := &emitter{
		db:      db,
		webhook: webhook,
		wake:    make(chan struct{}, 1),
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	if webhook.URL == "" {
		logger.Warn("webhook.url not set: matched events stay in the outbox table")
		close(e.done)
		return e
	}
	go e.dispatch(ctx)
	return e
}

func (e *emitter) enqueue(topic string, key any, payload []byte) error {
	// OR IGNORE: a re-processed block re-emits the same key while it is still pending.
	if _, err := e.db.Exec(
		`INSERT OR IGNORE INTO outbox (topic, key, payload) VALUES (?, ?, ?)`,
		topic, key, payload,
	); err != nil {
		return err
	}
	select {
	case e.wake <- struct{}{}:
	default: // a wake-up is already pending
	}
	return nil
}

func (e *emitter) EmitBlock(chain string, block *types.Block) error {
	// TODO: implement
	return nil
}

func (e *emitter) EmitTransaction(chain string, tx *types.Transaction) error {
	txBytes, err := tx.MarshalBinary()
	if err != nil {
		return err
	}
	return e.enqueue(TopicTransfer, tx.Hash(), txBytes)
}

func (e *emitter) EmitUTXO(chain string, utxo *types.UTXOEvent) error {
	utxoBytes, err := utxo.MarshalBinary()
	if err != nil {
		return err
	}
	return e.enqueue(TopicUTXO, utxo.Hash(), utxoBytes)
}

func (e *emitter) EmitError(chain string, err error) error {
	// TODO: implement
	return nil
}

func (e *emitter) Emit(event IndexerEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return e.enqueue(TopicEvent, nil, data)
}

// Close stops the dispatcher; undelivered events stay in the outbox for the next start.
func (e *emitter) Close() {
	e.once.Do(func() {
		e.cancel()
		<-e.done
	})
}
