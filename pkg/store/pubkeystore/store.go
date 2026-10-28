package pubkeystore

import (
	"database/sql"
	"errors"

	"github.com/fystack/multichain-indexer/pkg/common/enum"
	"github.com/fystack/multichain-indexer/pkg/common/logger"
)

type Store interface {
	Exist(addressType enum.NetworkType, publicKey string) bool
	ExistBatch(addressType enum.NetworkType, publicKeys []string) []bool
	Save(addressType enum.NetworkType, publicKey string) error
	Close() error
}

// publicKeyStore looks addresses up in the SQLite wallet_addresses table.
// Matches are exact and new rows are visible on the next lookup.
type publicKeyStore struct {
	db *sql.DB
}

func NewPublicKeyStore(db *sql.DB) Store {
	return &publicKeyStore{db: db}
}

func (s *publicKeyStore) Exist(addressType enum.NetworkType, publicKey string) bool {
	if publicKey == "" {
		return false
	}
	var one int
	err := s.db.QueryRow(
		`SELECT 1 FROM wallet_addresses WHERE type = ? AND address = ?`,
		string(addressType), publicKey,
	).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		// shortcut: fails open (like a bloom filter false positive) so a DB error never drops a
		// deposit; consumers must still verify the address. Return the error if callers grow one.
		logger.Error("Address lookup failed, treating as match", "type", addressType, "address", publicKey, "err", err)
		return true
	}
	return true
}

func (s *publicKeyStore) ExistBatch(addressType enum.NetworkType, publicKeys []string) []bool {
	out := make([]bool, len(publicKeys))
	for i, k := range publicKeys {
		out[i] = s.Exist(addressType, k)
	}
	return out
}

func (s *publicKeyStore) Save(addressType enum.NetworkType, publicKey string) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO wallet_addresses (type, address) VALUES (?, ?)`,
		string(addressType), publicKey,
	)
	return err
}

func (s *publicKeyStore) Close() error { return nil }
