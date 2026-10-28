package kvstore

import (
	"database/sql"
	"errors"

	"github.com/fystack/multichain-indexer/pkg/infra"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyEmpty    = errors.New("key is empty")
)

func checkKeyAndValue(k string, v any) error {
	if k == "" {
		return ErrKeyEmpty
	}
	if v == nil {
		return errors.New("the passed value is nil, which is not allowed")
	}
	return nil
}

// SQLiteStore implements infra.KVStore on the shared SQLite file's kv table.
type SQLiteStore struct {
	db    *sql.DB
	codec infra.Codec
}

func NewSQLiteStore(db *sql.DB, codec infra.Codec) *SQLiteStore {
	return &SQLiteStore{db: db, codec: codec}
}

func (s *SQLiteStore) GetName() string { return "sqlite" }

func (s *SQLiteStore) get(key string) ([]byte, error) {
	if key == "" {
		return nil, ErrKeyEmpty
	}
	var v []byte
	err := s.db.QueryRow(`SELECT value FROM kv WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrKeyNotFound
	}
	return v, err
}

func (s *SQLiteStore) set(key string, value []byte) error {
	if key == "" {
		return ErrKeyEmpty
	}
	_, err := s.db.Exec(
		`INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value,
	)
	return err
}

func (s *SQLiteStore) Get(key string) (string, error) {
	v, err := s.get(key)
	return string(v), err
}

func (s *SQLiteStore) Set(key, value string) error {
	return s.set(key, []byte(value))
}

func (s *SQLiteStore) SetAny(key string, value any) error {
	if err := checkKeyAndValue(key, value); err != nil {
		return err
	}
	data, err := s.codec.Marshal(value)
	if err != nil {
		return err
	}
	return s.set(key, data)
}

func (s *SQLiteStore) GetAny(key string, value any) (bool, error) {
	if err := checkKeyAndValue(key, value); err != nil {
		return false, err
	}
	data, err := s.get(key)
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, s.codec.Unmarshal(data, value)
}

func (s *SQLiteStore) List(prefix string) ([]*infra.KVPair, error) {
	if prefix == "" {
		return nil, errors.New("prefix is empty")
	}
	// Range scan on the primary key; 0xff never appears in UTF-8 keys, so it bounds the prefix.
	rows, err := s.db.Query(`SELECT key, value FROM kv WHERE key >= ? AND key < ? ORDER BY key`, prefix, prefix+"\xff")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*infra.KVPair, 0)
	for rows.Next() {
		p := &infra.KVPair{}
		if err := rows.Scan(&p.Key, &p.Value); err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) Delete(key string) error {
	if key == "" {
		return ErrKeyEmpty
	}
	_, err := s.db.Exec(`DELETE FROM kv WHERE key = ?`, key)
	return err
}

// BatchSet writes all pairs in one transaction.
func (s *SQLiteStore) BatchSet(pairs []infra.KVPair) error {
	if len(pairs) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, p := range pairs {
		if _, err := tx.Exec(
			`INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
			p.Key, p.Value,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Close is a no-op: the *sql.DB is shared and closed by its owner.
func (s *SQLiteStore) Close() error { return nil }
