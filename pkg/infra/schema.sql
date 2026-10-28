-- All indexer state lives in one SQLite file.

-- Block progress, catchup ranges, failed blocks, block hashes (KVStore).
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value BLOB NOT NULL
) WITHOUT ROWID;

-- Watched addresses. Lookups are exact, so no bloom filter is needed.
CREATE TABLE IF NOT EXISTS wallet_addresses (
    type    TEXT NOT NULL,
    address TEXT NOT NULL,
    PRIMARY KEY (type, address)
) WITHOUT ROWID;

-- Block ranges queued for the manual worker.
CREATE TABLE IF NOT EXISTS missing_ranges (
    chain        TEXT    NOT NULL,
    start_block  INTEGER NOT NULL,
    end_block    INTEGER NOT NULL,
    locked_until INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chain, start_block)
) WITHOUT ROWID;

-- Matched events waiting for webhook delivery. Rows are deleted once delivered.
CREATE TABLE IF NOT EXISTS outbox (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    topic   TEXT NOT NULL,
    key     TEXT UNIQUE,
    payload BLOB NOT NULL
);
