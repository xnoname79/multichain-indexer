package missingblockstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/fystack/multichain-indexer/pkg/common/constant"
)

type BlockRange struct {
	StartBlock uint64
	EndBlock   uint64
}

const MaxBlocksPerRange = 5

type MissingBlocksStore interface {
	AddMissingBlockRange(ctx context.Context, networkCode string, start, end uint64) error
	GetNextRange(ctx context.Context, networkCode string) (uint64, uint64, error)
	RemoveRange(ctx context.Context, networkCode string, start, end uint64) error
	ListRanges(ctx context.Context, networkCode string) ([]BlockRange, error)
	CountRanges(ctx context.Context, networkCode string) (int64, error)
}

// missingBlocksStore keeps manual-worker ranges in the SQLite missing_ranges table.
type missingBlocksStore struct {
	db *sql.DB
}

func NewMissingBlocksStore(db *sql.DB) MissingBlocksStore {
	return &missingBlocksStore{db: db}
}

// AddMissingBlockRange merges [start,end] with overlapping or adjacent ranges and
// stores the result split into chunks of MaxBlocksPerRange.
func (m *missingBlocksStore) AddMissingBlockRange(ctx context.Context, networkCode string, start, end uint64) error {
	if start == 0 || end == 0 || start > end {
		return fmt.Errorf("invalid range: start=%d, end=%d", start, end)
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete with the original bounds: stored chunks sit next to each other, so the
	// widened bounds would also catch neighbours that were not merged.
	const touching = `chain = ? AND end_block + 1 >= ? AND start_block <= ? + 1`
	var minStart, maxEnd sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT MIN(start_block), MAX(end_block) FROM missing_ranges WHERE `+touching,
		networkCode, start, end,
	).Scan(&minStart, &maxEnd); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM missing_ranges WHERE `+touching, networkCode, start, end); err != nil {
		return err
	}
	if minStart.Valid && uint64(minStart.Int64) < start {
		start = uint64(minStart.Int64)
	}
	if maxEnd.Valid && uint64(maxEnd.Int64) > end {
		end = uint64(maxEnd.Int64)
	}

	for from := start; from <= end; from += MaxBlocksPerRange {
		to := min(from+MaxBlocksPerRange-1, end)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO missing_ranges (chain, start_block, end_block) VALUES (?, ?, ?)`,
			networkCode, from, to,
		); err != nil {
			return fmt.Errorf("failed to add missing block range: %w", err)
		}
	}
	return tx.Commit()
}

// GetNextRange claims the lowest unclaimed range for RangeProcessingTimeout.
// A range not removed in time becomes claimable again. Returns 0,0 when none is available.
func (m *missingBlocksStore) GetNextRange(ctx context.Context, networkCode string) (uint64, uint64, error) {
	now := time.Now().Unix()
	var start, end uint64
	err := m.db.QueryRowContext(ctx, `
		UPDATE missing_ranges SET locked_until = ?
		WHERE chain = ? AND start_block = (
			SELECT start_block FROM missing_ranges
			WHERE chain = ? AND locked_until <= ?
			ORDER BY start_block LIMIT 1
		)
		RETURNING start_block, end_block`,
		now+int64(constant.RangeProcessingTimeout.Seconds()), networkCode, networkCode, now,
	).Scan(&start, &end)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("failed to claim range: %w", err)
	}
	return start, end, nil
}

func (m *missingBlocksStore) RemoveRange(ctx context.Context, networkCode string, start, end uint64) error {
	if _, err := m.db.ExecContext(ctx,
		`DELETE FROM missing_ranges WHERE chain = ? AND start_block = ? AND end_block = ?`,
		networkCode, start, end,
	); err != nil {
		return fmt.Errorf("failed to remove range: %w", err)
	}
	return nil
}

func (m *missingBlocksStore) ListRanges(ctx context.Context, networkCode string) ([]BlockRange, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT start_block, end_block FROM missing_ranges WHERE chain = ? ORDER BY start_block`,
		networkCode,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ranges: %w", err)
	}
	defer rows.Close()

	ranges := make([]BlockRange, 0)
	for rows.Next() {
		var r BlockRange
		if err := rows.Scan(&r.StartBlock, &r.EndBlock); err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}
	return ranges, rows.Err()
}

func (m *missingBlocksStore) CountRanges(ctx context.Context, networkCode string) (int64, error) {
	var n int64
	if err := m.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM missing_ranges WHERE chain = ?`, networkCode,
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("failed to count ranges: %w", err)
	}
	return n, nil
}
