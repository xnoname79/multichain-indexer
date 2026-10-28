package missingblockstore

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fystack/multichain-indexer/pkg/infra"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) MissingBlocksStore {
	t.Helper()
	db, err := infra.OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return NewMissingBlocksStore(db)
}

func TestAddMissingBlockRange_SplitsAndMerges(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	require.NoError(t, s.AddMissingBlockRange(ctx, "eth", 1, 12))
	require.NoError(t, s.AddMissingBlockRange(ctx, "eth", 30, 31))
	// Overlaps [11,12] chunk and is adjacent to nothing else: [1..10] chunks must survive.
	require.NoError(t, s.AddMissingBlockRange(ctx, "eth", 12, 14))
	require.NoError(t, s.AddMissingBlockRange(ctx, "btc", 5, 5))

	ranges, err := s.ListRanges(ctx, "eth")
	require.NoError(t, err)
	require.Equal(t, []BlockRange{{1, 5}, {6, 10}, {11, 14}, {30, 31}}, ranges)

	n, err := s.CountRanges(ctx, "btc")
	require.NoError(t, err)
	require.EqualValues(t, 1, n)

	require.Error(t, s.AddMissingBlockRange(ctx, "eth", 0, 3))
	require.Error(t, s.AddMissingBlockRange(ctx, "eth", 9, 3))
}

func TestGetNextRange_ClaimsInOrderAndRemoves(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.AddMissingBlockRange(ctx, "eth", 1, 8))

	start, end, err := s.GetNextRange(ctx, "eth")
	require.NoError(t, err)
	require.Equal(t, [2]uint64{1, 5}, [2]uint64{start, end})

	// First range is locked, so the next claim gets the following one.
	start, end, err = s.GetNextRange(ctx, "eth")
	require.NoError(t, err)
	require.Equal(t, [2]uint64{6, 8}, [2]uint64{start, end})

	start, end, err = s.GetNextRange(ctx, "eth")
	require.NoError(t, err)
	require.Zero(t, start+end, "all ranges locked")

	require.NoError(t, s.RemoveRange(ctx, "eth", 1, 5))
	ranges, err := s.ListRanges(ctx, "eth")
	require.NoError(t, err)
	require.Equal(t, []BlockRange{{6, 8}}, ranges)
}
