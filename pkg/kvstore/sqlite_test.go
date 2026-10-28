package kvstore

import (
	"path/filepath"
	"testing"

	"github.com/fystack/multichain-indexer/pkg/infra"
	"github.com/stretchr/testify/require"
)

func TestSQLiteStore(t *testing.T) {
	db, err := infra.OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	defer db.Close()
	s := NewSQLiteStore(db, infra.JSON)

	_, err = s.Get("missing")
	require.ErrorIs(t, err, ErrKeyNotFound)

	require.NoError(t, s.Set("k", "v1"))
	require.NoError(t, s.Set("k", "v2"))
	v, err := s.Get("k")
	require.NoError(t, err)
	require.Equal(t, "v2", v)

	require.NoError(t, s.SetAny("blocks", []uint64{1, 2}))
	var blocks []uint64
	found, err := s.GetAny("blocks", &blocks)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []uint64{1, 2}, blocks)

	require.NoError(t, s.BatchSet([]infra.KVPair{
		{Key: "states/eth/catchup/1-10", Value: []byte("5")},
		{Key: "states/eth/catchup/11-20", Value: []byte("11")},
		{Key: "states/eth/catchupX", Value: []byte("no")},
		{Key: "states/btc/catchup/1-2", Value: []byte("1")},
	}))
	pairs, err := s.List("states/eth/catchup/")
	require.NoError(t, err)
	require.Len(t, pairs, 2)
	require.Equal(t, "states/eth/catchup/1-10", pairs[0].Key)
	require.Equal(t, []byte("5"), pairs[0].Value)

	require.NoError(t, s.Delete("k"))
	_, err = s.Get("k")
	require.ErrorIs(t, err, ErrKeyNotFound)
}
