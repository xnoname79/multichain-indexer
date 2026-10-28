package pubkeystore

import (
	"path/filepath"
	"testing"

	"github.com/fystack/multichain-indexer/pkg/common/enum"
	"github.com/fystack/multichain-indexer/pkg/infra"
	"github.com/stretchr/testify/require"
)

func TestPublicKeyStore_ExactMatch(t *testing.T) {
	db, err := infra.OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	defer db.Close()
	s := NewPublicKeyStore(db)

	require.NoError(t, s.Save(enum.NetworkTypeTron, "TAWdqnuYCNU3dKsi7pR8d7sDkx1Evb2giV"))
	require.NoError(t, s.Save(enum.NetworkTypeTron, "TAWdqnuYCNU3dKsi7pR8d7sDkx1Evb2giV")) // idempotent

	require.True(t, s.Exist(enum.NetworkTypeTron, "TAWdqnuYCNU3dKsi7pR8d7sDkx1Evb2giV"))
	require.False(t, s.Exist(enum.NetworkTypeEVM, "TAWdqnuYCNU3dKsi7pR8d7sDkx1Evb2giV"), "scoped by type")
	require.False(t, s.Exist(enum.NetworkTypeTron, "tawdqnuycnu3dksi7pr8d7sdkx1evb2giv"), "case-sensitive")
	require.False(t, s.Exist(enum.NetworkTypeTron, ""))
	require.Equal(t, []bool{true, false},
		s.ExistBatch(enum.NetworkTypeTron, []string{"TAWdqnuYCNU3dKsi7pR8d7sDkx1Evb2giV", "other"}))
}
