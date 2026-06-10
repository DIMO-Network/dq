package fsstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/DIMO-Network/dq/internal/materializer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir())
	require.NoError(t, err)
	return s
}

func TestNew_RejectsRelativeRoot(t *testing.T) {
	t.Parallel()
	_, err := New("relative/path")
	require.Error(t, err)
}

func TestGetObject_MissingWrapsErrNotFound(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	_, err := s.GetObject(context.Background(), "decoded/v1/_state/watermark.json")
	require.Error(t, err)
	assert.True(t, errors.Is(err, materializer.ErrNotFound))
}

func TestPutOverwritesWatermarkAtomically(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.PutObject(ctx, "decoded/v1/_state/watermark.json", []byte(`{"p":"k1"}`)))
	require.NoError(t, s.PutObject(ctx, "decoded/v1/_state/watermark.json", []byte(`{"p":"k2"}`)))

	got, err := s.GetObject(ctx, "decoded/v1/_state/watermark.json")
	require.NoError(t, err)
	assert.JSONEq(t, `{"p":"k2"}`, string(got))

	entries, err := os.ReadDir(filepath.Join(s.root, "decoded", "v1", "_state"))
	require.NoError(t, err)
	require.Len(t, entries, 1, "no temp residue")
}

func TestList_SortedAndPrefixed(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.PutObject(ctx, "raw/type=dimo.status/date=2026-06-10/ingest-2.parquet", []byte("b")))
	require.NoError(t, s.PutObject(ctx, "raw/type=dimo.status/date=2026-06-09/ingest-1.parquet", []byte("a")))
	require.NoError(t, s.PutObject(ctx, "raw/type=dimo.fingerprint/date=2026-06-10/ingest-3.parquet", []byte("c")))

	all, err := s.List(ctx, "raw/type=")
	require.NoError(t, err)
	require.Len(t, all, 3)
	for i := 1; i < len(all); i++ {
		assert.Less(t, all[i-1].Key, all[i].Key, "sorted like S3")
	}

	one, err := s.List(ctx, "raw/type=dimo.status/")
	require.NoError(t, err)
	require.Len(t, one, 2)
}

func TestList_MissingPrefixEmpty(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	out, err := s.List(context.Background(), "raw/type=")
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestList_HidesTempFiles(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.PutObject(ctx, "raw/a.parquet", []byte("x")))
	require.NoError(t, os.WriteFile(filepath.Join(s.root, "raw", tempPrefix+"42"), []byte("partial"), 0o644))

	out, err := s.List(ctx, "raw/")
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "raw/a.parquet", out[0].Key)
}
