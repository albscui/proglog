package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), t.Name())
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{Segment: Segment{MaxIndexBytes: 1024}}
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Error(t, err)

	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		off uint32
		pos uint64
	}{
		{off: 0, pos: 0},
		{off: 1, pos: 10},
	}
	for _, entry := range entries {
		err = idx.Write(entry.off, entry.pos)
		require.NoError(t, err)

		off, pos, err := idx.Read(int64(entry.off))
		require.NoError(t, err)
		require.Equal(t, entry.off, off)
		require.Equal(t, entry.pos, pos)
	}

	// index and scanner should error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.ErrorIs(t, io.EOF, err)
	require.NoError(t, idx.Close())

	// index should build its state from the existing file
	f2, err := os.OpenFile(f.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)
	idx, err = newIndex(f2, c)
	require.NoError(t, err)

	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, uint64(10), pos)
}
