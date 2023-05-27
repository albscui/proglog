package log

import (
	"io"
	"os"
	"testing"

	api "github.com/albscui/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	// dir, err := ioutil.TempDir(os.TempDir(), t.Name())
	dir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)
	defer os.Remove(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{Segment: Segment{MaxStoreBytes: 1024, MaxIndexBytes: entryWidth * 3}}

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.baseOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	// maxed index
	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	// maxed store
	require.True(t, s.IsMaxed())

	require.NoError(t, s.Remove())
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
