package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	expectedData = []byte("hello world")
	width        = uint64(len(expectedData)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", t.Name())
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// verify our service will recover after a restart
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := 0; i < 3; i++ {
		n, pos, err := s.Append(expectedData)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*uint64(i+1))
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := 0; i < 3; i++ {
		data, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, expectedData, data)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := 0, int64(0); i < 3; i++ {
		// read the size first
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		// read the data
		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, expectedData, b)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", t.Name())
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(expectedData)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	require.NoError(t, s.Close())
	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
