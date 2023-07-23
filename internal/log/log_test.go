package log

import (
	"os"
	"testing"

	api "github.com/albscui/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	tests := map[string]func(t *testing.T, l *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		// "reader":                            testReader,
		// "truncate":                          testTruncate,
	}
	for scenario, testFn := range tests {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			require.NoError(t, err)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			l, err := NewLog(dir, c)
			require.NoError(t, err)
			testFn(t, l)
		})
	}
}

func testAppendRead(t *testing.T, curLog *Log) {
	for i, s := range []string{"hello world", "foo bar", "alice bob", "a"} {
		expectedRecord := &api.Record{Value: []byte(s)}
		off, err := curLog.Append(expectedRecord)
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)

		gotRecord, err := curLog.Read(off)
		require.NoError(t, err)
		require.Equal(t, expectedRecord.Value, gotRecord.Value)
	}
}

func testOutOfRangeErr(t *testing.T, curLog *Log) {
	read, err := curLog.Read(1)
	require.Nil(t, read)
	require.ErrorContains(t, err, "offset out of range")
}

func testInitExisting(t *testing.T, oldLog *Log) {
	record := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := oldLog.Append(record)
		require.NoError(t, err)
	}
	require.NoError(t, oldLog.Close())

	off, err := oldLog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = oldLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	// Initialize new log based on existing files
	newLog, err := NewLog(oldLog.Dir, oldLog.Config)
	require.NoError(t, err)

	off, err = newLog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = newLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}
