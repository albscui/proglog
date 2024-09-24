package log_test

import (
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	api "github.com/albscui/proglog/api/v1"
	"github.com/albscui/proglog/internal/log"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMultipleNodes(t *testing.T) {
	var logs []*log.DistributedLog
	nodeCount := 3
	ports := dynaport.Get(nodeCount)

	// Set up a cluster consisting of three servers.
	for i := 0; i < nodeCount; i++ {
		dataDir, err := os.MkdirTemp("", "distributed-log")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		config := log.Config{}
		config.Raft.StreamLayer = log.NewSteamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond
		if i == 0 {
			// The first server bootstraps the cluster, becomes the leader, and adds the other two servers to the cluster.
			config.Raft.Bootstrap = true
		}

		l, err := log.NewDistributedLog(dataDir, config)
		require.NoError(t, err)
		if i != 0 {
			// worker
			err = logs[0].Join(fmt.Sprintf("%d", i), ln.Addr().String())
			require.NoError(t, err)
		} else {
			err = l.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}
		logs = append(logs, l)
		defer l.Close()
	}

	// Append some records via the leader, and verify the followers replicated them.
	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	leader := logs[0]
	for _, record := range records {
		off, err := leader.Append(record)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				got, err := logs[j].Read(off)
				if err != nil {
					return false
				}
				record.Offset = off
				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	// Remove a server from the cluster.
	err := logs[0].Leave("1")
	require.NoError(t, err)

	// Wait for the server to leave.
	time.Sleep(50 * time.Millisecond)

	off, err := leader.Append(&api.Record{Value: []byte("third")})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	record, err := logs[1].Read(off)
	assert.True(t, errors.As(err, new(*log.OffsetOutOfRangeError)))
	require.IsType(t, &log.OffsetOutOfRangeError{}, err)
	require.Nil(t, record)

	record, err = logs[2].Read(off)
	require.NoError(t, err)
	require.Equal(t, []byte("third"), record.Value)
	require.Equal(t, off, record.Offset)
}
