package log

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	api "github.com/albscui/proglog/api/v1"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
)

// DistributedLog is a distributed, replicated log built with Raft.
type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{config: config}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return l, nil
}

// setupLog creates the log for this server, where this server will store the user's records.
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	if err != nil {
		return err
	}
	return nil
}

// setupRaft creates and configures the server's Raft instance.
// Generally, you'll bootstream a server configured with itself as the only voter,
// wait until it become the leader, and then tell the leader to add more servers to the cluster.
// The subsequently added servers don't bootstrap.
func (l *DistributedLog) setupRaft(dataDir string) error {
	// Create our finite-state machine (FSM)
	// FSM has access to our user's logs because it will apply the commands and write actual records to it.
	fsm := &fsm{log: l.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	// Create another log for Raft's internals. Notice we use our own Log for Raft!
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1 // Raft requires the initial offset to be 1
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// The stable store is a key-value store where Raft stores important metadata, like the server's current term or the candidate the server voted for.
	// Bolt is an embedded and persisted key-value database for Go.
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	// Raft snapshots to recover and restore data efficiently.
	// When a server fails, and a new server is brought back, rather than streaming all data from the leader, it can restore from a snapshot
	// and then get the latest changes from the leader.
	// retain=1 specifies we'll keep one snapshot.
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"), 1 /* retain */, os.Stderr)
	if err != nil {
		return err
	}

	// We create our transport that wraps our own stream layer.
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(l.config.Raft.StreamLayer, 5 /* maxPool */, timeout, os.Stderr)

	// Under normal operation, the default config is fine.
	// We support overriding the default timeouts to speed up Raft for testing.
	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID // unique ID for this server
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	// Create the Raft instance, and bootstrap cluster.
	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		if err := l.raft.BootstrapCluster(config).Error(); err != nil {
			return err
		}
	}
	return nil
}

// Append appends the record to the log. Unlike Log.Append, where we append directly,
// we tell Raft to apply a command that tells the FSM to append the record to the log.
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(AppendRequestType, &api.ProduceRequest{Record: record})
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

// apply wraps Raft's API to apply requests and return their responses.
// Useful for handling different request types.
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (any, error) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout) // Raft magic
	if future.Error() != nil {
		// Something went wrong with Raft's replication.
		// Not to be confused with our service's errors.
		return nil, future.Error()
	}
	// FSM's response.
	res := future.Response()
	if err, ok := res.(error); ok {
		// this is the error returned by our service
		return nil, err
	}
	return res, nil
}

// Read reads the record for the offset from the server's log.
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	// When you are okay with relaxing consistency, then we don't need to go through Raft.
	// For strong consistency, use Raft.
	return l.log.Read(offset)
}

var _ raft.FSM = &fsm{}

// fsm implements the raft.FSM interface
type fsm struct {
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Apply is involked by Raft, after commiting a log entry.
func (m *fsm) Apply(record *raft.Log) any {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return m.applyAppend(buf[1:])
	}
	return nil
}

func (m *fsm) applyAppend(b []byte) any {
	var req api.ProduceRequest
	if err := proto.Unmarshal(b, &req); err != nil {
		return err
	}
	offset, err := m.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

// Snapshot -- Raft periodically calls this method to snapshot its state.
// Snapshots serve 2 purposes:
// 1. They allow Raft to compact its log so it doesn't store logs whose commands Raft has applied already.
// 2. They allow Raft to bootstrap new servers more efficiently than if the leader had to replicate its entire log again.
// Raft calls Snapshot according to your configured SnapshotInterval -- default is two minutes.
// and SnapshotThreshold (how many logs since the last snapshot before making a new one) -- default is 8192.
func (m *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := m.log.Reader()
	return &snapshot{reader: r}, nil
}

// Restore -- Raft calls this to restore an FSM from snapshot.
func (m *fsm) Restore(snapshot io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(snapshot, b)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		size := int64(enc.Uint64(b))
		_, err = io.CopyN(&buf, snapshot, size)
		if err != nil {
			return err
		}
		record := &api.Record{}
		if err := proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			// first record found, reset log, and set its initial offset to the record's offset
			m.log.Config.Segment.InitialOffset = record.Offset
			if err := m.log.Reset(); err != nil {
				return err
			}
		}
		// keep appending records to our new log
		if _, err = m.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

var _ raft.FSMSnapshot = &snapshot{}

// snapshot implements raft.FSMSnapshot
type snapshot struct {
	reader io.Reader
}

// Persist -- Raft calls this to write to a sink that, depending on the snapshot store you configured Raft with,
// could be in-memory, a file, an S3 bucket -- something to store the bytes in.
// We're using the file snapshot store so that when the snapshot completes, we'll have a file containing all the Raft's log data.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release -- Raft calls this when it's finished with the snapshot.
func (s *snapshot) Release() {}

var _ raft.LogStore = &logStore{}

// logStore wraps our Log, and implements raft.LogStore.
// Raft stores its internal log here.
type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	indexEntry, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Index = indexEntry.Offset
	out.Data = indexEntry.Value
	out.Type = raft.LogType(indexEntry.Type)
	out.Term = indexEntry.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		_, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

// StreamLayer implements Raft's StreamLayer.
// Raft uses a stream layer in the transport to provide low-level stream abstration to connection with Raft servers.
type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config // for accepting incoming connections
	peerTLSConfig   *tls.Config // for creating outgoing connections
}

func NewSteamLayer(ln net.Listener, serverTLSConfig, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{ln: ln, serverTLSConfig: serverTLSConfig, peerTLSConfig: peerTLSConfig}
}

const RaftRPC = 1

func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	// identify to mux this is a raft rpc
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

// Accept accepts the incoming connection and read the byte that identifies the connection and then create a server-side TLS connection.
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
