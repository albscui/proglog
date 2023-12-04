package server_test

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	api "github.com/albscui/proglog/api/v1"
	"github.com/albscui/proglog/internal/log"
	"github.com/albscui/proglog/internal/server"
)

type testCase func(*testing.T, api.LogClient, *server.Config)

// TestServer defines all test cases and runs a subtest for each.
func TestServer(t *testing.T) {
	for name, test := range map[string]testCase{
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(name, func(t *testing.T) {
			client, config, teardown := setupTest(t)
			defer teardown()
			test(t, client, config)
		})
	}
}

// setupTest creates the client and runs the server locally.
func setupTest(t *testing.T) (client api.LogClient, config *server.Config, teardown func()) {
	t.Helper()

	// Create a listener on the local network address that our server will run on.
	// The 0 port will automatically assign us a free port.
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// Create an insecure connection to our listener, which will be used by our client.
	cc, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	// Create our server and start serving requests in a goroutine.
	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	config = &server.Config{CommitLog: clog}
	gsrv, err := server.NewGRPCServer(config)
	require.NoError(t, err)

	// non-blocking
	go gsrv.Serve(l)

	client = api.NewLogClient(cc)

	teardown = func() {
		gsrv.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
	return
}

// tests that producing and consuming works by using our client and server to produce a redord to the log,
// consume it back, and then check the record we sent is the same one we got back.
func testProduceConsume(t *testing.T, client api.LogClient, _ *server.Config) {
	ctx := context.Background()

	want := &api.Record{Value: []byte("hello world")}
	produceRes, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consumeRes, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produceRes.Offset})
	require.NoError(t, err)

	require.Equal(t, want.Value, consumeRes.Record.Value)
	require.Equal(t, want.Offset, consumeRes.Record.Offset)
}

// tests that our server responds with an api.ErrOffsetOutOfRange error when a client tries to consume beyong the log's boundaries.
func testConsumePastBoundary(t *testing.T, client api.LogClient, _ *server.Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	if consume != nil {
		t.Fatal("consume must be nil")
	}

	got := status.Code(err)
	want := status.Code(api.OffsetOutOfRangeError{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

// testProduceConsumeStream tests whether we can produce and consume through stream.
func testProduceConsumeStream(t *testing.T, client api.LogClient, _ *server.Config) {
	ctx := context.Background()

	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}

	stream, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	for _, record := range records {
		require.NoError(t, stream.Send(&api.ProduceRequest{Record: record}))
		res, err := stream.Recv()
		require.NoError(t, err)
		if res.Offset != record.Offset {
			t.Fatalf("got offset: %d, want: %d", res.Offset, record.Offset)
		}
	}

	consumeStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)

	for i, record := range records {
		res, err := consumeStream.Recv()
		require.NoError(t, err)
		require.Equal(t, res.Record, &api.Record{Value: record.Value, Offset: uint64(i)})
	}
}
