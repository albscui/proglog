package server_test

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	api "github.com/albscui/proglog/api/v1"
	"github.com/albscui/proglog/internal/log"
	"github.com/albscui/proglog/internal/server"
)

type testCase func(*testing.T, api.LogClient, *server.Config)

// TestServer defines all test cases and runs a subtest for each.
func TestServer(t *testing.T) {
	for name, test := range map[string]testCase{
		"produce/consume a message to/from the log succeeds": testProduceConsume,
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
	client = api.NewLogClient(cc)

	// Create our server and start serving requests in a goroutine.
	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	config = &server.Config{CommitLog: clog}
	gsrv, err := server.NewGRPCServer(config)
	require.NoError(t, err)

	// non-blocking
	go func() {
		gsrv.Serve(l)
	}()

	teardown = func() {
		gsrv.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
	return
}

func testProduceConsume(t *testing.T, client api.LogClient, config *server.Config) {
	ctx := context.Background()

	want := &api.Record{Value: []byte("hello world")}
	produceRes, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consumeRes, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produceRes.Offset})
	require.NoError(t, err)

	require.Equal(t, want.Value, consumeRes.Record.Value)
	require.Equal(t, want.Offset, consumeRes.Record.Offset)
}
