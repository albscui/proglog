package server

import (
	"context"
	"flag"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	api "github.com/albscui/proglog/api/v1"
	"github.com/albscui/proglog/internal/auth"
	"github.com/albscui/proglog/internal/config"
	"github.com/albscui/proglog/internal/log"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

// Go will run TestMain instead of running tests directly.
// TestMain gives us a place for setup that applies to all tests in this file.
func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

// TestServer defines all test cases and runs a subtest for each.
func TestServer(t *testing.T) {
	for name, test := range map[string]testCase{
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized":                                       testUnauthorized,
	} {
		t.Run(name, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t)
			defer teardown()
			test(t, rootClient, nobodyClient, config)
		})
	}
}

// setupTest creates the client and runs the server locally.
func setupTest(t *testing.T) (rootClient api.LogClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	// Create a listener on the local network address that our server will run on.
	// The 0 port will automatically assign us a free port.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// helper function to create multiple clients
	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		// Configure our client's TLS credentials to use our CA as the client's Root CA (the CA it will use to verify the server).
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(config.RootClientCertFile, config.RootClientKeyFile)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	// Hook up our server with its certificate and enable it to handle TLS connections.
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	// Create our server and start serving requests in a goroutine.
	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// Auth
	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	// Starts the telemetry exporter to write to two files.
	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := os.CreateTemp("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	gsrv, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	// non-blocking
	go func() {
		err := gsrv.Serve(l)
		require.NoError(t, err)
	}()

	teardown = func() {
		gsrv.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove()
		if telemetryExporter != nil {
			// sleep for 1.5 secs to give the telemetry exporter enough time to flush its data to disk.
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
	return
}

// tests that producing and consuming works by using our client and server to produce a redord to the log,
// consume it back, and then check the record we sent is the same one we got back.
func testProduceConsume(t *testing.T, client, _ api.LogClient, _ *Config) {
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
func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, _ *Config) {
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
func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, _ *Config) {
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

func testUnauthorized(
	t *testing.T,
	_,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	produce, err := client.Produce(ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}

type testCase func(*testing.T, api.LogClient, api.LogClient, *Config)
