package backend

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const forwardingTestToken = "test-forwarding-token"

type forwardingPeer struct {
	clusterpb.UnimplementedClusterBackendServiceServer
	label          string
	blocked        chan struct{}
	uncertainCalls atomic.Int32
}

func (p *forwardingPeer) ExecuteRaftGraphRead(ctx context.Context, req *clusterpb.ExecuteRaftGraphReadRequest) (*clusterpb.ExecuteRaftGraphReadResponse, error) {
	if !slices.Equal(metadata.ValueFromIncomingContext(ctx, "mycel-cluster-token"), []string{forwardingTestToken}) {
		return nil, status.Error(codes.PermissionDenied, "missing graph-forward auth")
	}
	if string(req.GetPayload()) == "block" {
		close(p.blocked)
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &clusterpb.ExecuteRaftGraphReadResponse{Payload: []byte(p.label + ":" + string(req.GetPayload()))}, nil
}

func (p *forwardingPeer) ForwardClientRequest(ctx context.Context, req *clusterpb.ForwardClientRequestRequest) (*clusterpb.ForwardClientRequestResponse, error) {
	if !slices.Equal(metadata.ValueFromIncomingContext(ctx, "mycel-cluster-token"), []string{forwardingTestToken}) {
		return nil, status.Error(codes.PermissionDenied, "missing client-forward auth")
	}
	if req.GetOperation() == "transaction.commit" {
		p.uncertainCalls.Add(1)
		return nil, status.Error(codes.Unavailable, "commit outcome not confirmed")
	}
	return &clusterpb.ForwardClientRequestResponse{Payload: []byte(p.label + ":" + string(req.GetPayload())), PayloadType: req.GetPayloadType()}, nil
}

func serveForwardingPeer(t *testing.T, handler *forwardingPeer) (*bufconn.Listener, *grpc.Server) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	clusterpb.RegisterClusterBackendServiceServer(server, handler)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener, server
}

func newTestForwardingClient(t *testing.T, dialer func(context.Context, string) (net.Conn, error), peers ...string) (Client, *ForwardingConnPool) {
	t.Helper()
	client := Client{AuthToken: forwardingTestToken, DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer)}}
	pool := NewForwardingConnPool(client, peers)
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Error(err)
		}
	})
	return client.WithForwardingConnPool(pool), pool
}

func forwardingRequest(operation, payload string) ForwardClientRequestInput {
	return ForwardClientRequestInput{ClusterID: "test-cluster", Operation: operation, RequesterNode: 1, TargetNode: 2, Principal: ForwardedPrincipal{Kind: "human", PrincipalID: "test-user"}, PayloadType: PayloadTypeProto, Payload: []byte(payload)}
}

func TestForwardingConnPoolReusesChannelAcrossRPCsAndPeers(t *testing.T) {
	peerA, _ := serveForwardingPeer(t, &forwardingPeer{label: "first"})
	peerB, _ := serveForwardingPeer(t, &forwardingPeer{label: "second"})
	var dialsA, dialsB atomic.Int32
	client, pool := newTestForwardingClient(t, func(ctx context.Context, addr string) (net.Conn, error) {
		switch addr {
		case "peer-a":
			dialsA.Add(1)
			return peerA.DialContext(ctx)
		case "peer-b":
			dialsB.Add(1)
			return peerB.DialContext(ctx)
		default:
			return nil, fmt.Errorf("unexpected test dial %q", addr)
		}
	}, "peer-a", "peer-b")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	for i := range 32 {
		workers.Go(func() {
			payload := fmt.Sprintf("call-%d", i)
			var response []byte
			var err error
			if i%2 == 0 {
				response, err = client.ExecuteRaftGraphRead(ctx, "peer-a", "test-space", []byte(payload))
			} else {
				result, forwardErr := client.ForwardClientRequest(ctx, "peer-a", forwardingRequest("transaction.get", payload))
				response, err = result.Payload, forwardErr
			}
			if err != nil || string(response) != "first:"+payload {
				t.Errorf("forward %d: %q (%v)", i, response, err)
			}
		})
	}
	workers.Wait()
	for range 3 {
		result, err := client.ForwardClientRequest(ctx, "peer-b", forwardingRequest("session.get", "control"))
		if err != nil || string(result.Payload) != "second:control" || result.PayloadType != PayloadTypeProto {
			t.Fatalf("forward to second peer: %+v (%v)", result, err)
		}
	}
	if _, err := client.ExecuteRaftGraphRead(ctx, "unconfigured", "test-space", nil); status.Code(err) != codes.Unavailable {
		t.Fatalf("unconfigured destination was not rejected: %v", err)
	}
	if dialsA.Load() != 1 || dialsB.Load() != 1 {
		t.Fatalf("transport dials = (%d, %d), want one per peer across repeated and concurrent RPCs", dialsA.Load(), dialsB.Load())
	}
	connA, err := pool.conn(ctx, "peer-a")
	if err != nil {
		t.Fatal(err)
	}
	connB, err := pool.conn(ctx, "peer-b")
	if err != nil || connB == connA {
		t.Fatalf("second peer did not get a distinct channel: %v", err)
	}
	if err := pool.Close(); err != nil || connA.GetState() != connectivity.Shutdown || connB.GetState() != connectivity.Shutdown {
		t.Fatalf("pool close did not shut down channels: %v", err)
	}
	if _, err := client.ForwardClientRequest(ctx, "peer-b", forwardingRequest("session.get", "closed")); status.Code(err) != codes.Unavailable {
		t.Fatalf("forward after pool shutdown: %v", err)
	}
}

func TestForwardingConnPoolKeepsCancellationAndWriteErrorsRequestLocal(t *testing.T) {
	peer := &forwardingPeer{label: "peer", blocked: make(chan struct{})}
	listener, _ := serveForwardingPeer(t, peer)
	var dials atomic.Int32
	client, _ := newTestForwardingClient(t, func(ctx context.Context, _ string) (net.Conn, error) {
		dials.Add(1)
		return listener.DialContext(ctx)
	}, "peer")
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := client.ExecuteRaftGraphRead(canceled, "peer", "test-space", nil); status.Code(err) != codes.Canceled || dials.Load() != 0 {
		t.Fatalf("an already canceled request started a channel or lost its status: %v", err)
	}
	request, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	blocked := make(chan error, 1)
	go func() {
		_, err := client.ExecuteRaftGraphRead(request, "peer", "test-space", []byte("block"))
		blocked <- err
	}()
	select {
	case <-peer.blocked:
	case err := <-blocked:
		t.Fatalf("blocking read returned early: %v", err)
	case <-ctx.Done():
		t.Fatal("blocking read did not reach peer")
	}
	if _, err := client.ForwardClientRequest(ctx, "peer", forwardingRequest("transaction.commit", "write")); status.Code(err) != codes.Unavailable || peer.uncertainCalls.Load() != 1 {
		t.Fatalf("ambiguous write did not return unchanged or was retried: %v, attempts=%d", err, peer.uncertainCalls.Load())
	}
	cancelRequest()
	select {
	case err := <-blocked:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("blocking request returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("canceled read did not return")
	}
	if result, err := client.ExecuteRaftGraphRead(ctx, "peer", "test-space", []byte("still-live")); err != nil || string(result) != "peer:still-live" || dials.Load() != 1 {
		t.Fatalf("cancellation or unavailable response killed the channel: %q %v, dials=%d", result, err, dials.Load())
	}
}

func TestForwardingConnPoolReconnectsAfterPeerRestart(t *testing.T) {
	first, firstServer := serveForwardingPeer(t, &forwardingPeer{label: "first"})
	var active atomic.Pointer[bufconn.Listener]
	active.Store(first)
	client, pool := newTestForwardingClient(t, func(ctx context.Context, _ string) (net.Conn, error) {
		listener := active.Load()
		if listener == nil {
			return nil, net.ErrClosed
		}
		return listener.DialContext(ctx)
	}, "restarting-peer")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if result, err := client.ExecuteRaftGraphRead(ctx, "restarting-peer", "test-space", []byte("before")); err != nil || string(result) != "first:before" {
		t.Fatalf("before restart: %q (%v)", result, err)
	}
	original, err := pool.conn(ctx, "restarting-peer")
	if err != nil {
		t.Fatal(err)
	}
	active.Store(nil)
	firstServer.Stop()
	if _, err := client.ExecuteRaftGraphRead(ctx, "restarting-peer", "test-space", []byte("offline")); status.Code(err) != codes.Unavailable {
		t.Fatalf("offline read returned %v", err)
	}
	second, _ := serveForwardingPeer(t, &forwardingPeer{label: "restarted"})
	active.Store(second)
	// Wait for gRPC to reconnect the existing channel; this test does not
	// add retries to either forwarding path.
	for state := original.GetState(); state != connectivity.Ready; state = original.GetState() {
		if !original.WaitForStateChange(ctx, state) {
			t.Fatalf("channel never reconnected, state=%v", original.GetState())
		}
	}
	if result, err := client.ExecuteRaftGraphRead(ctx, "restarting-peer", "test-space", []byte("after")); err != nil || string(result) != "restarted:after" {
		t.Fatalf("after restart: %q (%v)", result, err)
	}
	if current, err := pool.conn(ctx, "restarting-peer"); err != nil || current != original {
		t.Fatalf("peer restart replaced the channel: %v", err)
	}
}
