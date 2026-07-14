package clustering

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestExchangeWithPeerDiscoversIdentity(t *testing.T) {
	ctx := context.Background()
	serverDir := t.TempDir()
	clientDir := t.TempDir()
	serverNode, err := LoadOrCreate(ctx, Options{DataDir: serverDir, NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:19091"})
	if err != nil {
		t.Fatal(err)
	}
	clientNode, err := LoadOrCreate(ctx, Options{DataDir: clientDir, NodeName: "node-b", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:19092"})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	RegisterClusterService(grpcServer, &ExchangeServer{DataDir: serverDir, Identity: serverNode.Identity, State: serverNode.State})
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()
	res, err := ExchangeWithPeer(ctx, lis.Addr().String(), clientNode.Identity, clientNode.State)
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if res.Identity.NodeID != serverNode.Identity.NodeID {
		t.Fatalf("node id=%s want %s", res.Identity.NodeID, serverNode.Identity.NodeID)
	}
	store, err := ReadPeers(serverDir)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range store.Peers {
		if p.NodeID == clientNode.Identity.NodeID && p.State == PeerStateActive {
			found = true
		}
	}
	if !found {
		t.Fatalf("server did not record client peer: %#v", store.Peers)
	}
}

func TestDiscoverSeedsMarksUnreachable(t *testing.T) {
	dir := t.TempDir()
	node, err := LoadOrCreate(context.Background(), Options{DataDir: dir, BackendAdvertiseAddr: "127.0.0.1:19093"})
	if err != nil {
		t.Fatal(err)
	}
	DiscoverSeeds(context.Background(), DiscoveryOptions{DataDir: dir, Identity: node.Identity, State: node.State, Seeds: []string{"127.0.0.1:1"}, Timeout: 50 * time.Millisecond})
	store, err := ReadPeers(dir)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range store.Peers {
		if p.BackendAdvertiseAddr == "127.0.0.1:1" && p.State == PeerStateUnreachable {
			found = true
		}
	}
	if !found {
		t.Fatalf("unreachable seed not recorded: %#v", store.Peers)
	}
}
