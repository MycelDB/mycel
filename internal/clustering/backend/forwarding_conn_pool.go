package backend

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ForwardingConnPool owns one lazy channel per configured peer address for
// forwarded graph reads and client RPCs. Its owner closes it at daemon shutdown.
// Raft message delivery uses a separate pool.
type ForwardingConnPool struct {
	dialer Client
	mu     sync.Mutex
	// An entry with a nil channel is an allowed address not yet used. A nil
	// map means the pool has closed and must not start any more channels.
	peers map[string]*grpc.ClientConn
}

func NewForwardingConnPool(dialer Client, peerAddrs []string) *ForwardingConnPool {
	peers := make(map[string]*grpc.ClientConn, len(peerAddrs))
	for _, addr := range peerAddrs {
		if addr = strings.TrimSpace(addr); addr != "" {
			peers[addr] = nil
		}
	}
	return &ForwardingConnPool{dialer: Client{DialOptions: slices.Clone(dialer.DialOptions)}, peers: peers}
}

func (p *ForwardingConnPool) conn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if p.peers == nil {
		return nil, status.Error(codes.Unavailable, "forwarding connection pool is closed")
	}
	addr = strings.TrimSpace(addr)
	conn, ok := p.peers[addr]
	if !ok {
		return nil, status.Error(codes.Unavailable, "forwarding destination is not a configured peer")
	}
	if conn != nil {
		return conn, nil
	}
	// With the daemon's non-blocking dial options, acquisition doesn't wait
	// for a handshake. gRPC owns transport reconnection after peer restarts.
	conn, err := p.dialer.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	p.peers[addr] = conn
	return conn, nil
}

func (p *ForwardingConnPool) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	peers := p.peers
	p.peers = nil
	p.mu.Unlock()
	var err error
	for _, conn := range peers {
		if conn != nil {
			err = errors.Join(err, conn.Close())
		}
	}
	return err
}
