package backend

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc"
)

const DefaultRaftMessageSendTimeout = 500 * time.Millisecond

type RaftClientConnPool struct {
	Client Client

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	closed bool
}

func NewRaftClientConnPool(client Client) *RaftClientConnPool {
	return &RaftClientConnPool{Client: client, conns: map[string]*grpc.ClientConn{}}
}

func (p *RaftClientConnPool) Conn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	if p == nil {
		return nil, errors.New("raft client connection pool is nil")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("raft client connection pool is closed")
	}
	if conn := p.conns[addr]; conn != nil {
		return conn, nil
	}
	conn, err := p.Client.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = conn
	return conn, nil
}

func (p *RaftClientConnPool) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	var closeErr error
	for addr, conn := range p.conns {
		if err := conn.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		delete(p.conns, addr)
	}
	return closeErr
}

type RaftMessageSender struct {
	Client  Client
	Addr    string
	Pool    *RaftClientConnPool
	Timeout time.Duration
}

func (s RaftMessageSender) SendRaftMessage(ctx context.Context, env consensus.MessageEnvelope) error {
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = DefaultRaftMessageSendTimeout
	}
	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, closeConn, err := s.conn(sendCtx)
	if err != nil {
		return err
	}
	if closeConn != nil {
		defer closeConn()
	}
	_, err = clusterpb.NewClusterBackendServiceClient(conn).DeliverRaftMessages(s.Client.authContext(sendCtx), &clusterpb.DeliverRaftMessagesRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		Messages: []*clusterpb.RaftMessageEnvelope{{
			GroupId:    string(env.GroupID),
			FromNodeId: uint64(env.From),
			Message:    append([]byte(nil), env.Message...),
		}},
	})
	return err
}

func (s RaftMessageSender) conn(ctx context.Context) (*grpc.ClientConn, func(), error) {
	if s.Pool != nil {
		conn, err := s.Pool.Conn(ctx, s.Addr)
		return conn, nil, err
	}
	conn, err := s.Client.dial(ctx, s.Addr)
	if err != nil {
		return nil, nil, err
	}
	return conn, func() { _ = conn.Close() }, nil
}
