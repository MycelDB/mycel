package backend

import (
	"context"
	"io"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

func (c Client) StreamWal(ctx context.Context, addr string, req *clusterpb.StreamWalRequest, handle func(*clusterpb.WalRecord) error) error {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := clusterpb.NewClusterBackendServiceClient(conn).StreamWal(c.authContext(ctx), req)
	if err != nil {
		return err
	}
	for {
		rec, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := handle(rec); err != nil {
			return err
		}
	}
}
