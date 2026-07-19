package backend

import (
	"context"
	"io"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

func (c Client) GetBlobPayload(ctx context.Context, addr string, req *clusterpb.GetBlobPayloadRequest, handle func(io.Reader) error) error {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := clusterpb.NewClusterBackendServiceClient(conn).GetBlobPayload(c.authContext(ctx), req)
	if err != nil {
		return err
	}
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				errCh <- nil
				return
			}
			if err != nil {
				_ = pw.CloseWithError(err)
				errCh <- err
				return
			}
			if _, err := pw.Write(chunk.GetData()); err != nil {
				errCh <- err
				return
			}
		}
	}()
	handleErr := handle(pr)
	_ = pr.Close()
	streamErr := <-errCh
	if handleErr != nil {
		return handleErr
	}
	return streamErr
}
