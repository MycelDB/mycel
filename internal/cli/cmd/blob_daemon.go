package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
)

const blobUploadChunkSize = 256 * 1024

func NewUploadBlobCommand(a *app.App) *cobra.Command {
	var spaceID, declaredMimeType, originalFilename string
	cmd := &cobra.Command{Use: "upload FILE", Aliases: []string{"add"}, Short: "Upload raw blob content through daemon gRPC", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		file, err := os.Open(args[0])
		if err != nil {
			return err
		}
		defer file.Close()
		if originalFilename == "" {
			originalFilename = filepath.Base(args[0])
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		stream, err := clientv1.NewBlobServiceClient(conn).UploadBlob(authCtx)
		if err != nil {
			return err
		}
		if err := stream.Send(&clientv1.UploadBlobRequest{Part: &clientv1.UploadBlobRequest_Metadata{Metadata: &clientv1.UploadBlobMetadata{SpaceId: spaceID, DeclaredMimeType: declaredMimeType, OriginalFilename: originalFilename}}}); err != nil {
			return err
		}
		buf := make([]byte, blobUploadChunkSize)
		for {
			n, readErr := file.Read(buf)
			if n > 0 {
				chunk := append([]byte(nil), buf[:n]...)
				if err := stream.Send(&clientv1.UploadBlobRequest{Part: &clientv1.UploadBlobRequest_Chunk{Chunk: chunk}}); err != nil {
					return err
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				return readErr
			}
		}
		res, err := stream.CloseAndRecv()
		if err != nil {
			return err
		}
		return a.Print(res.GetBlob(), fmt.Sprintf("blob uploaded: %s (%d bytes, %s)\n", res.GetBlob().GetBlobId(), res.GetBlob().GetSizeBytes(), res.GetBlob().GetMimeType()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	cmd.Flags().StringVar(&declaredMimeType, "mime-type", "", "declared MIME type")
	cmd.Flags().StringVar(&originalFilename, "filename", "", "original filename metadata")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func NewGetRawBlobCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "get BLOB_ID", Aliases: []string{"metadata", "show"}, Short: "Get raw blob metadata", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewBlobServiceClient(conn).GetBlob(authCtx, &clientv1.GetBlobRequest{SpaceId: spaceID, BlobId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res.GetBlob(), fmt.Sprintf("blob: %s (%d bytes, %s)\n", res.GetBlob().GetBlobId(), res.GetBlob().GetSizeBytes(), res.GetBlob().GetMimeType()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func NewDownloadRawBlobCommand(a *app.App) *cobra.Command {
	var spaceID, outputPath string
	cmd := &cobra.Command{Use: "download BLOB_ID", Short: "Download raw blob content", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		stream, err := clientv1.NewBlobServiceClient(conn).DownloadBlob(authCtx, &clientv1.DownloadBlobRequest{SpaceId: spaceID, BlobId: args[0]})
		if err != nil {
			return err
		}
		var meta *clientv1.Blob
		var out *os.File
		defer func() {
			if out != nil {
				_ = out.Close()
			}
		}()
		for {
			res, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if blob := res.GetBlob(); blob != nil {
				meta = blob
				target := outputPath
				if target == "" {
					target = meta.GetOriginalFilename()
				}
				if target == "" {
					target = meta.GetBlobId()
				}
				out, err = os.Create(target)
				if err != nil {
					return err
				}
				outputPath = target
				continue
			}
			chunk := res.GetChunk()
			if len(chunk) > 0 {
				if out == nil {
					return fmt.Errorf("download stream sent chunk before metadata")
				}
				if _, err := out.Write(chunk); err != nil {
					return err
				}
			}
		}
		if out == nil || meta == nil {
			return fmt.Errorf("download stream returned no blob metadata")
		}
		if err := out.Close(); err != nil {
			return err
		}
		out = nil
		return a.Print(map[string]any{"blob_id": meta.GetBlobId(), "space_id": meta.GetSpaceId(), "output": outputPath, "size_bytes": meta.GetSizeBytes(), "mime_type": meta.GetMimeType()}, fmt.Sprintf("blob written: %s (%d bytes, %s)\n", outputPath, meta.GetSizeBytes(), meta.GetMimeType()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	cmd.Flags().StringVarP(&outputPath, "output-file", "o", "", "output file path")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}

func NewDeleteRawBlobCommand(a *app.App) *cobra.Command {
	var spaceID string
	cmd := &cobra.Command{Use: "delete BLOB_ID", Aliases: []string{"del", "remove", "rm"}, Short: "Delete an unreferenced raw blob", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		res, err := clientv1.NewBlobServiceClient(conn).DeleteBlob(authCtx, &clientv1.DeleteBlobRequest{SpaceId: spaceID, BlobId: args[0]})
		if err != nil {
			return err
		}
		return a.Print(res, fmt.Sprintf("blob deleted: %s\n", res.GetDeletedBlobId()))
	}}
	cmd.Flags().StringVar(&spaceID, "space-id", "", "space ID")
	_ = cmd.MarkFlagRequired("space-id")
	return cmd
}
