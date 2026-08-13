package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/myceldb/mycel/internal/cli/app"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/spf13/cobra"
)

type domainJSONDocument struct {
	Format       string                         `json:"format"`
	Manifest     *clientv1.DomainExportManifest `json:"manifest,omitempty"`
	BlobMetadata []*clientv1.BlobImportMetadata `json:"blob_metadata,omitempty"`
	BlobChunks   []*clientv1.BlobImportChunk    `json:"blob_chunks,omitempty"`
	Nodes        []*clientv1.Node               `json:"nodes,omitempty"`
	Edges        []*clientv1.Edge               `json:"edges,omitempty"`
}

func NewExportCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "export", Short: "Export Mycel data through daemon gRPC"}
	cmd.AddCommand(NewExportDomainCommand(a))
	return cmd
}

func NewExportDomainCommand(a *app.App) *cobra.Command {
	var transactionID, filePath string
	var includeBlobs bool
	cmd := &cobra.Command{Use: "domain", Short: "Export a domain snapshot from a readable transaction", RunE: func(cmd *cobra.Command, args []string) error {
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		stream, err := clientv1.NewImportExportServiceClient(conn).ExportDomain(authCtx, &clientv1.ExportDomainRequest{TransactionId: transactionID, Format: clientv1.DomainExportFormat_DOMAIN_EXPORT_FORMAT_MYCEL_STREAM, Options: &clientv1.DomainExportOptions{IncludeBlobs: includeBlobs}})
		if err != nil {
			return err
		}
		doc := domainJSONDocument{Format: "mycel-domain-json-v1"}
		for {
			res, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if manifest := res.GetManifest(); manifest != nil {
				doc.Manifest = manifest
				continue
			}
			if record := res.GetRecord(); record != nil {
				if blobMetadata := record.GetBlobMetadata(); blobMetadata != nil {
					doc.BlobMetadata = append(doc.BlobMetadata, blobMetadata)
				}
				if blobChunk := record.GetBlobChunk(); blobChunk != nil {
					doc.BlobChunks = append(doc.BlobChunks, blobChunk)
				}
				if node := record.GetNode(); node != nil {
					doc.Nodes = append(doc.Nodes, node)
				}
				if edge := record.GetEdge(); edge != nil {
					doc.Edges = append(doc.Edges, edge)
				}
			}
		}
		raw, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			return err
		}
		raw = append(raw, '\n')
		if filePath == "" || filePath == "-" {
			_, err = os.Stdout.Write(raw)
			return err
		}
		if err := os.WriteFile(filePath, raw, 0o600); err != nil {
			return err
		}
		return a.Print(map[string]any{"file": filePath, "blobs": len(doc.BlobMetadata), "nodes": len(doc.Nodes), "edges": len(doc.Edges)}, fmt.Sprintf("domain exported: %s (%d blobs, %d nodes, %d edges)\n", filePath, len(doc.BlobMetadata), len(doc.Nodes), len(doc.Edges)))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "readable transaction ID")
	cmd.Flags().StringVarP(&filePath, "file", "f", "", "output file path (or -/empty for stdout)")
	cmd.Flags().BoolVar(&includeBlobs, "include-blobs", false, "include blob payloads referenced by blob nodes")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func NewImportCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "import", Short: "Import Mycel data through daemon gRPC"}
	cmd.AddCommand(NewImportDomainCommand(a))
	return cmd
}

func NewImportDomainCommand(a *app.App) *cobra.Command {
	var transactionID, filePath, mode string
	var preserveIDs, dryRun, includeBlobs bool
	cmd := &cobra.Command{Use: "domain", Short: "Import a Mycel domain JSON document into a read-write transaction", RunE: func(cmd *cobra.Command, args []string) error {
		raw, err := readImportFile(filePath)
		if err != nil {
			return err
		}
		var doc domainJSONDocument
		if err := json.Unmarshal(raw, &doc); err != nil {
			return fmt.Errorf("invalid domain import JSON: %w", err)
		}
		conn, authCtx, _, err := loginDaemonPrincipal(cmd.Context(), a)
		if err != nil {
			return err
		}
		defer conn.Close()
		stream, err := clientv1.NewImportExportServiceClient(conn).ImportDomain(authCtx)
		if err != nil {
			return err
		}
		metadata := &clientv1.ImportDomainMetadata{TransactionId: transactionID, Format: clientv1.DomainImportFormat_DOMAIN_IMPORT_FORMAT_MYCEL_STREAM, Mode: parseDomainImportMode(mode), Options: &clientv1.DomainImportOptions{IncludeBlobs: includeBlobs || len(doc.BlobMetadata) > 0 || len(doc.BlobChunks) > 0, PreserveIds: preserveIDs, DryRun: dryRun}}
		if err := stream.Send(&clientv1.ImportDomainRequest{Part: &clientv1.ImportDomainRequest_Metadata{Metadata: metadata}}); err != nil {
			return err
		}
		for _, blobMetadata := range doc.BlobMetadata {
			if err := stream.Send(&clientv1.ImportDomainRequest{Part: &clientv1.ImportDomainRequest_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_BlobMetadata{BlobMetadata: blobMetadata}}}}); err != nil {
				return err
			}
		}
		for _, blobChunk := range doc.BlobChunks {
			if err := stream.Send(&clientv1.ImportDomainRequest{Part: &clientv1.ImportDomainRequest_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_BlobChunk{BlobChunk: blobChunk}}}}); err != nil {
				return err
			}
		}
		for _, node := range doc.Nodes {
			if err := stream.Send(&clientv1.ImportDomainRequest{Part: &clientv1.ImportDomainRequest_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_Node{Node: node}}}}); err != nil {
				return err
			}
		}
		for _, edge := range doc.Edges {
			if err := stream.Send(&clientv1.ImportDomainRequest{Part: &clientv1.ImportDomainRequest_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_Edge{Edge: edge}}}}); err != nil {
				return err
			}
		}
		res, err := stream.CloseAndRecv()
		if err != nil {
			return err
		}
		return a.Print(res.GetSummary(), fmt.Sprintf("domain imported: %d blobs, %d nodes, %d edges\n", res.GetSummary().GetBlobsImported(), res.GetSummary().GetNodesImported()+res.GetSummary().GetNodesUpdated(), res.GetSummary().GetEdgesImported()+res.GetSummary().GetEdgesUpdated()))
	}}
	cmd.Flags().StringVar(&transactionID, "transaction-id", "", "read-write transaction ID")
	cmd.Flags().StringVarP(&filePath, "file", "f", "-", "input file path or - for stdin")
	cmd.Flags().StringVar(&mode, "mode", "append", "import mode: append, upsert, or replace-domain")
	cmd.Flags().BoolVar(&preserveIDs, "preserve-ids", true, "preserve node/edge IDs from the document")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate/count without mutating the transaction")
	cmd.Flags().BoolVar(&includeBlobs, "include-blobs", false, "apply blob payloads from the document")
	_ = cmd.MarkFlagRequired("transaction-id")
	return cmd
}

func readImportFile(filePath string) ([]byte, error) {
	if filePath == "" || filePath == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(filePath)
}

func parseDomainImportMode(mode string) clientv1.DomainImportMode {
	switch mode {
	case "upsert":
		return clientv1.DomainImportMode_DOMAIN_IMPORT_MODE_UPSERT
	case "replace-domain", "replace":
		return clientv1.DomainImportMode_DOMAIN_IMPORT_MODE_REPLACE_DOMAIN
	default:
		return clientv1.DomainImportMode_DOMAIN_IMPORT_MODE_APPEND
	}
}
