package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	daemonblob "github.com/myceldb/mycel/internal/blob/service"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	storetemplate "github.com/myceldb/mycel/internal/graph/template/storage"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ImportExportService struct {
	clientv1.UnimplementedImportExportServiceServer
	sessions daemonsession.Manager
	graphs   daegraph.Manager
	blobs    daemonblob.Manager
	spaces   daemonspace.Manager
}

func NewImportExportService(sessions daemonsession.Manager, graphs daegraph.Manager, blobs daemonblob.Manager, spaces daemonspace.Manager) *ImportExportService {
	return &ImportExportService{sessions: sessions, graphs: graphs, blobs: blobs, spaces: spaces}
}

func (s *ImportExportService) ExportDomain(req *clientv1.ExportDomainRequest, stream clientv1.ImportExportService_ExportDomainServer) error {
	ctx := stream.Context()
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.UserID, req.GetTransactionId())
	if err != nil {
		return mapSessionError(err, "export domain")
	}
	if tx.State != daemonsession.TransactionStateActive {
		return status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	format := req.GetFormat()
	if format == clientv1.DomainExportFormat_DOMAIN_EXPORT_FORMAT_UNSPECIFIED {
		format = clientv1.DomainExportFormat_DOMAIN_EXPORT_FORMAT_MYCEL_STREAM
	}
	if format != clientv1.DomainExportFormat_DOMAIN_EXPORT_FORMAT_MYCEL_STREAM {
		return status.Error(codes.Unimplemented, "only MYCEL_STREAM export is currently implemented")
	}
	if opts := req.GetOptions(); opts != nil && opts.GetIncludeSemanticIndexes() {
		return status.Error(codes.Unimplemented, "semantic index export is not supported by the client API")
	}
	manifest := &clientv1.DomainExportManifest{Format: format, SpaceId: tx.SpaceID, DomainId: tx.DomainID, BaseRevision: tx.BaseRevision, ExportTime: timestamppb.Now(), Options: req.GetOptions()}
	if err := stream.Send(&clientv1.ExportDomainResponse{Part: &clientv1.ExportDomainResponse_Manifest{Manifest: manifest}}); err != nil {
		return err
	}
	if req.GetOptions().GetIncludeTemplates() {
		templates, err := s.spaces.ListVisibleTemplates(ctx, principal.UserID, tx.SpaceID, true, true)
		if err != nil {
			return mapTemplateError(err, "export templates")
		}
		for _, template := range templates {
			if err := stream.Send(&clientv1.ExportDomainResponse{Part: &clientv1.ExportDomainResponse_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_Template{Template: MapTemplateDefinition(template)}}}}); err != nil {
				return err
			}
		}
	}
	nodes, err := allExportNodes(ctx, s.graphs, tx)
	if err != nil {
		return mapGraphError(err, "export nodes")
	}
	if req.GetOptions().GetIncludeBlobs() {
		if err := s.exportBlobs(ctx, tx.SpaceID, nodes, stream); err != nil {
			return err
		}
	}
	for _, node := range nodes {
		if err := stream.Send(&clientv1.ExportDomainResponse{Part: &clientv1.ExportDomainResponse_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_Node{Node: mapProtoNode(node)}}}}); err != nil {
			return err
		}
	}
	edges, err := allExportEdges(ctx, s.graphs, tx)
	if err != nil {
		return mapGraphError(err, "export edges")
	}
	for _, edge := range edges {
		if err := stream.Send(&clientv1.ExportDomainResponse{Part: &clientv1.ExportDomainResponse_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_Edge{Edge: mapProtoEdge(edge)}}}}); err != nil {
			return err
		}
	}
	return nil
}

func (s *ImportExportService) ImportDomain(stream clientv1.ImportExportService_ImportDomainServer) error {
	ctx := stream.Context()
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return err
	}
	var metadata *clientv1.ImportDomainMetadata
	var tx *daemonsession.GraphTransaction
	state := newImportState()
	summary := &clientv1.ImportSummary{}
	for {
		req, err := stream.Recv()
		if errors.Is(err, context.Canceled) {
			return err
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if m := req.GetMetadata(); m != nil {
			if metadata != nil {
				return status.Error(codes.InvalidArgument, "import metadata must be sent once")
			}
			metadata = m
			summary.DryRun = m.GetOptions().GetDryRun()
			continue
		}
		if metadata == nil {
			return status.Error(codes.InvalidArgument, "import metadata must be sent before records")
		}
		record := req.GetRecord()
		if record == nil {
			if len(req.GetChunk()) > 0 {
				return status.Error(codes.Unimplemented, "chunk JSON/NDJSON import is not currently implemented")
			}
			continue
		}
		if tx == nil {
			resolved, err := s.importTransaction(ctx, principal.UserID, metadata)
			if err != nil {
				return err
			}
			tx = &resolved
			if metadata.GetMode() == clientv1.DomainImportMode_DOMAIN_IMPORT_MODE_REPLACE_DOMAIN && !metadata.GetOptions().GetDryRun() {
				if err := s.replaceDomain(ctx, *tx, summary); err != nil {
					return err
				}
			}
		}
		if err := s.importRecord(ctx, *tx, metadata, record, state, summary); err != nil {
			return err
		}
	}
	if metadata == nil {
		return status.Error(codes.InvalidArgument, "import metadata is required")
	}
	return stream.SendAndClose(&clientv1.ImportDomainResponse{Summary: summary})
}

type importState struct {
	blobs map[string]*pendingImportBlob
}

type pendingImportBlob struct {
	metadata *clientv1.BlobImportMetadata
	buf      bytes.Buffer
	uploaded string
}

func newImportState() *importState { return &importState{blobs: map[string]*pendingImportBlob{}} }

func (s *ImportExportService) importTransaction(ctx context.Context, userID string, metadata *clientv1.ImportDomainMetadata) (daemonsession.GraphTransaction, error) {
	format := metadata.GetFormat()
	if format == clientv1.DomainImportFormat_DOMAIN_IMPORT_FORMAT_UNSPECIFIED {
		format = clientv1.DomainImportFormat_DOMAIN_IMPORT_FORMAT_MYCEL_STREAM
	}
	if format != clientv1.DomainImportFormat_DOMAIN_IMPORT_FORMAT_MYCEL_STREAM {
		return daemonsession.GraphTransaction{}, status.Error(codes.Unimplemented, "only MYCEL_STREAM import is currently implemented")
	}
	mode := metadata.GetMode()
	if mode == clientv1.DomainImportMode_DOMAIN_IMPORT_MODE_UNSPECIFIED {
		mode = clientv1.DomainImportMode_DOMAIN_IMPORT_MODE_APPEND
	}
	if mode == clientv1.DomainImportMode_DOMAIN_IMPORT_MODE_REPLACE_DOMAIN {
		// Handled once after transaction validation in ImportDomain.
	}
	tx, err := s.sessions.GetTransaction(ctx, userID, metadata.GetTransactionId())
	if err != nil {
		return daemonsession.GraphTransaction{}, mapSessionError(err, "import domain")
	}
	if tx.State != daemonsession.TransactionStateActive {
		return daemonsession.GraphTransaction{}, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	if tx.Mode != daemonsession.TransactionModeReadWrite {
		return daemonsession.GraphTransaction{}, status.Error(codes.FailedPrecondition, "import requires a read-write transaction")
	}
	return tx, nil
}

func (s *ImportExportService) importRecord(ctx context.Context, tx daemonsession.GraphTransaction, metadata *clientv1.ImportDomainMetadata, record *clientv1.ImportExportRecord, state *importState, summary *clientv1.ImportSummary) error {
	if metadata.GetOptions().GetDryRun() {
		switch record.GetRecord().(type) {
		case *clientv1.ImportExportRecord_Node:
			summary.NodesImported++
		case *clientv1.ImportExportRecord_Edge:
			summary.EdgesImported++
		case *clientv1.ImportExportRecord_Template:
			summary.TemplatesImported++
		case *clientv1.ImportExportRecord_BlobMetadata:
			summary.BlobsImported++
		case *clientv1.ImportExportRecord_BlobChunk:
			// chunks are counted by their metadata record
		}
		return nil
	}
	switch value := record.GetRecord().(type) {
	case *clientv1.ImportExportRecord_Node:
		input := nodeInputFromExportNode(value.Node, metadata.GetOptions().GetPreserveIds())
		if input.BlobID != "" {
			if uploaded := state.uploadedBlobID(input.BlobID); uploaded != "" {
				input.BlobID = uploaded
			}
		}
		if metadata.GetMode() == clientv1.DomainImportMode_DOMAIN_IMPORT_MODE_UPSERT {
			if _, err := s.graphs.UpsertNode(ctx, tx, input); err != nil {
				return mapGraphError(err, "import node")
			}
			summary.NodesUpdated++
		} else {
			if _, err := s.graphs.CreateNode(ctx, tx, input); err != nil {
				return mapGraphError(err, "import node")
			}
			summary.NodesImported++
		}
		return nil
	case *clientv1.ImportExportRecord_Edge:
		input := edgeInputFromExportEdge(value.Edge, metadata.GetOptions().GetPreserveIds())
		if metadata.GetMode() == clientv1.DomainImportMode_DOMAIN_IMPORT_MODE_UPSERT && input.EdgeID != "" {
			if _, err := s.graphs.GetEdge(ctx, tx, input.EdgeID); err == nil {
				kind := input.Kind
				if _, err := s.graphs.UpdateEdge(ctx, tx, daegraph.UpdateEdgeInput{EdgeID: input.EdgeID, Kind: &kind, Props: input.Props}); err != nil {
					return mapGraphError(err, "import edge")
				}
				summary.EdgesUpdated++
				return nil
			}
		}
		if _, err := s.graphs.CreateEdge(ctx, tx, input); err != nil {
			return mapGraphError(err, "import edge")
		}
		summary.EdgesImported++
		return nil
	case *clientv1.ImportExportRecord_Template:
		if !metadata.GetOptions().GetIncludeTemplates() {
			summary.Warnings = append(summary.Warnings, "template record skipped because include_templates=false")
			return nil
		}
		input, err := TemplateImportFromProto(value.Template)
		if err != nil {
			return err
		}
		if _, err := s.spaces.ImportTemplates(ctx, tx.UserID, tx.SpaceID, []storetemplate.TemplateImport{input}); err != nil {
			return mapTemplateError(err, "import template")
		}
		summary.TemplatesImported++
		return nil
	case *clientv1.ImportExportRecord_BlobMetadata:
		if !metadata.GetOptions().GetIncludeBlobs() {
			summary.Warnings = append(summary.Warnings, "blob metadata skipped because include_blobs=false")
			return nil
		}
		pending := &pendingImportBlob{metadata: value.BlobMetadata}
		state.blobs[value.BlobMetadata.GetImportBlobId()] = pending
		if value.BlobMetadata.GetSizeBytes() == 0 {
			meta, err := s.blobs.UploadBlob(ctx, daemonblob.UploadInput{SpaceID: tx.SpaceID, DeclaredMimeType: value.BlobMetadata.GetDeclaredMimeType(), OriginalFilename: value.BlobMetadata.GetOriginalFilename(), Reader: bytes.NewReader(nil)})
			if err != nil {
				return mapBlobError(err, "import blob")
			}
			pending.uploaded = meta.BlobID
			summary.BlobsImported++
		}
		return nil
	case *clientv1.ImportExportRecord_BlobChunk:
		if !metadata.GetOptions().GetIncludeBlobs() {
			return nil
		}
		pending := state.blobs[value.BlobChunk.GetImportBlobId()]
		if pending == nil {
			return status.Errorf(codes.InvalidArgument, "blob chunk references unknown import_blob_id %q", value.BlobChunk.GetImportBlobId())
		}
		if _, err := pending.buf.Write(value.BlobChunk.GetChunk()); err != nil {
			return err
		}
		if pending.metadata.GetSizeBytes() > 0 && int64(pending.buf.Len()) >= pending.metadata.GetSizeBytes() && pending.uploaded == "" {
			meta, err := s.blobs.UploadBlob(ctx, daemonblob.UploadInput{SpaceID: tx.SpaceID, DeclaredMimeType: pending.metadata.GetDeclaredMimeType(), OriginalFilename: pending.metadata.GetOriginalFilename(), Reader: bytes.NewReader(pending.buf.Bytes())})
			if err != nil {
				return mapBlobError(err, "import blob")
			}
			pending.uploaded = meta.BlobID
			summary.BlobsImported++
		}
		return nil
	default:
		return status.Error(codes.InvalidArgument, "import record is required")
	}
}

func (s *ImportExportService) exportBlobs(ctx context.Context, spaceID string, nodes []domaingraph.Node, stream clientv1.ImportExportService_ExportDomainServer) error {
	seen := map[string]bool{}
	for _, node := range nodes {
		if node.BlobRef == nil || strings.TrimSpace(string(*node.BlobRef)) == "" {
			continue
		}
		blobID := string(*node.BlobRef)
		if seen[blobID] {
			continue
		}
		seen[blobID] = true
		meta, reader, err := s.blobs.OpenBlob(ctx, spaceID, blobID)
		if err != nil {
			return mapBlobError(err, "export blob")
		}
		metadata := &clientv1.BlobImportMetadata{ImportBlobId: blobID, DeclaredMimeType: meta.DeclaredMimeType, OriginalFilename: meta.OriginalFilename, Digest: meta.Digest, SizeBytes: meta.SizeBytes}
		if err := stream.Send(&clientv1.ExportDomainResponse{Part: &clientv1.ExportDomainResponse_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_BlobMetadata{BlobMetadata: metadata}}}}); err != nil {
			_ = reader.Close()
			return err
		}
		buf := make([]byte, 64*1024)
		for {
			n, readErr := reader.Read(buf)
			if n > 0 {
				chunk := append([]byte(nil), buf[:n]...)
				if err := stream.Send(&clientv1.ExportDomainResponse{Part: &clientv1.ExportDomainResponse_Record{Record: &clientv1.ImportExportRecord{Record: &clientv1.ImportExportRecord_BlobChunk{BlobChunk: &clientv1.BlobImportChunk{ImportBlobId: blobID, Chunk: chunk}}}}}); err != nil {
					_ = reader.Close()
					return err
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				_ = reader.Close()
				return readErr
			}
		}
		if err := reader.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *ImportExportService) replaceDomain(ctx context.Context, tx daemonsession.GraphTransaction, summary *clientv1.ImportSummary) error {
	edges, err := allExportEdges(ctx, s.graphs, tx)
	if err != nil {
		return mapGraphError(err, "replace domain list edges")
	}
	for _, edge := range edges {
		if _, err := s.graphs.DeleteEdge(ctx, tx, edge.ID.String()); err != nil {
			return mapGraphError(err, "replace domain delete edge")
		}
	}
	nodes, err := allExportNodes(ctx, s.graphs, tx)
	if err != nil {
		return mapGraphError(err, "replace domain list nodes")
	}
	for _, node := range nodes {
		if _, _, err := s.graphs.DeleteNode(ctx, tx, node.ID.String(), true); err != nil {
			return mapGraphError(err, "replace domain delete node")
		}
	}
	if len(nodes) > 0 || len(edges) > 0 {
		summary.Warnings = append(summary.Warnings, "target domain contents were cleared before import")
	}
	return nil
}

func (s *importState) uploadedBlobID(importBlobID string) string {
	pending := s.blobs[importBlobID]
	if pending == nil {
		return ""
	}
	return pending.uploaded
}

func nodeInputFromExportNode(node *clientv1.Node, preserveIDs bool) daegraph.NodeInput {
	payload := structMap(node.GetPayload())
	blobID, _ := payload["blob_id"].(string)
	content, _ := payload["text"].(string)
	input := daegraph.NodeInput{TemplateID: node.GetTemplateId(), Labels: node.GetLabels(), Properties: structMap(node.GetProperties()), Payload: payload, Meta: structMap(node.GetMeta()), BlobID: blobID, Content: content}
	if preserveIDs {
		input.NodeID = node.GetNodeId()
	}
	return input
}

func edgeInputFromExportEdge(edge *clientv1.Edge, preserveIDs bool) daegraph.EdgeInput {
	input := daegraph.EdgeInput{FromNodeID: edge.GetFromNodeId(), ToNodeID: edge.GetToNodeId(), Kind: edge.GetKind(), Props: structMap(edge.GetProps())}
	if preserveIDs {
		input.EdgeID = edge.GetEdgeId()
	}
	return input
}

func allExportNodes(ctx context.Context, graphs daegraph.Manager, tx daemonsession.GraphTransaction) ([]domaingraph.Node, error) {
	all := []domaingraph.Node{}
	token := ""
	for {
		nodes, next, err := graphs.ListNodes(ctx, tx, queryMaxPageSize, token)
		if err != nil {
			return nil, err
		}
		all = append(all, nodes...)
		if next == "" {
			return all, nil
		}
		token = next
	}
}

func allExportEdges(ctx context.Context, graphs daegraph.Manager, tx daemonsession.GraphTransaction) ([]domaingraph.Edge, error) {
	all := []domaingraph.Edge{}
	token := ""
	for {
		edges, next, err := graphs.ListEdges(ctx, tx, queryMaxPageSize, token)
		if err != nil {
			return nil, err
		}
		all = append(all, edges...)
		if next == "" {
			return all, nil
		}
		token = next
	}
}
