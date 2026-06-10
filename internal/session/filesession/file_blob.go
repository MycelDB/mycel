package filesession

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/blobstorage"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/graphstorage"
	sessionapi "martinbeauvais.com/mbgit/knotbase/knotdb/session/api"
	storetemplate "martinbeauvais.com/mbgit/knotbase/knotdb/store/template"
)

// System blob template identity. It is the fallback template applied by
// AddBlobNode when no TemplateID is supplied, so every blob node is typed.
const (
	SystemBlobTemplateKey     = "blob"
	SystemBlobTemplateVersion = "1.0.0"
)

// Auto-populated blob metadata property names.
const (
	PropMimeType         = "mime_type"
	PropDeclaredMimeType = "declared_mime_type"
	PropOriginalFilename = "original_filename"
	PropSizeBytes        = "size_bytes"
)

// Caller-supplied blob text property names. Blob nodes have no inline
// Content, so text about the blob lives in props.
const (
	PropCaption = "caption"
	PropAltText = "alt_text"
)

// sniffLen matches http.DetectContentType's maximum useful prefix.
const sniffLen = 512

// AddBlobNode streams binary content into the space's content-addressed blob
// store and creates a node referencing it, in a single call.
//
// The blob file is fully written and fsynced before the node transaction
// commits, so a crash in between leaves only an orphan blob (reclaimed by the
// sweep on next open), never a node pointing at a missing blob.
func (s *FileSession) AddBlobNode(ctx context.Context, in sessionapi.AddBlobNodeInput) (graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return graph.Node{}, err
	}
	if in.Reader == nil {
		return graph.Node{}, fmt.Errorf("%w: reader is required", storetemplate.ErrInvalidInput)
	}

	templateID := in.TemplateID
	if templateID == nil {
		id, err := s.ensureSystemBlobTemplate(ctx)
		if err != nil {
			return graph.Node{}, err
		}
		templateID = &id
	}

	blobs, err := s.blobStore()
	if err != nil {
		return graph.Node{}, err
	}

	// Sniff the MIME type from the leading bytes, then stream head + rest
	// into the blob store without buffering the full content.
	head := make([]byte, sniffLen)
	n, err := io.ReadFull(in.Reader, head)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return graph.Node{}, err
	}
	head = head[:n]
	mimeType := normalizeMimeType(http.DetectContentType(head))
	blobID, size, err := blobs.Put(ctx, io.MultiReader(bytes.NewReader(head), in.Reader))
	if err != nil {
		return graph.Node{}, err
	}

	nodes, err := s.readNodes()
	if err != nil {
		return graph.Node{}, err
	}
	nodeID, err := newGraphUUID()
	if err != nil {
		return graph.Node{}, err
	}
	if in.ID != nil {
		nodeID = *in.ID
	}

	props := copyProps(in.Props)
	props[PropMimeType] = mimeType
	props[PropSizeBytes] = size
	if in.DeclaredMimeType != "" {
		props[PropDeclaredMimeType] = in.DeclaredMimeType
	}
	if in.OriginalFilename != "" {
		props[PropOriginalFilename] = filepath.Base(in.OriginalFilename)
	}

	// Blob nodes never carry inline Content; text about the blob belongs in
	// props (caption, alt_text, ...) or annotation children.
	node, err := s.buildNode(ctx, nodes, nodeID, templateID, "", props)
	if err != nil {
		return graph.Node{}, err
	}
	node.BlobRef = &blobID
	now := time.Now().UTC()
	node.CreatedAt = now
	node.UpdatedAt = now
	if err := s.commitGraph(ctx, []graph.Node{node}, nil, nil, nil); err != nil {
		return graph.Node{}, err
	}
	return node, nil
}

// GetBlob opens the blob attached to a node for streaming reads.
func (s *FileSession) GetBlob(ctx context.Context, in sessionapi.GetBlobInput) (sessionapi.GetBlobResult, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return sessionapi.GetBlobResult{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return sessionapi.GetBlobResult{}, err
	}
	if err := s.ensureRead(); err != nil {
		return sessionapi.GetBlobResult{}, err
	}
	store, err := s.graphStore()
	if err != nil {
		return sessionapi.GetBlobResult{}, err
	}
	node, err := store.GetNode(ctx, in.NodeID)
	if err != nil {
		if errors.Is(err, graphstorage.ErrNotFound) {
			return sessionapi.GetBlobResult{}, s.errors.NotFound
		}
		return sessionapi.GetBlobResult{}, err
	}
	if node.BlobRef == nil {
		return sessionapi.GetBlobResult{}, fmt.Errorf("%w: node has no blob", s.errors.NotFound)
	}
	blobs, err := s.blobStore()
	if err != nil {
		return sessionapi.GetBlobResult{}, err
	}
	reader, err := blobs.Open(ctx, *node.BlobRef)
	if err != nil {
		if errors.Is(err, blobstorage.ErrNotFound) {
			return sessionapi.GetBlobResult{}, fmt.Errorf("%w: blob %s is missing", s.errors.NotFound, *node.BlobRef)
		}
		return sessionapi.GetBlobResult{}, err
	}
	size, err := blobs.Size(ctx, *node.BlobRef)
	if err != nil {
		reader.Close()
		return sessionapi.GetBlobResult{}, err
	}
	return sessionapi.GetBlobResult{
		Reader: reader,
		Meta: graph.BlobMeta{
			ID:               *node.BlobRef,
			SizeBytes:        size,
			MimeType:         stringProp(node.Props, PropMimeType),
			DeclaredMimeType: stringProp(node.Props, PropDeclaredMimeType),
			OriginalFilename: stringProp(node.Props, PropOriginalFilename),
			CreatedAt:        node.CreatedAt,
		},
	}, nil
}

func (s *FileSession) blobPath() string {
	return filepath.Join(s.blobsDir, safeID(s.spaceID))
}

// blobStore lazily opens the per-space blob store. On first open it sweeps
// stale staging files and orphan objects no live node references.
func (s *FileSession) blobStore() (*blobstorage.Store, error) {
	if s.blobs != nil {
		return s.blobs, nil
	}
	blobs, err := blobstorage.Open(s.blobPath())
	if err != nil {
		return nil, err
	}
	s.blobs = blobs
	s.sweepOrphanBlobs(context.Background())
	return s.blobs, nil
}

// sweepOrphanBlobs removes blob objects with no live referencing node (e.g.
// left behind by a crash between blob write and node commit) and stale
// staging files. Best-effort by design.
func (s *FileSession) sweepOrphanBlobs(ctx context.Context) {
	if s.blobs == nil {
		return
	}
	store, err := s.graphStore()
	if err != nil {
		return
	}
	_ = s.blobs.SweepTmp(ctx)
	ids, err := s.blobs.List(ctx)
	if err != nil {
		return
	}
	for _, id := range ids {
		count, err := store.BlobRefCount(ctx, id)
		if err != nil || count > 0 {
			continue
		}
		_ = s.blobs.Delete(ctx, id)
	}
}

// ensureSystemBlobTemplate resolves the system blob template for the space,
// importing it on first use.
func (s *FileSession) ensureSystemBlobTemplate(ctx context.Context) (graph.TemplateID, error) {
	t, err := s.templateManager.Find(ctx, s.spaceID, SystemBlobTemplateKey, SystemBlobTemplateVersion)
	if err == nil {
		return t.ID, nil
	}
	if !errors.Is(err, storetemplate.ErrTemplateNotFound) {
		return graph.TemplateID{}, err
	}
	created, err := s.templateManager.Import(ctx, s.spaceID, systemBlobTemplateDocument())
	if err != nil {
		if errors.Is(err, storetemplate.ErrDuplicateTemplateVersion) {
			// Lost a registration race; the template exists now.
			t, findErr := s.templateManager.Find(ctx, s.spaceID, SystemBlobTemplateKey, SystemBlobTemplateVersion)
			if findErr != nil {
				return graph.TemplateID{}, findErr
			}
			return t.ID, nil
		}
		return graph.TemplateID{}, err
	}
	for _, t := range created {
		if t.Key == SystemBlobTemplateKey {
			return t.ID, nil
		}
	}
	return graph.TemplateID{}, fmt.Errorf("system blob template import returned no template")
}

func systemBlobTemplateDocument() storetemplate.ImportDocument {
	return storetemplate.ImportDocument{
		SchemaVersion: 1,
		Templates: []storetemplate.TemplateImport{
			{
				Key:         SystemBlobTemplateKey,
				Version:     SystemBlobTemplateVersion,
				DisplayName: "Blob",
				Description: "Binary content stored in the space blob store (images, PDFs, audio, ...). Children act as annotations.",
				System:      true,
				Properties: storetemplate.PropertyPolicyImport{
					AllowExtra: true,
					Allowed: []storetemplate.TemplatePropertyImport{
						{Name: PropMimeType, Type: graph.PropertyTypeString, Description: "MIME type sniffed from content"},
						{Name: PropDeclaredMimeType, Type: graph.PropertyTypeString, Description: "MIME type declared by the uploader"},
						{Name: PropOriginalFilename, Type: graph.PropertyTypeString, Description: "Original filename as uploaded"},
						{Name: PropSizeBytes, Type: graph.PropertyTypeNumber, Description: "Blob size in bytes"},
						{Name: PropCaption, Type: graph.PropertyTypeString, Description: "Caption text for the blob"},
						{Name: PropAltText, Type: graph.PropertyTypeString, Description: "Alternative text for accessibility"},
					},
				},
				// Any child template is allowed so annotations can be attached
				// as a subtree under the blob node.
				Children: storetemplate.ChildPolicyImport{Allowed: true},
			},
		},
	}
}

func normalizeMimeType(detected string) string {
	if idx := strings.Index(detected, ";"); idx >= 0 {
		detected = detected[:idx]
	}
	return strings.TrimSpace(detected)
}

func stringProp(props map[string]any, key string) string {
	v, _ := props[key].(string)
	return v
}
