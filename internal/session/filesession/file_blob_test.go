package filesession

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainspace "github.com/myceldb/mycel/domain/space"
	sessionapi "github.com/myceldb/mycel/session/api"
	storetemplate "github.com/myceldb/mycel/store/template"
)

// pngHeader is a minimal payload http.DetectContentType sniffs as image/png.
var pngHeader = []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0, 0, 0, 13, 'I', 'H', 'D', 'R'}

func TestAddBlobNodeDefaultsToSystemBlobTemplate(t *testing.T) {
	ctx := context.Background()
	sess, _ := newBlobTestSession(t)

	node, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{
		Reader:           bytes.NewReader(pngHeader),
		DeclaredMimeType: "image/x-declared",
		OriginalFilename: "photo.png",
		Props:            map[string]any{PropCaption: "a caption"},
	})
	if err != nil {
		t.Fatalf("add blob node failed: %v", err)
	}
	if node.BlobRef == nil {
		t.Fatal("expected blob ref to be set")
	}
	if node.TemplateID == nil {
		t.Fatal("expected default system blob template to be applied")
	}
	if node.Content != "" {
		t.Fatalf("expected empty content on blob node, got %q", node.Content)
	}
	if got := node.Props[PropCaption]; got != "a caption" {
		t.Fatalf("unexpected caption prop: %v", got)
	}
	if got := node.Props[PropMimeType]; got != "image/png" {
		t.Fatalf("expected sniffed mime_type image/png, got %v", got)
	}
	if got := node.Props[PropDeclaredMimeType]; got != "image/x-declared" {
		t.Fatalf("unexpected declared_mime_type: %v", got)
	}
	if got := node.Props[PropOriginalFilename]; got != "photo.png" {
		t.Fatalf("unexpected original_filename: %v", got)
	}
	if got := node.Props[PropSizeBytes]; got != int64(len(pngHeader)) {
		t.Fatalf("unexpected size_bytes: %v", got)
	}

	templates, err := sess.ListTemplates(ctx)
	if err != nil {
		t.Fatalf("list templates failed: %v", err)
	}
	found := false
	for _, tmpl := range templates {
		if tmpl.Key == SystemBlobTemplateKey && tmpl.System && tmpl.Children.Allowed {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected system blob template registered, got %+v", templates)
	}
}

func TestGetBlobRoundTrip(t *testing.T) {
	ctx := context.Background()
	sess, _ := newBlobTestSession(t)
	node, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader), OriginalFilename: "img.png"})
	if err != nil {
		t.Fatalf("add blob node failed: %v", err)
	}

	res, err := sess.GetBlob(ctx, sessionapi.GetBlobInput{NodeID: node.ID})
	if err != nil {
		t.Fatalf("get blob failed: %v", err)
	}
	defer res.Reader.Close()
	got, err := io.ReadAll(res.Reader)
	if err != nil {
		t.Fatalf("read blob failed: %v", err)
	}
	if !bytes.Equal(got, pngHeader) {
		t.Fatalf("blob content mismatch")
	}
	if res.Meta.ID != *node.BlobRef || res.Meta.SizeBytes != int64(len(pngHeader)) || res.Meta.MimeType != "image/png" || res.Meta.OriginalFilename != "img.png" {
		t.Fatalf("unexpected blob meta: %+v", res.Meta)
	}
}

func TestGetBlobOnNonBlobNodeFails(t *testing.T) {
	ctx := context.Background()
	sess, _ := newBlobTestSession(t)
	node, err := sess.AddNode(ctx, sessionapi.AddNodeInput{Content: "text only"})
	if err != nil {
		t.Fatalf("add node failed: %v", err)
	}
	if _, err := sess.GetBlob(ctx, sessionapi.GetBlobInput{NodeID: node.ID}); err == nil {
		t.Fatal("expected error for node without blob")
	}
	if _, err := sess.GetBlob(ctx, sessionapi.GetBlobInput{NodeID: graph.NodeID(uuid.New())}); err == nil {
		t.Fatal("expected error for missing node")
	}
}

func TestAddBlobNodeDeduplicatesContent(t *testing.T) {
	ctx := context.Background()
	sess, fs := newBlobTestSession(t)
	first, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)})
	if err != nil {
		t.Fatalf("first add failed: %v", err)
	}
	second, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)})
	if err != nil {
		t.Fatalf("second add failed: %v", err)
	}
	if *first.BlobRef != *second.BlobRef {
		t.Fatalf("expected shared blob ref, got %s and %s", *first.BlobRef, *second.BlobRef)
	}
	ids, err := fs.blobs.List(ctx)
	if err != nil {
		t.Fatalf("list blobs failed: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected one stored object, got %d", len(ids))
	}
}

func TestDeleteNodeReleasesBlobOnlyWhenUnreferenced(t *testing.T) {
	ctx := context.Background()
	sess, fs := newBlobTestSession(t)
	first, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)})
	if err != nil {
		t.Fatalf("first add failed: %v", err)
	}
	second, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)})
	if err != nil {
		t.Fatalf("second add failed: %v", err)
	}
	blobID := *first.BlobRef

	if err := sess.DeleteNode(ctx, sessionapi.DeleteNodeInput{ID: first.ID}); err != nil {
		t.Fatalf("delete first node failed: %v", err)
	}
	exists, err := fs.blobs.Exists(ctx, blobID)
	if err != nil || !exists {
		t.Fatalf("expected blob to survive while still referenced, exists=%v err=%v", exists, err)
	}

	if err := sess.DeleteNode(ctx, sessionapi.DeleteNodeInput{ID: second.ID}); err != nil {
		t.Fatalf("delete second node failed: %v", err)
	}
	exists, err = fs.blobs.Exists(ctx, blobID)
	if err != nil || exists {
		t.Fatalf("expected blob removed with last reference, exists=%v err=%v", exists, err)
	}
}

func TestRecursiveDeleteReleasesBlobsOfSubtree(t *testing.T) {
	ctx := context.Background()
	sess, fs := newBlobTestSession(t)
	parent, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)})
	if err != nil {
		t.Fatalf("add parent blob node failed: %v", err)
	}
	annotation, err := sess.AddNode(ctx, sessionapi.AddNodeInput{Content: "annotation"})
	if err != nil {
		t.Fatalf("add annotation failed: %v", err)
	}
	if _, err := sess.AddEdge(ctx, sessionapi.AddEdgeInput{FromID: parent.ID, ToID: annotation.ID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": 0}}); err != nil {
		t.Fatalf("attach annotation failed: %v", err)
	}

	// Non-recursive delete must fail because the blob node has children.
	if err := sess.DeleteNode(ctx, sessionapi.DeleteNodeInput{ID: parent.ID}); err == nil {
		t.Fatal("expected conflict deleting blob node with annotation child")
	}
	if err := sess.DeleteNode(ctx, sessionapi.DeleteNodeInput{ID: parent.ID, Recursive: true}); err != nil {
		t.Fatalf("recursive delete failed: %v", err)
	}
	exists, err := fs.blobs.Exists(ctx, *parent.BlobRef)
	if err != nil || exists {
		t.Fatalf("expected blob removed after recursive delete, exists=%v err=%v", exists, err)
	}
}

func TestUpdateNodePreservesBlobRefAndRejectsInlineContent(t *testing.T) {
	ctx := context.Background()
	sess, _ := newBlobTestSession(t)
	node, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)})
	if err != nil {
		t.Fatalf("add blob node failed: %v", err)
	}

	// Blob nodes can never gain inline content.
	if _, err := sess.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: node.ID, TemplateID: node.TemplateID, Content: "inline text", Props: node.Props}); !errors.Is(err, storetemplate.ErrInvalidInput) {
		t.Fatalf("expected invalid input updating blob node with content, got %v", err)
	}

	// Prop-only updates (e.g. caption) are fine and preserve the blob ref.
	props := copyProps(node.Props)
	props[PropCaption] = "new caption"
	updated, err := sess.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: node.ID, TemplateID: node.TemplateID, Props: props})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if updated.BlobRef == nil || *updated.BlobRef != *node.BlobRef {
		t.Fatalf("expected blob ref preserved across update, got %v", updated.BlobRef)
	}
	if updated.Content != "" {
		t.Fatalf("expected content to stay empty, got %q", updated.Content)
	}
	if got := updated.Props[PropCaption]; got != "new caption" {
		t.Fatalf("unexpected caption prop: %v", got)
	}
}

func TestOrphanBlobSweepOnOpen(t *testing.T) {
	ctx := context.Background()
	sess, fs := newBlobTestSession(t)
	node, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)})
	if err != nil {
		t.Fatalf("add blob node failed: %v", err)
	}
	// Simulate a crash between blob write and node commit: a blob exists with
	// no referencing node.
	orphanID, _, err := fs.blobs.Put(ctx, bytes.NewReader([]byte("orphaned bytes")))
	if err != nil {
		t.Fatalf("put orphan failed: %v", err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// A fresh session sweeps the orphan on blob store open but keeps the
	// referenced blob.
	fresh := newSessionLike(t, fs)
	defer fresh.Close()
	res, err := fresh.GetBlob(ctx, sessionapi.GetBlobInput{NodeID: node.ID})
	if err != nil {
		t.Fatalf("get blob after reopen failed: %v", err)
	}
	res.Reader.Close()
	freshFS := fresh.(*FileSession)
	exists, err := freshFS.blobs.Exists(ctx, orphanID)
	if err != nil || exists {
		t.Fatalf("expected orphan swept, exists=%v err=%v", exists, err)
	}
	exists, err = freshFS.blobs.Exists(ctx, *node.BlobRef)
	if err != nil || !exists {
		t.Fatalf("expected referenced blob kept, exists=%v err=%v", exists, err)
	}
}

func TestAddBlobNodeRequiresReaderAndWriteAccess(t *testing.T) {
	ctx := context.Background()
	sess, fs := newBlobTestSession(t)
	if _, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{}); err == nil {
		t.Fatal("expected error without reader")
	}
	readOnly := New(fs.graphsDir, fs.blobsDir, fs.spaceID, fs.templateManager, sessionapi.Permissions{Read: true}, fs.errors)
	defer readOnly.Close()
	if _, err := readOnly.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)}); !errors.Is(err, fs.errors.Unauthorized) {
		t.Fatalf("expected unauthorized, got %v", err)
	}
}

func TestAddBlobNodeRejectsPDFOverConfiguredLimit(t *testing.T) {
	ctx := context.Background()
	pdf := append([]byte("%PDF-1.7\n"), bytes.Repeat([]byte("x"), 32)...)
	sess, fs := newBlobTestSessionWithConfig(t, Config{BlobLimits: sessionapi.BlobLimits{
		MaxSizeBytes:  -1,
		MaxImageBytes: -1,
		MaxPDFBytes:   8,
		MaxAudioBytes: -1,
		MaxVideoBytes: -1,
		MaxOtherBytes: -1,
	}})

	if _, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pdf)}); !errors.Is(err, sessionapi.ErrBlobTooLarge) {
		t.Fatalf("expected ErrBlobTooLarge, got %v", err)
	}
	nodes, err := fs.readNodes()
	if err != nil {
		t.Fatalf("read nodes failed: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected no committed blob node, got %d nodes", len(nodes))
	}
	if fs.blobs != nil {
		ids, err := fs.blobs.List(ctx)
		if err != nil {
			t.Fatalf("list blobs failed: %v", err)
		}
		if len(ids) != 0 {
			t.Fatalf("expected no reachable blob objects, got %d", len(ids))
		}
	}
}

func TestAddBlobNodeUsesMimeOverrideAndGlobalCap(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		limits sessionapi.BlobLimits
	}{
		{
			name: "exact MIME override",
			limits: sessionapi.BlobLimits{
				MaxSizeBytes:   -1,
				MaxImageBytes:  1024,
				MaxPDFBytes:    -1,
				MaxAudioBytes:  -1,
				MaxVideoBytes:  -1,
				MaxOtherBytes:  -1,
				MimeTypeLimits: map[string]int64{"image/png": int64(len(pngHeader) - 1)},
			},
		},
		{
			name: "global cap",
			limits: sessionapi.BlobLimits{
				MaxSizeBytes:   int64(len(pngHeader) - 1),
				MaxImageBytes:  1024,
				MaxPDFBytes:    -1,
				MaxAudioBytes:  -1,
				MaxVideoBytes:  -1,
				MaxOtherBytes:  -1,
				MimeTypeLimits: map[string]int64{"image/png": 1024},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess, _ := newBlobTestSessionWithConfig(t, Config{BlobLimits: tc.limits})
			if _, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)}); !errors.Is(err, sessionapi.ErrBlobTooLarge) {
				t.Fatalf("expected ErrBlobTooLarge, got %v", err)
			}
		})
	}
}

func TestAddBlobNodeRejectsDisallowedMimeType(t *testing.T) {
	ctx := context.Background()
	sess, _ := newBlobTestSessionWithConfig(t, Config{BlobLimits: sessionapi.BlobLimits{
		MaxSizeBytes:   -1,
		MaxImageBytes:  -1,
		MaxPDFBytes:    -1,
		MaxAudioBytes:  -1,
		MaxVideoBytes:  -1,
		MaxOtherBytes:  -1,
		MimeTypeLimits: map[string]int64{"image/png": 0},
	}})
	if _, err := sess.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)}); !errors.Is(err, sessionapi.ErrBlobTypeDisallowed) {
		t.Fatalf("expected ErrBlobTypeDisallowed, got %v", err)
	}
}

func newBlobTestSession(t *testing.T) (sessionapi.Session, *FileSession) {
	return newBlobTestSessionWithConfig(t, Config{})
}

func newBlobTestSessionWithConfig(t *testing.T, cfg Config) (sessionapi.Session, *FileSession) {
	t.Helper()
	spaceID := domainspace.SpaceID(uuid.New())
	graphsDir := t.TempDir()
	blobsDir := t.TempDir()
	prepareSpaceDir(t, graphsDir, spaceID)
	manager := storetemplate.NewManager()
	if err := manager.Init(context.Background(), t.TempDir()); err != nil {
		t.Fatalf("init template manager failed: %v", err)
	}
	sess := NewConfig(graphsDir, blobsDir, spaceID, manager, sessionapi.Permissions{Read: true, Write: true, Admin: true}, sessionapi.Errors{Closed: errors.New("closed"), NotFound: errors.New("not found"), Unauthorized: errors.New("unauthorized"), Conflict: errors.New("conflict")}, cfg)
	return sess, sess.(*FileSession)
}

// newSessionLike opens a new session over the same directories and template
// manager as an existing session.
func newSessionLike(t *testing.T, fs *FileSession) sessionapi.Session {
	t.Helper()
	return New(fs.graphsDir, fs.blobsDir, fs.spaceID, fs.templateManager, fs.permissions, fs.errors)
}

func TestTransactionAddBlobNodeCommit(t *testing.T) {
	ctx := context.Background()
	sess, fs := newBlobTestSession(t)
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	node, err := tx.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader), OriginalFilename: "img.png"})
	if err != nil {
		t.Fatalf("tx add blob failed: %v", err)
	}
	if node.BlobRef == nil {
		t.Fatalf("expected blob ref")
	}
	exists, err := fs.blobs.Exists(ctx, *node.BlobRef)
	if err != nil {
		t.Fatalf("exists before commit failed: %v", err)
	}
	if exists {
		t.Fatalf("new staged blob should not be visible before commit")
	}
	if _, err := tx.GetNode(ctx, node.ID); err != nil {
		t.Fatalf("tx should see staged blob node: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	exists, err = fs.blobs.Exists(ctx, *node.BlobRef)
	if err != nil || !exists {
		t.Fatalf("expected blob visible after commit exists=%v err=%v", exists, err)
	}
	res, err := sess.GetBlob(ctx, sessionapi.GetBlobInput{NodeID: node.ID})
	if err != nil {
		t.Fatalf("get blob failed: %v", err)
	}
	defer res.Reader.Close()
	data, _ := io.ReadAll(res.Reader)
	if !bytes.Equal(data, pngHeader) {
		t.Fatalf("blob data mismatch: %v", data)
	}
}

func TestTransactionAddBlobNodeRollbackDiscardsStagedBlob(t *testing.T) {
	ctx := context.Background()
	sess, fs := newBlobTestSession(t)
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	node, err := tx.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader(pngHeader)})
	if err != nil {
		t.Fatalf("tx add blob failed: %v", err)
	}
	if node.BlobRef == nil {
		t.Fatalf("expected blob ref")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if _, err := sess.GetNode(ctx, node.ID); err == nil {
		t.Fatalf("rollback should not persist blob node")
	}
	exists, err := fs.blobs.Exists(ctx, *node.BlobRef)
	if err != nil {
		t.Fatalf("exists failed: %v", err)
	}
	if exists {
		t.Fatalf("rollback should discard staged blob object")
	}
}

func TestTransactionAddBlobNodeConflictCleansPromotedBlob(t *testing.T) {
	ctx := context.Background()
	sess, fs := newBlobTestSession(t)
	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	node, err := tx.AddBlobNode(ctx, sessionapi.AddBlobNodeInput{Reader: bytes.NewReader([]byte("unique-conflict-blob"))})
	if err != nil {
		t.Fatalf("tx add blob failed: %v", err)
	}
	if _, err := sess.AddNode(ctx, sessionapi.AddNodeInput{Content: "advance revision", Props: map[string]any{}}); err != nil {
		t.Fatalf("advance revision failed: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatalf("expected conflict")
	}
	if node.BlobRef == nil {
		t.Fatalf("expected blob ref")
	}
	exists, err := fs.blobs.Exists(ctx, *node.BlobRef)
	if err != nil {
		t.Fatalf("exists failed: %v", err)
	}
	if exists {
		t.Fatalf("conflicted commit should clean promoted unreferenced blob")
	}
}
