package filesession

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	"github.com/myceldb/mycel/internal/blobstorage"
	storeembedding "github.com/myceldb/mycel/internal/embedding/store"
	"github.com/myceldb/mycel/internal/graphstorage"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
	"github.com/myceldb/mycel/query"
	storeaccounting "github.com/myceldb/mycel/store/accounting"
	storesemantic "github.com/myceldb/mycel/store/semantic"
	storetemplate "github.com/myceldb/mycel/store/template"
)

// Config carries runtime knobs for the file-backed session implementation.
type Config struct {
	BlobLimits                sessionapi.BlobLimits
	BlobStaleTmpAge           time.Duration
	CurrentUserID             identity.UserID
	EmbeddingManager          storeembedding.Manager
	SemanticManager           storesemantic.GlobalManager
	AccountingManager         storeaccounting.Manager
	UserStoreEncryptionKeyB64 string
	DomainID                  graph.DomainID
	AdvancedSemanticEnabled   bool
}

// New opens the default file-backed session implementation.
func New(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, templateManager storetemplate.Manager, permissions sessionapi.Permissions, errs sessionapi.Errors) sessionapi.Session {
	return &FileSession{graphsDir: graphsDir, blobsDir: blobsDir, spaceID: spaceID, templateManager: templateManager, permissions: permissions, errors: errs, closeStore: true}
}

// NewWithStore opens a session that borrows an engine-owned graph store.
func NewWithStore(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, templateManager storetemplate.Manager, permissions sessionapi.Permissions, errs sessionapi.Errors, store *graphstorage.LocalStore) sessionapi.Session {
	return &FileSession{graphsDir: graphsDir, blobsDir: blobsDir, spaceID: spaceID, templateManager: templateManager, permissions: permissions, errors: errs, store: store}
}

// NewWithStoreConfig opens a session that borrows an engine-owned graph store.
func NewWithStoreConfig(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, templateManager storetemplate.Manager, permissions sessionapi.Permissions, errs sessionapi.Errors, store *graphstorage.LocalStore, cfg Config) sessionapi.Session {
	return &FileSession{graphsDir: graphsDir, blobsDir: blobsDir, spaceID: spaceID, domainID: cfg.DomainID, templateManager: templateManager, permissions: permissions, errors: errs, store: store, blobLimits: cfg.BlobLimits, blobStaleTmpAge: cfg.BlobStaleTmpAge, currentUserID: cfg.CurrentUserID, embeddingManager: cfg.EmbeddingManager, semanticManager: cfg.SemanticManager, accountingManager: cfg.AccountingManager, userStoreEncryptionKeyB64: cfg.UserStoreEncryptionKeyB64, advancedSemanticEnabled: cfg.AdvancedSemanticEnabled}
}

// NewConfig opens the default file-backed session implementation.
func NewConfig(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, templateManager storetemplate.Manager, permissions sessionapi.Permissions, errs sessionapi.Errors, cfg Config) sessionapi.Session {
	return &FileSession{graphsDir: graphsDir, blobsDir: blobsDir, spaceID: spaceID, domainID: cfg.DomainID, templateManager: templateManager, permissions: permissions, errors: errs, closeStore: true, blobLimits: cfg.BlobLimits, blobStaleTmpAge: cfg.BlobStaleTmpAge, currentUserID: cfg.CurrentUserID, embeddingManager: cfg.EmbeddingManager, semanticManager: cfg.SemanticManager, accountingManager: cfg.AccountingManager, userStoreEncryptionKeyB64: cfg.UserStoreEncryptionKeyB64, advancedSemanticEnabled: cfg.AdvancedSemanticEnabled}
}

// FileSession is the default file-backed Session implementation.
type FileSession struct {
	graphsDir                 string
	blobsDir                  string
	spaceID                   domainspace.SpaceID
	domainID                  graph.DomainID
	templateManager           storetemplate.Manager
	permissions               sessionapi.Permissions
	errors                    sessionapi.Errors
	store                     *graphstorage.LocalStore
	blobs                     *blobstorage.Store
	blobLimits                sessionapi.BlobLimits
	blobStaleTmpAge           time.Duration
	currentUserID             identity.UserID
	embeddingManager          storeembedding.Manager
	semanticManager           storesemantic.GlobalManager
	accountingManager         storeaccounting.Manager
	userStoreEncryptionKeyB64 string
	advancedSemanticEnabled   bool
	closeStore                bool
	closed                    bool
}

// Query starts a programmatic graph query over this session.
func (s *FileSession) Query() *query.Builder { return query.NewBuilder(s) }

func (s *FileSession) ImportTemplates(ctx context.Context, in sessionapi.ImportTemplatesInput) ([]graph.Template, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureAdmin(); err != nil {
		return nil, err
	}
	return s.templateManager.Import(ctx, s.spaceID, in.Document)
}

func (s *FileSession) ListTemplates(ctx context.Context) ([]graph.Template, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	return s.templateManager.ListBySpace(ctx, s.spaceID)
}

func (s *FileSession) AddNode(ctx context.Context, in sessionapi.AddNodeInput) (graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureWrite(); err != nil {
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
	n, err := s.buildNode(ctx, nodes, nodeID, in.TemplateID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	now := time.Now().UTC()
	n.CreatedAt = now
	n.UpdatedAt = now
	if err := s.commitGraph(ctx, []graph.Node{n}, nil, nil, nil); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *FileSession) ListNodes(ctx context.Context) ([]graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return nil, err
	}
	return cloneNodes(nodes), nil
}

func (s *FileSession) UpdateNode(ctx context.Context, in sessionapi.UpdateNodeInput) (graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return graph.Node{}, err
	}
	if in.ID == uuid.Nil {
		return graph.Node{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	nodes, err := s.readNodes()
	if err != nil {
		return graph.Node{}, err
	}
	idx := findNodeIndex(nodes, in.ID)
	if idx < 0 {
		return graph.Node{}, s.errors.NotFound
	}
	// A node has inline text content or blob content, never both.
	if nodes[idx].BlobRef != nil && in.Content != "" {
		return graph.Node{}, fmt.Errorf("%w: blob nodes cannot have inline content; use props (e.g. caption) or annotation children", storetemplate.ErrInvalidInput)
	}
	n, err := s.buildNode(ctx, nodes, in.ID, in.TemplateID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	// Updates never touch the blob reference or domain; replacing blob content or
	// moving domains are separate operations.
	n.BlobRef = nodes[idx].BlobRef
	n.DomainID = nodes[idx].DomainID
	n.CreatedAt = nodes[idx].CreatedAt
	if n.CreatedAt.IsZero() {
		n.CreatedAt = time.Now().UTC()
	}
	n.UpdatedAt = time.Now().UTC()
	candidateNodes := append([]graph.Node(nil), nodes...)
	candidateNodes[idx] = n
	edges, err := s.readEdges()
	if err != nil {
		return graph.Node{}, err
	}
	if err := s.validateIncidentContains(ctx, n, candidateNodes, edges); err != nil {
		return graph.Node{}, err
	}
	if err := s.commitGraph(ctx, []graph.Node{n}, nil, nil, nil); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *FileSession) UpdateNodeAndCreateSibling(ctx context.Context, in sessionapi.UpdateNodeAndCreateSiblingInput) (sessionapi.UpdateNodeAndCreateSiblingResult, error) {
	var out sessionapi.UpdateNodeAndCreateSiblingResult
	err := s.Tx(ctx, sessionapi.TxOptions{}, func(tx sessionapi.Tx) error {
		var err error
		out, err = tx.UpdateNodeAndCreateSibling(ctx, in)
		return err
	})
	return out, err
}

func (s *FileSession) UpsertNode(ctx context.Context, in sessionapi.UpsertNodeInput) (graph.Node, error) {
	if in.ID == nil {
		return s.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: in.TemplateID, Content: in.Content, Props: in.Props})
	}
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return graph.Node{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return graph.Node{}, err
	}
	if findNodeIndex(nodes, *in.ID) >= 0 {
		return s.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: *in.ID, TemplateID: in.TemplateID, Content: in.Content, Props: in.Props})
	}
	n, err := s.buildNode(ctx, nodes, *in.ID, in.TemplateID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	now := time.Now().UTC()
	n.CreatedAt = now
	n.UpdatedAt = now
	if err := s.commitGraph(ctx, []graph.Node{n}, nil, nil, nil); err != nil {
		return graph.Node{}, err
	}
	return n, nil
}

func (s *FileSession) AddEdge(ctx context.Context, in sessionapi.AddEdgeInput) (graph.Edge, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Edge{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return graph.Edge{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return graph.Edge{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return graph.Edge{}, err
	}
	from, ok := findNode(nodes, in.FromID)
	if !ok {
		return graph.Edge{}, fmt.Errorf("%w: from node not found", s.errors.NotFound)
	}
	to, ok := findNode(nodes, in.ToID)
	if !ok {
		return graph.Edge{}, fmt.Errorf("%w: to node not found", s.errors.NotFound)
	}

	edges, err := s.readEdges()
	if err != nil {
		return graph.Edge{}, err
	}
	if err := s.validateNewEdge(ctx, from, to, in.Kind, edges); err != nil {
		return graph.Edge{}, err
	}
	edgeID, err := newGraphUUID()
	if err != nil {
		return graph.Edge{}, err
	}
	if in.ID != nil {
		edgeID = *in.ID
	}
	e := graph.Edge{ID: edgeID, FromID: in.FromID, ToID: in.ToID, Kind: in.Kind, Props: copyProps(in.Props)}
	if err := s.commitGraph(ctx, nil, []graph.Edge{e}, nil, nil); err != nil {
		return graph.Edge{}, err
	}
	return e, nil
}

func (s *FileSession) ListEdges(ctx context.Context) ([]graph.Edge, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	edges, err := s.readEdges()
	if err != nil {
		return nil, err
	}
	return cloneEdges(edges), nil
}

func (s *FileSession) Children(ctx context.Context, parentID graph.NodeID) ([]graph.Edge, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	store, err := s.graphStore()
	if err != nil {
		return nil, err
	}
	return store.Children(ctx, parentID)
}

func (s *FileSession) Parent(ctx context.Context, childID graph.NodeID) (*graph.Edge, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	store, err := s.graphStore()
	if err != nil {
		return nil, err
	}
	return store.Parent(ctx, childID)
}

func (s *FileSession) AddGraph(ctx context.Context, in sessionapi.AddGraphInput) error {
	_, err := s.ApplyGraph(ctx, sessionapi.ApplyGraphInput{AddNodes: in.Nodes, AddEdges: in.Edges, Atomic: in.Atomic})
	return err
}

func (s *FileSession) ApplyGraph(ctx context.Context, in sessionapi.ApplyGraphInput) (sessionapi.ApplyGraphResult, error) {
	var out sessionapi.ApplyGraphResult
	err := s.Tx(ctx, sessionapi.TxOptions{}, func(tx sessionapi.Tx) error {
		var err error
		out, err = tx.ApplyGraph(ctx, in)
		return err
	})
	return out, err
}

func (s *FileSession) GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return graph.Node{}, err
	}
	if err := s.ensureRead(); err != nil {
		return graph.Node{}, err
	}
	store, err := s.graphStore()
	if err != nil {
		return graph.Node{}, err
	}
	n, err := store.GetNode(ctx, id)
	if err != nil {
		if errors.Is(err, graphstorage.ErrNotFound) {
			return graph.Node{}, s.errors.NotFound
		}
		return graph.Node{}, err
	}
	if s.domainID != uuid.Nil && n.DomainID != s.domainID {
		return graph.Node{}, s.errors.NotFound
	}
	return n, nil
}

func (s *FileSession) DeleteNode(ctx context.Context, in sessionapi.DeleteNodeInput) error {
	return s.Tx(ctx, sessionapi.TxOptions{}, func(tx sessionapi.Tx) error {
		return tx.DeleteNode(ctx, in)
	})
}

func (s *FileSession) applyDeleteNode(nodes []graph.Node, edges []graph.Edge, in sessionapi.DeleteNodeInput) ([]graph.NodeID, []graph.Node, []graph.Edge, error) {
	if in.ID == uuid.Nil {
		return nil, nil, nil, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	if _, ok := findNode(nodes, in.ID); !ok {
		return nil, nil, nil, s.errors.NotFound
	}
	deleteIDs := map[graph.NodeID]struct{}{in.ID: {}}
	if in.Recursive {
		changed := true
		for changed {
			changed = false
			for _, edge := range edges {
				if edge.Kind != graph.EdgeKindContains {
					continue
				}
				if _, parentDeleted := deleteIDs[edge.FromID]; parentDeleted {
					if _, already := deleteIDs[edge.ToID]; !already {
						deleteIDs[edge.ToID] = struct{}{}
						changed = true
					}
				}
			}
		}
	} else {
		for _, edge := range edges {
			if edge.Kind == graph.EdgeKindContains && edge.FromID == in.ID {
				if s.errors.Conflict != nil {
					return nil, nil, nil, fmt.Errorf("%w: node has child nodes", s.errors.Conflict)
				}
				return nil, nil, nil, fmt.Errorf("node has child nodes")
			}
		}
	}

	deleted := make([]graph.NodeID, 0, len(deleteIDs))
	remainingNodes := make([]graph.Node, 0, len(nodes))
	for _, n := range nodes {
		if _, isDeleted := deleteIDs[n.ID]; isDeleted {
			deleted = append(deleted, n.ID)
			continue
		}
		remainingNodes = append(remainingNodes, n)
	}
	remainingEdges := make([]graph.Edge, 0, len(edges))
	for _, edge := range edges {
		_, fromDeleted := deleteIDs[edge.FromID]
		_, toDeleted := deleteIDs[edge.ToID]
		if !fromDeleted && !toDeleted {
			remainingEdges = append(remainingEdges, edge)
		}
	}
	return deleted, remainingNodes, remainingEdges, nil
}

func (s *FileSession) Close() error {
	s.closed = true
	if s.store != nil && s.closeStore {
		return s.store.Close()
	}
	return nil
}

func (s *FileSession) ensureOpen(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if s.closed {
		return s.errors.Closed
	}
	return nil
}

func (s *FileSession) ensureRead() error {
	if !s.permissions.Read {
		return s.errors.Unauthorized
	}
	return nil
}

func (s *FileSession) ensureWrite() error {
	if !s.permissions.Write {
		return s.errors.Unauthorized
	}
	return nil
}

func (s *FileSession) ensureAdmin() error {
	if !s.permissions.Admin {
		return s.errors.Unauthorized
	}
	return nil
}

func (s *FileSession) spacePath() string {
	return filepath.Join(s.graphsDir, safeID(s.spaceID))
}

func (s *FileSession) markerPath() string {
	return filepath.Join(s.spacePath(), ".space")
}

func (s *FileSession) ensureSpaceLive() error {
	if _, err := os.Stat(s.markerPath()); err != nil {
		if os.IsNotExist(err) {
			return s.errors.NotFound
		}
		return err
	}
	return nil
}

func (s *FileSession) graphStore() (*graphstorage.LocalStore, error) {
	if s.store != nil {
		return s.store, nil
	}
	store, err := graphstorage.Open(context.Background(), s.spacePath())
	if err != nil {
		return nil, err
	}
	s.store = store
	return s.store, nil
}

func (s *FileSession) readNodes() ([]graph.Node, error) {
	store, err := s.graphStore()
	if err != nil {
		return nil, err
	}
	if s.domainID == uuid.Nil {
		return store.ListNodes(context.Background())
	}
	return store.ListNodesByDomain(context.Background(), s.domainID)
}

func (s *FileSession) readAllNodes() ([]graph.Node, error) {
	store, err := s.graphStore()
	if err != nil {
		return nil, err
	}
	return store.ListNodes(context.Background())
}

func (s *FileSession) readEdges() ([]graph.Edge, error) {
	store, err := s.graphStore()
	if err != nil {
		return nil, err
	}
	return store.ListEdges(context.Background())
}

func (s *FileSession) commitGraph(ctx context.Context, putNodes []graph.Node, putEdges []graph.Edge, deleteNodes []graph.NodeID, deleteEdges []graph.EdgeID) error {
	return s.commitGraphAtRevision(ctx, putNodes, putEdges, deleteNodes, deleteEdges, nil)
}

func (s *FileSession) commitGraphAtRevision(ctx context.Context, putNodes []graph.Node, putEdges []graph.Edge, deleteNodes []graph.NodeID, deleteEdges []graph.EdgeID, expectedRevision *uint64) error {
	store, err := s.graphStore()
	if err != nil {
		return err
	}
	releasedBlobs := s.blobRefsOfNodes(ctx, store, deleteNodes)
	txn, err := store.Begin(ctx)
	if err != nil {
		return err
	}
	if expectedRevision != nil {
		txn.ExpectRevision(*expectedRevision)
	}
	if s.advancedSemanticEnabled && (len(putNodes) > 0 || len(putEdges) > 0 || len(deleteNodes) > 0 || len(deleteEdges) > 0) {
		event, err := s.buildGraphDirtyEvent(ctx, store, putNodes, putEdges, deleteNodes, deleteEdges)
		if err != nil {
			_ = txn.Rollback()
			return err
		}
		txn.SetCommitHook(func(info graphstorage.CommitInfo) error {
			event.TxnID = info.TxnID
			event.GraphRevision = info.NextRevision
			mgr := storesemantic.NewSpaceManager()
			if err := mgr.Init(ctx, filepath.Join(s.graphsDir, s.spaceID.String(), "semantic"), s.spaceID); err != nil {
				return err
			}
			_, err := mgr.AppendGraphDirtyEvent(ctx, event)
			return err
		})
	}
	for _, node := range putNodes {
		if err := txn.PutNode(node); err != nil {
			_ = txn.Rollback()
			return err
		}
	}
	for _, edge := range putEdges {
		if err := txn.PutEdge(edge); err != nil {
			_ = txn.Rollback()
			return err
		}
	}
	for _, id := range deleteEdges {
		if err := txn.DeleteEdge(id); err != nil {
			_ = txn.Rollback()
			return err
		}
	}
	for _, id := range deleteNodes {
		if err := txn.DeleteNode(id); err != nil {
			_ = txn.Rollback()
			return err
		}
	}
	if err := txn.Commit(); err != nil {
		if errors.Is(err, graphstorage.ErrConflict) && s.errors.Conflict != nil {
			return s.errors.Conflict
		}
		return err
	}
	s.releaseUnreferencedBlobs(ctx, store, releasedBlobs)
	return nil
}

func (s *FileSession) buildGraphDirtyEvent(ctx context.Context, store *graphstorage.LocalStore, putNodes []graph.Node, putEdges []graph.Edge, deleteNodes []graph.NodeID, deleteEdges []graph.EdgeID) (domainsemantic.GraphDirtyEvent, error) {
	event := domainsemantic.GraphDirtyEvent{SpaceID: s.spaceID, DomainIDs: []graph.DomainID{}, CreatedNodeIDs: []graph.NodeID{}, UpdatedNodeIDs: []graph.NodeID{}, DeletedNodeIDs: []graph.NodeID{}, ChangedEdges: []domainsemantic.GraphDirtyEdgeChange{}, OldParentByNodeID: map[graph.NodeID]graph.NodeID{}, NewParentByNodeID: map[graph.NodeID]graph.NodeID{}, OldDomainByNodeID: map[graph.NodeID]graph.DomainID{}, NewDomainByNodeID: map[graph.NodeID]graph.DomainID{}, CommittedAt: time.Now().UTC()}
	domains := map[graph.DomainID]bool{}
	addDomain := func(id graph.DomainID) {
		if id != uuid.Nil {
			domains[id] = true
		}
	}
	for _, node := range putNodes {
		old, err := store.GetNode(ctx, node.ID)
		if err == nil {
			event.UpdatedNodeIDs = append(event.UpdatedNodeIDs, node.ID)
			event.OldDomainByNodeID[node.ID] = old.DomainID
			addDomain(old.DomainID)
		} else if errors.Is(err, graphstorage.ErrNotFound) {
			event.CreatedNodeIDs = append(event.CreatedNodeIDs, node.ID)
		} else {
			return domainsemantic.GraphDirtyEvent{}, err
		}
		event.NewDomainByNodeID[node.ID] = node.DomainID
		addDomain(node.DomainID)
		if parent, err := store.Parent(ctx, node.ID); err == nil && parent != nil {
			event.OldParentByNodeID[node.ID] = parent.FromID
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return domainsemantic.GraphDirtyEvent{}, err
		}
	}
	for _, id := range deleteNodes {
		event.DeletedNodeIDs = append(event.DeletedNodeIDs, id)
		old, err := store.GetNode(ctx, id)
		if err == nil {
			event.OldDomainByNodeID[id] = old.DomainID
			addDomain(old.DomainID)
		} else if !errors.Is(err, graphstorage.ErrNotFound) {
			return domainsemantic.GraphDirtyEvent{}, err
		}
		if parent, err := store.Parent(ctx, id); err == nil && parent != nil {
			event.OldParentByNodeID[id] = parent.FromID
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return domainsemantic.GraphDirtyEvent{}, err
		}
	}
	for _, edge := range putEdges {
		change := "added"
		if old, err := store.GetEdge(ctx, edge.ID); err == nil {
			change = "updated"
			if old.Kind == graph.EdgeKindContains && old.ToID == edge.ToID && old.FromID != edge.FromID {
				event.OldParentByNodeID[old.ToID] = old.FromID
			}
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return domainsemantic.GraphDirtyEvent{}, err
		}
		if edge.Kind == graph.EdgeKindContains {
			event.NewParentByNodeID[edge.ToID] = edge.FromID
		}
		event.ChangedEdges = append(event.ChangedEdges, domainsemantic.GraphDirtyEdgeChange{EdgeID: edge.ID, Kind: edge.Kind, Change: change, FromID: edge.FromID, ToID: edge.ToID})
	}
	for _, id := range deleteEdges {
		edge, err := store.GetEdge(ctx, id)
		if err != nil {
			if errors.Is(err, graphstorage.ErrNotFound) {
				continue
			}
			return domainsemantic.GraphDirtyEvent{}, err
		}
		if edge.Kind == graph.EdgeKindContains {
			event.OldParentByNodeID[edge.ToID] = edge.FromID
		}
		event.ChangedEdges = append(event.ChangedEdges, domainsemantic.GraphDirtyEdgeChange{EdgeID: edge.ID, Kind: edge.Kind, Change: "removed", FromID: edge.FromID, ToID: edge.ToID})
	}
	for id := range domains {
		event.DomainIDs = append(event.DomainIDs, id)
	}
	return event, nil
}

// blobRefsOfNodes collects the blob IDs referenced by nodes about to be
// deleted, so unreferenced blob files can be released after commit.
func (s *FileSession) blobRefsOfNodes(ctx context.Context, store *graphstorage.LocalStore, nodeIDs []graph.NodeID) map[graph.BlobID]struct{} {
	if len(nodeIDs) == 0 {
		return nil
	}
	out := map[graph.BlobID]struct{}{}
	for _, id := range nodeIDs {
		n, err := store.GetNode(ctx, id)
		if err != nil {
			continue
		}
		if n.BlobRef != nil {
			out[*n.BlobRef] = struct{}{}
		}
	}
	return out
}

// releaseUnreferencedBlobs deletes blob files whose last referencing node was
// just removed. Removal is best-effort: leftovers are reclaimed by the orphan
// sweep when the blob store is next opened.
func (s *FileSession) releaseUnreferencedBlobs(ctx context.Context, store *graphstorage.LocalStore, candidates map[graph.BlobID]struct{}) {
	if len(candidates) == 0 {
		return
	}
	blobs, err := s.blobStore()
	if err != nil {
		return
	}
	for id := range candidates {
		count, err := store.BlobRefCount(ctx, id)
		if err != nil || count > 0 {
			continue
		}
		_ = blobs.Delete(ctx, id)
	}
}

func (s *FileSession) buildNode(ctx context.Context, nodes []graph.Node, nodeID graph.NodeID, templateID *graph.TemplateID, content string, inputProps map[string]any) (graph.Node, error) {
	if nodeID == uuid.Nil {
		return graph.Node{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	props := copyProps(inputProps)
	if templateID != nil {
		t, err := s.templateManager.GetByID(ctx, *templateID)
		if err != nil {
			if errors.Is(err, storetemplate.ErrTemplateNotFound) {
				return graph.Node{}, fmt.Errorf("%w: template not found", s.errors.NotFound)
			}
			return graph.Node{}, err
		}
		if t.SpaceID != s.spaceID {
			return graph.Node{}, fmt.Errorf("%w: template not found in space", s.errors.NotFound)
		}
		if err := validateProps(&props, t); err != nil {
			return graph.Node{}, err
		}
	}
	return graph.Node{ID: nodeID, DomainID: s.domainID, TemplateID: templateID, Content: content, Props: props}, nil
}

func (s *FileSession) validateNewEdge(ctx context.Context, from graph.Node, to graph.Node, kind graph.EdgeKind, edges []graph.Edge) error {
	if kind != graph.EdgeKindContains {
		return nil
	}
	if from.ID == to.ID {
		return fmt.Errorf("%w: contains edge cannot target itself", storetemplate.ErrInvalidInput)
	}
	if from.DomainID != uuid.Nil && to.DomainID != uuid.Nil && from.DomainID != to.DomainID {
		return fmt.Errorf("%w: contains edges cannot cross domains", storetemplate.ErrInvalidInput)
	}
	for _, edge := range edges {
		if edge.Kind == graph.EdgeKindContains && edge.ToID == to.ID {
			return fmt.Errorf("%w: node already has a contains parent", storetemplate.ErrInvalidInput)
		}
	}
	if containsPath(edges, to.ID, from.ID) {
		return fmt.Errorf("%w: contains edge would create a cycle", storetemplate.ErrInvalidInput)
	}
	childTemplate, err := s.nodeTemplate(ctx, to, "child")
	if err != nil {
		return err
	}
	return s.validateChild(ctx, from, childTemplate)
}

func (s *FileSession) validateIncidentContains(ctx context.Context, node graph.Node, nodes []graph.Node, edges []graph.Edge) error {
	for _, edge := range edges {
		if edge.Kind != graph.EdgeKindContains {
			continue
		}
		if edge.FromID == node.ID {
			child, ok := findNode(nodes, edge.ToID)
			if !ok {
				continue
			}
			childTemplate, err := s.nodeTemplate(ctx, child, "child")
			if err != nil {
				return err
			}
			if err := s.validateChild(ctx, node, childTemplate); err != nil {
				return err
			}
		}
		if edge.ToID == node.ID {
			parent, ok := findNode(nodes, edge.FromID)
			if !ok {
				continue
			}
			childTemplate, err := s.nodeTemplate(ctx, node, "child")
			if err != nil {
				return err
			}
			if err := s.validateChild(ctx, parent, childTemplate); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *FileSession) nodeTemplate(ctx context.Context, node graph.Node, label string) (*graph.Template, error) {
	if node.TemplateID == nil {
		return nil, nil
	}
	t, err := s.templateManager.GetByID(ctx, *node.TemplateID)
	if err != nil {
		if errors.Is(err, storetemplate.ErrTemplateNotFound) {
			return nil, fmt.Errorf("%w: %s template not found", s.errors.NotFound, label)
		}
		return nil, err
	}
	if t.SpaceID != s.spaceID {
		return nil, fmt.Errorf("%w: %s template not found in space", s.errors.NotFound, label)
	}
	return &t, nil
}

func containsPath(edges []graph.Edge, from graph.NodeID, target graph.NodeID) bool {
	visited := map[graph.NodeID]struct{}{}
	var visit func(graph.NodeID) bool
	visit = func(id graph.NodeID) bool {
		if id == target {
			return true
		}
		if _, ok := visited[id]; ok {
			return false
		}
		visited[id] = struct{}{}
		for _, edge := range edges {
			if edge.Kind == graph.EdgeKindContains && edge.FromID == id {
				if visit(edge.ToID) {
					return true
				}
			}
		}
		return false
	}
	return visit(from)
}

func (s *FileSession) validateChild(ctx context.Context, parent graph.Node, childTemplate *graph.Template) error {
	if parent.TemplateID == nil {
		return nil
	}
	parentTemplate, err := s.templateManager.GetByID(ctx, *parent.TemplateID)
	if err != nil {
		if errors.Is(err, storetemplate.ErrTemplateNotFound) {
			return fmt.Errorf("%w: parent template not found", s.errors.NotFound)
		}
		return err
	}
	if parentTemplate.SpaceID != s.spaceID {
		return fmt.Errorf("%w: parent template not found in space", s.errors.NotFound)
	}
	if !parentTemplate.Children.Allowed {
		return fmt.Errorf("%w: parent template does not allow children", storetemplate.ErrInvalidInput)
	}
	if len(parentTemplate.Children.AllowedTemplates) == 0 {
		return nil
	}
	if childTemplate == nil {
		return fmt.Errorf("%w: child template is required", storetemplate.ErrInvalidInput)
	}
	for _, ref := range parentTemplate.Children.AllowedTemplates {
		if ref.Key == childTemplate.Key && ref.Version == childTemplate.Version {
			return nil
		}
	}
	// Application extension nodes are app-level primitives that may be inserted into
	// existing imported Logseq outlines whose templates predate the extension.
	// Keep normal template allow-list validation strict for all other cases.
	// Legacy pkm.* templates are accepted so existing pre-Mycel data can still be
	// edited before/mid migration.
	if (strings.HasPrefix(childTemplate.Key, "app.") || strings.HasPrefix(childTemplate.Key, "pkm.")) && strings.HasPrefix(parentTemplate.Key, "logseq.") {
		return nil
	}
	return fmt.Errorf("%w: child template %s@%s is not allowed", storetemplate.ErrInvalidInput, childTemplate.Key, childTemplate.Version)
}

func validateProps(props *map[string]any, tmpl graph.Template) error {
	if *props == nil {
		*props = map[string]any{}
	}
	allowed := map[string]graph.TemplateProperty{}
	for _, prop := range tmpl.Properties.Allowed {
		allowed[prop.Name] = prop
		if _, ok := (*props)[prop.Name]; !ok && prop.Default != nil {
			(*props)[prop.Name] = prop.Default
		}
	}
	for _, name := range tmpl.Properties.Forbidden {
		if _, ok := (*props)[name]; ok {
			return fmt.Errorf("%w: property %q is forbidden", storetemplate.ErrInvalidInput, name)
		}
	}
	for name, value := range *props {
		prop, ok := allowed[name]
		if !ok {
			if !tmpl.Properties.AllowExtra {
				return fmt.Errorf("%w: property %q is not allowed", storetemplate.ErrInvalidInput, name)
			}
			continue
		}
		if err := validatePropertyValue(prop, value); err != nil {
			return err
		}
	}
	for _, prop := range tmpl.Properties.Allowed {
		if !prop.Required {
			continue
		}
		value, ok := (*props)[prop.Name]
		if !ok || value == nil {
			return fmt.Errorf("%w: required property %q is missing", storetemplate.ErrInvalidInput, prop.Name)
		}
	}
	return nil
}

func validatePropertyValue(prop graph.TemplateProperty, value any) error {
	if value == nil {
		return fmt.Errorf("%w: property %q cannot be null", storetemplate.ErrInvalidInput, prop.Name)
	}
	valid := false
	switch prop.Type {
	case graph.PropertyTypeString:
		_, valid = value.(string)
	case graph.PropertyTypeNumber:
		valid = isNumber(value)
	case graph.PropertyTypeBool:
		_, valid = value.(bool)
	case graph.PropertyTypeObject:
		_, valid = value.(map[string]any)
	case graph.PropertyTypeArray:
		valid = isArray(value)
	case graph.PropertyTypeDate:
		valid = isDate(value)
	default:
		return fmt.Errorf("%w: unsupported property type %q", storetemplate.ErrInvalidInput, prop.Type)
	}
	if !valid {
		return fmt.Errorf("%w: property %q must be %s", storetemplate.ErrInvalidInput, prop.Name, prop.Type)
	}
	return nil
}

func isArray(value any) bool {
	if value == nil {
		return false
	}
	kind := reflect.TypeOf(value).Kind()
	return kind == reflect.Array || kind == reflect.Slice
}

func isNumber(value any) bool {
	switch n := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	case json.Number:
		_, err := n.Float64()
		return err == nil
	default:
		return false
	}
}

func isDate(value any) bool {
	s, ok := value.(string)
	if !ok {
		return false
	}
	if _, err := time.Parse(time.RFC3339, s); err == nil {
		return true
	}
	_, err := time.Parse("2006-01-02", s)
	return err == nil
}

func copyProps(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func newGraphUUID() (uuid.UUID, error) {
	return graphstorage.NewUUIDv7()
}

func changedEdges(original []graph.Edge, candidate []graph.Edge) []graph.Edge {
	byID := map[graph.EdgeID]graph.Edge{}
	for _, edge := range original {
		byID[edge.ID] = edge
	}
	out := []graph.Edge{}
	for _, edge := range candidate {
		old, ok := byID[edge.ID]
		if !ok || !edgesEqual(old, edge) {
			out = append(out, edge)
		}
	}
	return out
}

func edgesEqual(left graph.Edge, right graph.Edge) bool {
	return left.ID == right.ID && left.FromID == right.FromID && left.ToID == right.ToID && left.Kind == right.Kind && reflect.DeepEqual(left.Props, right.Props)
}

func deletedEdges(original []graph.Edge, remaining []graph.Edge) []graph.EdgeID {
	live := map[graph.EdgeID]struct{}{}
	for _, edge := range remaining {
		live[edge.ID] = struct{}{}
	}
	out := []graph.EdgeID{}
	for _, edge := range original {
		if _, ok := live[edge.ID]; !ok {
			out = append(out, edge.ID)
		}
	}
	return out
}

func mapKeys(m map[graph.NodeID]struct{}) []graph.NodeID {
	out := make([]graph.NodeID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	return out
}

func findNode(nodes []graph.Node, id graph.NodeID) (graph.Node, bool) {
	idx := findNodeIndex(nodes, id)
	if idx < 0 {
		return graph.Node{}, false
	}
	return nodes[idx], true
}

func indexNodes(nodes []graph.Node) map[graph.NodeID]int {
	out := make(map[graph.NodeID]int, len(nodes))
	for i, node := range nodes {
		out[node.ID] = i
	}
	return out
}

func indexEdges(edges []graph.Edge) map[graph.EdgeID]int {
	out := make(map[graph.EdgeID]int, len(edges))
	for i, edge := range edges {
		out[edge.ID] = i
	}
	return out
}

func findNodeIndex(nodes []graph.Node, id graph.NodeID) int {
	for i, n := range nodes {
		if n.ID == id {
			return i
		}
	}
	return -1
}

func cloneNodes(nodes []graph.Node) []graph.Node {
	out := make([]graph.Node, 0, len(nodes))
	for _, node := range nodes {
		node.Props = copyProps(node.Props)
		out = append(out, node)
	}
	return out
}

func cloneEdges(edges []graph.Edge) []graph.Edge {
	out := make([]graph.Edge, 0, len(edges))
	for _, edge := range edges {
		edge.Props = copyProps(edge.Props)
		out = append(out, edge)
	}
	return out
}

func safeID(id domainspace.SpaceID) string {
	repl := strings.NewReplacer(":", "_", "/", "_", "\\", "_")
	return repl.Replace(id.String())
}
