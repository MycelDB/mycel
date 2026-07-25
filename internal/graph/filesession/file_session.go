package filesession

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/blob/storage"
	"github.com/myceldb/mycel/internal/graph/change"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/graph/query"
	"github.com/myceldb/mycel/internal/graph/storage"
	"github.com/myceldb/mycel/internal/identity/model"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	storeaccounting "github.com/myceldb/mycel/internal/semantic/accounting"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

var errInvalidInput = errors.New("invalid input")

// Config carries runtime knobs for the file-backed session implementation.
type Config struct {
	BlobLimits                sessionapi.BlobLimits
	BlobStaleTmpAge           time.Duration
	CurrentUserID             identity.UserID
	SemanticManager           storesemantic.GlobalManager
	AccountingManager         storeaccounting.Manager
	UserStoreEncryptionKeyB64 string
	DomainID                  graph.DomainID
	AdvancedSemanticEnabled   bool
	GraphChangeSink           graphchange.Sink
	SchemaManager             schemaservice.Manager
}

// New opens the default file-backed session implementation.
func New(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, permissions sessionapi.Permissions, errs sessionapi.Errors) sessionapi.Session {
	return &FileSession{graphsDir: graphsDir, blobsDir: blobsDir, spaceID: spaceID, permissions: permissions, errors: errs, closeStore: true}
}

// NewWithStore opens a session that borrows an engine-owned graph store.
func NewWithStore(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, permissions sessionapi.Permissions, errs sessionapi.Errors, store *graphstorage.LocalStore) sessionapi.Session {
	return &FileSession{graphsDir: graphsDir, blobsDir: blobsDir, spaceID: spaceID, permissions: permissions, errors: errs, store: store}
}

// NewWithStoreConfig opens a session that borrows an engine-owned graph store.
func NewWithStoreConfig(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, permissions sessionapi.Permissions, errs sessionapi.Errors, store *graphstorage.LocalStore, cfg Config) sessionapi.Session {
	return &FileSession{graphsDir: graphsDir, blobsDir: blobsDir, spaceID: spaceID, domainID: cfg.DomainID, permissions: permissions, errors: errs, store: store, blobLimits: cfg.BlobLimits, blobStaleTmpAge: cfg.BlobStaleTmpAge, currentUserID: cfg.CurrentUserID, semanticManager: cfg.SemanticManager, accountingManager: cfg.AccountingManager, userStoreEncryptionKeyB64: cfg.UserStoreEncryptionKeyB64, advancedSemanticEnabled: cfg.AdvancedSemanticEnabled, graphChangeSink: cfg.GraphChangeSink, schemaManager: cfg.SchemaManager}
}

// NewConfig opens the default file-backed session implementation.
func NewConfig(graphsDir string, blobsDir string, spaceID domainspace.SpaceID, permissions sessionapi.Permissions, errs sessionapi.Errors, cfg Config) sessionapi.Session {
	return &FileSession{graphsDir: graphsDir, blobsDir: blobsDir, spaceID: spaceID, domainID: cfg.DomainID, permissions: permissions, errors: errs, closeStore: true, blobLimits: cfg.BlobLimits, blobStaleTmpAge: cfg.BlobStaleTmpAge, currentUserID: cfg.CurrentUserID, semanticManager: cfg.SemanticManager, accountingManager: cfg.AccountingManager, userStoreEncryptionKeyB64: cfg.UserStoreEncryptionKeyB64, advancedSemanticEnabled: cfg.AdvancedSemanticEnabled, graphChangeSink: cfg.GraphChangeSink, schemaManager: cfg.SchemaManager}
}

// FileSession is the default file-backed Session implementation.
type FileSession struct {
	graphsDir                 string
	blobsDir                  string
	spaceID                   domainspace.SpaceID
	domainID                  graph.DomainID
	permissions               sessionapi.Permissions
	errors                    sessionapi.Errors
	store                     *graphstorage.LocalStore
	blobs                     *blobstorage.Store
	blobLimits                sessionapi.BlobLimits
	blobStaleTmpAge           time.Duration
	currentUserID             identity.UserID
	semanticManager           storesemantic.GlobalManager
	accountingManager         storeaccounting.Manager
	userStoreEncryptionKeyB64 string
	advancedSemanticEnabled   bool
	graphChangeSink           graphchange.Sink
	schemaManager             schemaservice.Manager
	lastGraphChangeSinkErr    error
	closeStore                bool
	closed                    bool
}

// Query starts a programmatic graph query over this session.
func (s *FileSession) Query() *query.Builder { return query.NewBuilder(s) }

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
	nodeID, err := newGraphUUID()
	if err != nil {
		return graph.Node{}, err
	}
	if in.ID != nil {
		nodeID = *in.ID
	}
	n, err := s.buildNode(ctx, nodeID, in.Content, in.Properties)
	if err != nil {
		return graph.Node{}, err
	}
	applyNodeShape(&n, in.Labels, in.Properties, in.Payload, in.Meta)
	now := time.Now().UTC()
	n.CreatedAt = now
	n.UpdatedAt = now
	if err := s.validateSchemaNode(ctx, n); err != nil {
		return graph.Node{}, err
	}
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
		return graph.Node{}, fmt.Errorf("%w: blob nodes cannot have inline content; use props (e.g. caption) or annotation children", errInvalidInput)
	}
	properties := in.Properties
	if properties == nil {
		properties = in.Props
	}
	n, err := s.buildNode(ctx, in.ID, in.Content, properties)
	if err != nil {
		return graph.Node{}, err
	}
	applyNodeShape(&n, in.Labels, properties, in.Payload, in.Meta)
	// Updates never touch the blob reference or domain; replacing blob content or
	// moving domains are separate operations.
	n.BlobRef = nodes[idx].BlobRef
	n.DomainID = nodes[idx].DomainID
	n.CreatedAt = nodes[idx].CreatedAt
	if n.CreatedAt.IsZero() {
		n.CreatedAt = time.Now().UTC()
	}
	n.UpdatedAt = time.Now().UTC()
	if err := s.validateSchemaNode(ctx, n); err != nil {
		return graph.Node{}, err
	}
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
		return s.AddNode(ctx, sessionapi.AddNodeInput{Content: in.Content, Props: in.Properties})
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
		return s.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: *in.ID, Content: in.Content, Props: in.Properties})
	}
	n, err := s.buildNode(ctx, *in.ID, in.Content, in.Properties)
	if err != nil {
		return graph.Node{}, err
	}
	now := time.Now().UTC()
	n.CreatedAt = now
	n.UpdatedAt = now
	if err := s.validateSchemaNode(ctx, n); err != nil {
		return graph.Node{}, err
	}
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
	if err := s.validateNewEdge(ctx, from, to, in.Labels, edges); err != nil {
		return graph.Edge{}, err
	}
	edgeID, err := newGraphUUID()
	if err != nil {
		return graph.Edge{}, err
	}
	if in.ID != nil {
		edgeID = *in.ID
	}
	now := time.Now().UTC()
	e := graph.Edge{ID: edgeID, DomainID: s.domainID, FromID: in.FromID, ToID: in.ToID, Labels: append([]string(nil), in.Labels...), Properties: copyProps(in.Properties), Payload: copyProps(in.Payload), Meta: copyProps(in.Meta), CreatedAt: now, UpdatedAt: now}
	if err := s.validateSchemaEdge(ctx, e, from, to); err != nil {
		return graph.Edge{}, err
	}
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

func (s *FileSession) applyDeleteNode(ctx context.Context, nodes []graph.Node, edges []graph.Edge, in sessionapi.DeleteNodeInput) ([]graph.NodeID, []graph.Node, []graph.Edge, error) {
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
				isHierarchy, err := s.isHierarchyEdge(ctx, edge)
				if err != nil {
					return nil, nil, nil, err
				}
				if !isHierarchy {
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
			isHierarchy, err := s.isHierarchyEdge(ctx, edge)
			if err != nil {
				return nil, nil, nil, err
			}
			if isHierarchy && edge.FromID == in.ID {
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

func (s *FileSession) manifestPath() string {
	return filepath.Join(s.spacePath(), "manifest.mycel")
}

func (s *FileSession) ensureSpaceLive() error {
	if _, err := os.Stat(s.manifestPath()); err != nil {
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
	var event graphchange.CommittedEvent
	if s.graphChangeSink != nil && (len(putNodes) > 0 || len(putEdges) > 0 || len(deleteNodes) > 0 || len(deleteEdges) > 0) {
		event, err = s.buildGraphChangeEvent(ctx, store, putNodes, putEdges, deleteNodes, deleteEdges)
		if err != nil {
			_ = txn.Rollback()
			return err
		}
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
	info, err := txn.CommitWithInfo()
	if err != nil {
		if errors.Is(err, graphstorage.ErrConflict) && s.errors.Conflict != nil {
			return s.errors.Conflict
		}
		return err
	}
	if s.graphChangeSink != nil && !event.Empty() {
		event.TxnID = info.TxnID
		event.GraphRevision = info.NextRevision
		if err := s.graphChangeSink.OnGraphCommitted(ctx, event); err != nil {
			s.lastGraphChangeSinkErr = err
		} else {
			s.lastGraphChangeSinkErr = nil
		}
	}
	s.releaseUnreferencedBlobs(ctx, store, releasedBlobs)
	return nil
}

func (s *FileSession) LastGraphChangeSinkError() error { return s.lastGraphChangeSinkErr }

func (s *FileSession) buildGraphChangeEvent(ctx context.Context, store *graphstorage.LocalStore, putNodes []graph.Node, putEdges []graph.Edge, deleteNodes []graph.NodeID, deleteEdges []graph.EdgeID) (graphchange.CommittedEvent, error) {
	event := graphchange.CommittedEvent{SpaceID: s.spaceID, DomainIDs: []graph.DomainID{}, CreatedNodeIDs: []graph.NodeID{}, UpdatedNodeIDs: []graph.NodeID{}, DeletedNodeIDs: []graph.NodeID{}, ChangedEdges: []graphchange.EdgeChange{}, OldParentByNodeID: map[graph.NodeID]graph.NodeID{}, NewParentByNodeID: map[graph.NodeID]graph.NodeID{}, OldDomainByNodeID: map[graph.NodeID]graph.DomainID{}, NewDomainByNodeID: map[graph.NodeID]graph.DomainID{}, CommittedAt: time.Now().UTC()}
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
			return graphchange.CommittedEvent{}, err
		}
		event.NewDomainByNodeID[node.ID] = node.DomainID
		addDomain(node.DomainID)
		if parent, err := s.storeHierarchyParent(ctx, store, node.ID); err != nil {
			return graphchange.CommittedEvent{}, err
		} else if parent != nil {
			event.OldParentByNodeID[node.ID] = parent.FromID
		}
	}
	for _, id := range deleteNodes {
		event.DeletedNodeIDs = append(event.DeletedNodeIDs, id)
		old, err := store.GetNode(ctx, id)
		if err == nil {
			event.OldDomainByNodeID[id] = old.DomainID
			addDomain(old.DomainID)
		} else if !errors.Is(err, graphstorage.ErrNotFound) {
			return graphchange.CommittedEvent{}, err
		}
		if parent, err := s.storeHierarchyParent(ctx, store, id); err != nil {
			return graphchange.CommittedEvent{}, err
		} else if parent != nil {
			event.OldParentByNodeID[id] = parent.FromID
		}
	}
	for _, edge := range putEdges {
		change := "added"
		if old, err := store.GetEdge(ctx, edge.ID); err == nil {
			change = "updated"
			oldHierarchy, err := s.isHierarchyEdge(ctx, old)
			if err != nil {
				return graphchange.CommittedEvent{}, err
			}
			if oldHierarchy && old.ToID == edge.ToID && old.FromID != edge.FromID {
				event.OldParentByNodeID[old.ToID] = old.FromID
			}
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return graphchange.CommittedEvent{}, err
		}
		isHierarchy, err := s.isHierarchyEdge(ctx, edge)
		if err != nil {
			return graphchange.CommittedEvent{}, err
		}
		if isHierarchy {
			event.NewParentByNodeID[edge.ToID] = edge.FromID
		}
		event.ChangedEdges = append(event.ChangedEdges, graphchange.EdgeChange{EdgeID: edge.ID, Labels: append([]string(nil), edge.Labels...), Change: change, FromID: edge.FromID, ToID: edge.ToID})
	}
	for _, id := range deleteEdges {
		edge, err := store.GetEdge(ctx, id)
		if err != nil {
			if errors.Is(err, graphstorage.ErrNotFound) {
				continue
			}
			return graphchange.CommittedEvent{}, err
		}
		isHierarchy, err := s.isHierarchyEdge(ctx, edge)
		if err != nil {
			return graphchange.CommittedEvent{}, err
		}
		if isHierarchy {
			event.OldParentByNodeID[edge.ToID] = edge.FromID
		}
		event.ChangedEdges = append(event.ChangedEdges, graphchange.EdgeChange{EdgeID: edge.ID, Labels: append([]string(nil), edge.Labels...), Change: "removed", FromID: edge.FromID, ToID: edge.ToID})
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

func (s *FileSession) buildNode(ctx context.Context, nodeID graph.NodeID, content string, inputProps map[string]any) (graph.Node, error) {
	if nodeID == uuid.Nil {
		return graph.Node{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	props := copyProps(inputProps)
	_ = ctx
	node := graph.Node{ID: nodeID, DomainID: s.domainID, Content: content, Props: props}
	if content != "" {
		node.Payload = map[string]any{"text": content}
	}
	if props != nil {
		node.Properties = copyProps(props)
	}
	return node, nil
}

func (s *FileSession) validateSchemaNode(ctx context.Context, node graph.Node) error {
	if s.schemaManager == nil {
		return nil
	}
	result, err := s.schemaManager.ValidateNode(ctx, node.DomainID, node)
	if err != nil {
		return err
	}
	if result.Valid() {
		return nil
	}
	return fmt.Errorf("%w: schema validation failed: %s", errInvalidInput, formatSchemaIssues(result.Issues))
}

func (s *FileSession) validateSchemaEdge(ctx context.Context, edge graph.Edge, from graph.Node, to graph.Node) error {
	if s.schemaManager == nil {
		return nil
	}
	result, err := s.schemaManager.ValidateEdge(ctx, edge.DomainID, edge, from, to)
	if err != nil {
		return err
	}
	if result.Valid() {
		return nil
	}
	return fmt.Errorf("%w: schema validation failed: %s", errInvalidInput, formatSchemaIssues(result.Issues))
}

func formatSchemaIssues(issues []schemaservice.ValidationIssue) string {
	if len(issues) == 0 {
		return "unknown schema validation error"
	}
	parts := make([]string, 0, len(issues))
	for _, issue := range issues {
		if issue.Path != "" {
			parts = append(parts, issue.Path+": "+issue.Message)
			continue
		}
		parts = append(parts, issue.Message)
	}
	return strings.Join(parts, "; ")
}

func applyNodeShape(node *graph.Node, labels []string, properties, payload, meta map[string]any) {
	if labels != nil {
		node.Labels = append([]string(nil), labels...)
	}
	if properties != nil {
		node.Properties = copyProps(properties)
	}
	if payload != nil {
		node.Payload = copyProps(payload)
		if text, ok := payload["text"].(string); ok {
			node.Content = text
		}
	}
	if meta != nil {
		node.Meta = copyProps(meta)
	}
}

func (s *FileSession) validateNewEdge(ctx context.Context, from graph.Node, to graph.Node, labels []string, edges []graph.Edge) error {
	hierarchy, err := s.hierarchyPolicyForLabels(ctx, labels)
	if err != nil {
		return err
	}
	if hierarchy == nil {
		return nil
	}
	if from.ID == to.ID {
		return fmt.Errorf("%w: hierarchy edge cannot target itself", errInvalidInput)
	}
	if hierarchy.SameDomain && from.DomainID != uuid.Nil && to.DomainID != uuid.Nil && from.DomainID != to.DomainID {
		return fmt.Errorf("%w: hierarchy edges cannot cross domains", errInvalidInput)
	}
	if hierarchy.SingleParent {
		for _, edge := range edges {
			edgeHierarchy, err := s.hierarchyPolicyForLabels(ctx, edge.Labels)
			if err != nil {
				return err
			}
			if edgeHierarchy != nil && edge.ToID == to.ID {
				return fmt.Errorf("%w: node already has a hierarchy parent", errInvalidInput)
			}
		}
	}
	if hierarchy.Acyclic {
		cycle, err := containsPathWithPolicy(ctx, s, edges, to.ID, from.ID)
		if err != nil {
			return err
		}
		if cycle {
			return fmt.Errorf("%w: hierarchy edge would create a cycle", errInvalidInput)
		}
	}
	return nil
}

func (s *FileSession) validateIncidentContains(ctx context.Context, node graph.Node, nodes []graph.Node, edges []graph.Edge) error {
	return nil
}

func (s *FileSession) storeHierarchyParent(ctx context.Context, store *graphstorage.LocalStore, childID graph.NodeID) (*graph.Edge, error) {
	edges, err := store.ListEdges(ctx)
	if err != nil {
		return nil, err
	}
	for _, edge := range edges {
		isHierarchy, err := s.isHierarchyEdge(ctx, edge)
		if err != nil {
			return nil, err
		}
		if isHierarchy && edge.ToID == childID {
			copy := cloneEdge(edge)
			return &copy, nil
		}
	}
	return nil, nil
}

func (s *FileSession) isHierarchyEdge(ctx context.Context, edge graph.Edge) (bool, error) {
	policy, err := s.hierarchyPolicyForLabels(ctx, edge.Labels)
	return policy != nil, err
}

func (s *FileSession) hierarchyEdgeLabelsForMutation(ctx context.Context) ([]string, error) {
	if s.schemaManager != nil {
		schema, err := s.schemaManager.GetDomainSchema(ctx, s.domainID)
		if err != nil && !errors.Is(err, schemaservice.ErrSchemaNotFound) {
			return nil, err
		}
		if err == nil {
			for _, edgeType := range schema.EdgeTypes {
				if edgeType.Hierarchy != nil && edgeType.Hierarchy.Enabled {
					if len(edgeType.Labels) > 0 {
						return []string{edgeType.Labels[0]}, nil
					}
					return []string{edgeType.Name}, nil
				}
			}
		}
	}
	return []string{"contains"}, nil
}

func (s *FileSession) hierarchyPolicyForLabels(ctx context.Context, labels []string) (*schemamodel.HierarchyPolicy, error) {
	if s.schemaManager == nil {
		if hasEdgeLabel(labels, "contains") {
			return &schemamodel.HierarchyPolicy{Enabled: true, Acyclic: true, SingleParent: true, SameDomain: true}, nil
		}
		return nil, nil
	}
	matchedSchema := false
	for _, label := range labels {
		types, err := s.schemaManager.ResolveEdgeLabel(ctx, s.domainID, label)
		if errors.Is(err, schemaservice.ErrSchemaNotFound) {
			if hasEdgeLabel(labels, "contains") {
				return &schemamodel.HierarchyPolicy{Enabled: true, Acyclic: true, SingleParent: true, SameDomain: true}, nil
			}
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if len(types) > 0 {
			matchedSchema = true
		}
		for _, typ := range types {
			if typ.Hierarchy != nil && typ.Hierarchy.Enabled {
				policy := *typ.Hierarchy
				return &policy, nil
			}
		}
	}
	if matchedSchema {
		return nil, nil
	}
	if hasEdgeLabel(labels, "contains") {
		return &schemamodel.HierarchyPolicy{Enabled: true, Acyclic: true, SingleParent: true, SameDomain: true}, nil
	}
	return nil, nil
}

func containsPathWithPolicy(ctx context.Context, s *FileSession, edges []graph.Edge, from graph.NodeID, target graph.NodeID) (bool, error) {
	seen := map[graph.NodeID]struct{}{}
	queue := []graph.NodeID{from}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if id == target {
			return true, nil
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		for _, edge := range edges {
			policy, err := s.hierarchyPolicyForLabels(ctx, edge.Labels)
			if err != nil {
				return false, err
			}
			if policy != nil && edge.FromID == id {
				queue = append(queue, edge.ToID)
			}
		}
	}
	return false, nil
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
	return left.ID == right.ID && left.FromID == right.FromID && left.ToID == right.ToID && reflect.DeepEqual(left.Labels, right.Labels) && reflect.DeepEqual(left.Properties, right.Properties) && reflect.DeepEqual(left.Payload, right.Payload) && reflect.DeepEqual(left.Meta, right.Meta)
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
		node.Labels = append([]string(nil), node.Labels...)
		node.Properties = copyProps(node.Properties)
		node.Payload = copyProps(node.Payload)
		node.Meta = copyProps(node.Meta)
		node.Props = copyProps(node.Props)
		out = append(out, node)
	}
	return out
}

func cloneEdges(edges []graph.Edge) []graph.Edge {
	out := make([]graph.Edge, 0, len(edges))
	for _, edge := range edges {
		edge.Labels = append([]string(nil), edge.Labels...)
		edge.Properties = copyProps(edge.Properties)
		edge.Payload = copyProps(edge.Payload)
		edge.Meta = copyProps(edge.Meta)
		out = append(out, edge)
	}
	return out
}

func hasEdgeLabel(labels []string, label string) bool {
	for _, candidate := range labels {
		if candidate == label {
			return true
		}
	}
	return false
}

func safeID(id domainspace.SpaceID) string {
	repl := strings.NewReplacer(":", "_", "/", "_", "\\", "_")
	return repl.Replace(id.String())
}
