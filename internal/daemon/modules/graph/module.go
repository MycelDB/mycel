package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/graph/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

const childOrderStep = 1000

type Module struct {
	mu                     sync.Mutex
	dataDir                string
	stores                 map[string]*graphstorage.LocalStore
	overlays               map[string]*overlay
	changeSink             graphchange.Sink
	lastGraphChangeSinkErr error
	gate                   *quiesce.Gate
	wal                    *wal.Manager
	walProgress            wal.AppliedLSNStore
	walWaiter              *wal.ApplyWaiter
	writeAllowed           func() error
	raftGroups             *consensus.MultiGroup
	raftPartitionCount     uint32
	raftLocalNode          consensus.NodeID
	raftNodeAddrs          []string
	raftBackendAuthToken   string
	raftAppliedCommands    map[string]struct{}
}

type overlay struct {
	putNodes    map[domaingraph.NodeID]domaingraph.Node
	deleteNodes map[domaingraph.NodeID]struct{}
	putEdges    map[domaingraph.EdgeID]domaingraph.Edge
	deleteEdges map[domaingraph.EdgeID]struct{}
	opCount     int32
}

func NewModule() *Module {
	return &Module{stores: map[string]*graphstorage.LocalStore{}, overlays: map[string]*overlay{}, gate: quiesce.NewGate(ModuleName)}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) SetChangeSink(sink graphchange.Sink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changeSink = sink
}

func (m *Module) LastGraphChangeSinkError() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastGraphChangeSinkErr
}

func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult {
	m.dataDir = filepath.Join(rt.Config.DataDir, "graphs")
	if err := os.MkdirAll(m.dataDir, 0o700); err != nil {
		return daemonruntime.Abort(ModuleName, "storage", "create graph data directory", err)
	}
	if m.stores == nil {
		m.stores = map[string]*graphstorage.LocalStore{}
	}
	if m.overlays == nil {
		m.overlays = map[string]*overlay{}
	}
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	m.loadRaftAppliedCommands()
	m.wal = rt.WAL
	m.walProgress = rt.WALProgress
	m.walWaiter = rt.WALWaiter
	m.writeAllowed = rt.RequireLocalWriteAllowed
	if rt.WALRegistry != nil {
		if err := rt.WALRegistry.Register(recordTypeGraphCommit, wal.ApplierFunc(m.applyGraphCommit)); err != nil {
			return daemonruntime.Abort(ModuleName, "wal", "register graph commit WAL applier", err)
		}
	}
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if rt.Quiesce != nil {
		if err := rt.Quiesce.Register(m.gate); err != nil {
			return daemonruntime.Abort(ModuleName, "quiesce", "register graph quiesce participant", err)
		}
	}
	rt.Logger.Info("graph module initialized", "storage", "file", "path", m.dataDir)
	return daemonruntime.OK(ModuleName)
}

func (m *Module) GetNode(ctx context.Context, tx daemonsession.GraphTransaction, nodeID string) (domaingraph.Node, error) {
	if err := ensureReadable(tx); err != nil {
		return domaingraph.Node{}, err
	}
	id, err := parseUUID[domaingraph.NodeID](nodeID, "node_id")
	if err != nil {
		return domaingraph.Node{}, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphRead(tx.SpaceID); err != nil {
		return domaingraph.Node{}, err
	} else if forward {
		req := raftReadRequest("get_node", tx)
		req.ID = nodeID
		var res raftGraphNodeResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return domaingraph.Node{}, err
		}
		return res.Node, nil
	}
	return m.node(ctx, tx, id)
}

func (m *Module) ListNodes(ctx context.Context, tx daemonsession.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Node, string, error) {
	if err := ensureReadable(tx); err != nil {
		return nil, "", err
	}
	if leader, forward, err := m.shouldForwardRaftGraphRead(tx.SpaceID); err != nil {
		return nil, "", err
	} else if forward {
		req := raftReadRequest("list_nodes", tx)
		req.PageSize = pageSize
		req.PageToken = pageToken
		var res raftGraphNodesResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return nil, "", err
		}
		return res.Nodes, res.NextPageToken, nil
	}
	store, err := m.store(ctx, tx.SpaceID)
	if err != nil {
		return nil, "", err
	}
	base, err := store.ListNodesByDomain(ctx, mustDomainID(tx.DomainID))
	if err != nil {
		return nil, "", mapStorageError(err)
	}
	m.mu.Lock()
	o := m.overlays[tx.ID]
	merged := mergeNodes(base, o, mustDomainID(tx.DomainID))
	m.mu.Unlock()
	return paginateNodes(merged, pageSize, pageToken)
}

func (m *Module) CreateNode(ctx context.Context, tx daemonsession.GraphTransaction, input NodeInput) (domaingraph.Node, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Node{}, err
	}
	id, err := optionalUUID[domaingraph.NodeID](input.NodeID, "node_id")
	if err != nil {
		return domaingraph.Node{}, err
	}
	if id == uuid.Nil {
		id, err = graphstorage.NewUUIDv7()
		if err != nil {
			return domaingraph.Node{}, err
		}
	}
	if _, err := m.node(ctx, tx, id); err == nil {
		return domaingraph.Node{}, fmt.Errorf("%w: node already exists", ErrInvalidInput)
	} else if !errors.Is(err, ErrNotFound) {
		return domaingraph.Node{}, err
	}
	templateID, err := optionalTemplateID(input.TemplateID)
	if err != nil {
		return domaingraph.Node{}, err
	}
	now := time.Now().UTC()
	var blobRef *domaingraph.BlobID
	if strings.TrimSpace(input.BlobID) != "" {
		blobID := domaingraph.BlobID(strings.TrimSpace(input.BlobID))
		if _, err := blobID.Bytes(); err != nil {
			return domaingraph.Node{}, fmt.Errorf("%w: invalid blob_id", ErrInvalidInput)
		}
		if strings.TrimSpace(input.Content) != "" {
			return domaingraph.Node{}, fmt.Errorf("%w: blob nodes cannot have inline content", ErrInvalidInput)
		}
		blobRef = &blobID
	}
	n := domaingraph.Node{ID: id, DomainID: mustDomainID(tx.DomainID), TemplateID: templateID, BlobRef: blobRef, Content: input.Content, Props: cloneProps(input.Props), CreatedAt: now, UpdatedAt: now}
	m.stageNode(tx.ID, n)
	return cloneNode(n), nil
}

func (m *Module) UpdateNode(ctx context.Context, tx daemonsession.GraphTransaction, input UpdateNodeInput) (domaingraph.Node, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Node{}, err
	}
	id, err := parseUUID[domaingraph.NodeID](input.NodeID, "node_id")
	if err != nil {
		return domaingraph.Node{}, err
	}
	n, err := m.node(ctx, tx, id)
	if err != nil {
		return domaingraph.Node{}, err
	}
	paths := maskSet(input.UpdateMask)
	if input.TemplateID != nil && (len(paths) == 0 || paths["template_id"]) {
		templateID, err := optionalTemplateID(*input.TemplateID)
		if err != nil {
			return domaingraph.Node{}, err
		}
		n.TemplateID = templateID
	}
	if input.Content != nil && (len(paths) == 0 || paths["content"]) {
		if n.BlobRef != nil && *input.Content != "" {
			return domaingraph.Node{}, fmt.Errorf("%w: blob nodes cannot have inline content", ErrInvalidInput)
		}
		n.Content = *input.Content
	}
	if input.Props != nil && (len(paths) == 0 || paths["props"]) {
		n.Props = cloneProps(input.Props)
	}
	n.UpdatedAt = time.Now().UTC()
	m.stageNode(tx.ID, n)
	return cloneNode(n), nil
}

func (m *Module) UpsertNode(ctx context.Context, tx daemonsession.GraphTransaction, input NodeInput) (domaingraph.Node, error) {
	if strings.TrimSpace(input.NodeID) == "" {
		return m.CreateNode(ctx, tx, input)
	}
	id, err := parseUUID[domaingraph.NodeID](input.NodeID, "node_id")
	if err != nil {
		return domaingraph.Node{}, err
	}
	if _, err := m.node(ctx, tx, id); err == nil {
		content := input.Content
		tmpl := input.TemplateID
		return m.UpdateNode(ctx, tx, UpdateNodeInput{NodeID: input.NodeID, TemplateID: &tmpl, Content: &content, Props: input.Props})
	} else if !errors.Is(err, ErrNotFound) {
		return domaingraph.Node{}, err
	}
	return m.CreateNode(ctx, tx, input)
}

func (m *Module) DeleteNode(ctx context.Context, tx daemonsession.GraphTransaction, nodeID string, recursive bool) ([]string, []string, error) {
	if err := ensureWritable(tx); err != nil {
		return nil, nil, err
	}
	id, err := parseUUID[domaingraph.NodeID](nodeID, "node_id")
	if err != nil {
		return nil, nil, err
	}
	if _, err := m.node(ctx, tx, id); err != nil {
		return nil, nil, err
	}
	nodesToDelete := []domaingraph.NodeID{id}
	children, err := m.ListChildren(ctx, tx, id.String())
	if err != nil {
		return nil, nil, err
	}
	if len(children) > 0 && !recursive {
		return nil, nil, fmt.Errorf("%w: node has children; pass recursive=true", ErrInvalidState)
	}
	if recursive {
		for i := 0; i < len(nodesToDelete); i++ {
			kids, err := m.ListChildren(ctx, tx, nodesToDelete[i].String())
			if err != nil {
				return nil, nil, err
			}
			for _, edge := range kids {
				nodesToDelete = append(nodesToDelete, edge.ToID)
			}
		}
	}
	edges, _, err := m.ListEdges(ctx, tx, 0, "")
	if err != nil {
		return nil, nil, err
	}
	nodeSet := map[domaingraph.NodeID]struct{}{}
	for _, id := range nodesToDelete {
		nodeSet[id] = struct{}{}
	}
	deletedEdges := []string{}
	for _, edge := range edges {
		_, fromDeleted := nodeSet[edge.FromID]
		_, toDeleted := nodeSet[edge.ToID]
		if fromDeleted || toDeleted {
			m.stageEdgeDelete(tx.ID, edge.ID)
			deletedEdges = append(deletedEdges, edge.ID.String())
		}
	}
	deletedNodes := []string{}
	for _, id := range nodesToDelete {
		m.stageNodeDelete(tx.ID, id)
		deletedNodes = append(deletedNodes, id.String())
	}
	return deletedNodes, deletedEdges, nil
}

func (m *Module) GetEdge(ctx context.Context, tx daemonsession.GraphTransaction, edgeID string) (domaingraph.Edge, error) {
	if err := ensureReadable(tx); err != nil {
		return domaingraph.Edge{}, err
	}
	id, err := parseUUID[domaingraph.EdgeID](edgeID, "edge_id")
	if err != nil {
		return domaingraph.Edge{}, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphRead(tx.SpaceID); err != nil {
		return domaingraph.Edge{}, err
	} else if forward {
		req := raftReadRequest("get_edge", tx)
		req.ID = edgeID
		var res raftGraphEdgeResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return domaingraph.Edge{}, err
		}
		return res.Edge, nil
	}
	return m.edge(ctx, tx, id)
}

func (m *Module) ListEdges(ctx context.Context, tx daemonsession.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Edge, string, error) {
	if err := ensureReadable(tx); err != nil {
		return nil, "", err
	}
	if leader, forward, err := m.shouldForwardRaftGraphRead(tx.SpaceID); err != nil {
		return nil, "", err
	} else if forward {
		req := raftReadRequest("list_edges", tx)
		req.PageSize = pageSize
		req.PageToken = pageToken
		var res raftGraphEdgesResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return nil, "", err
		}
		return res.Edges, res.NextPageToken, nil
	}
	store, err := m.store(ctx, tx.SpaceID)
	if err != nil {
		return nil, "", err
	}
	base, err := store.ListEdges(ctx)
	if err != nil {
		return nil, "", mapStorageError(err)
	}
	m.mu.Lock()
	o := m.overlays[tx.ID]
	merged := mergeEdges(base, o)
	m.mu.Unlock()
	return paginateEdges(merged, pageSize, pageToken)
}

func (m *Module) CreateEdge(ctx context.Context, tx daemonsession.GraphTransaction, input EdgeInput) (domaingraph.Edge, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Edge{}, err
	}
	id, err := optionalUUID[domaingraph.EdgeID](input.EdgeID, "edge_id")
	if err != nil {
		return domaingraph.Edge{}, err
	}
	if id == uuid.Nil {
		id, err = graphstorage.NewUUIDv7()
		if err != nil {
			return domaingraph.Edge{}, err
		}
	}
	fromID, err := parseUUID[domaingraph.NodeID](input.FromNodeID, "from_node_id")
	if err != nil {
		return domaingraph.Edge{}, err
	}
	toID, err := parseUUID[domaingraph.NodeID](input.ToNodeID, "to_node_id")
	if err != nil {
		return domaingraph.Edge{}, err
	}
	kind := domaingraph.EdgeKind(strings.TrimSpace(input.Kind))
	if kind == "" {
		return domaingraph.Edge{}, fmt.Errorf("%w: edge kind is required", ErrInvalidInput)
	}
	if _, err := m.node(ctx, tx, fromID); err != nil {
		return domaingraph.Edge{}, fmt.Errorf("%w: from node: %v", ErrInvalidInput, err)
	}
	if _, err := m.node(ctx, tx, toID); err != nil {
		return domaingraph.Edge{}, fmt.Errorf("%w: to node: %v", ErrInvalidInput, err)
	}
	if _, err := m.edge(ctx, tx, id); err == nil {
		return domaingraph.Edge{}, fmt.Errorf("%w: edge already exists", ErrInvalidInput)
	} else if !errors.Is(err, ErrNotFound) {
		return domaingraph.Edge{}, err
	}
	if kind == domaingraph.EdgeKindContains {
		if existing, err := m.parentEdge(ctx, tx, toID); err != nil {
			return domaingraph.Edge{}, err
		} else if existing != nil {
			return domaingraph.Edge{}, fmt.Errorf("%w: child already has a contains parent", ErrInvalidInput)
		}
	}
	e := domaingraph.Edge{ID: id, FromID: fromID, ToID: toID, Kind: kind, Props: cloneProps(input.Props)}
	m.stageEdge(tx.ID, e)
	return cloneEdge(e), nil
}

func (m *Module) UpdateEdge(ctx context.Context, tx daemonsession.GraphTransaction, input UpdateEdgeInput) (domaingraph.Edge, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Edge{}, err
	}
	id, err := parseUUID[domaingraph.EdgeID](input.EdgeID, "edge_id")
	if err != nil {
		return domaingraph.Edge{}, err
	}
	e, err := m.edge(ctx, tx, id)
	if err != nil {
		return domaingraph.Edge{}, err
	}
	paths := maskSet(input.UpdateMask)
	if input.Kind != nil && (len(paths) == 0 || paths["kind"]) {
		e.Kind = domaingraph.EdgeKind(strings.TrimSpace(*input.Kind))
	}
	if input.Props != nil && (len(paths) == 0 || paths["props"]) {
		e.Props = cloneProps(input.Props)
	}
	m.stageEdge(tx.ID, e)
	return cloneEdge(e), nil
}

func (m *Module) DeleteEdge(ctx context.Context, tx daemonsession.GraphTransaction, edgeID string) (string, error) {
	if err := ensureWritable(tx); err != nil {
		return "", err
	}
	id, err := parseUUID[domaingraph.EdgeID](edgeID, "edge_id")
	if err != nil {
		return "", err
	}
	if _, err := m.edge(ctx, tx, id); err != nil {
		return "", err
	}
	m.stageEdgeDelete(tx.ID, id)
	return id.String(), nil
}

func (m *Module) ListChildren(ctx context.Context, tx daemonsession.GraphTransaction, parentNodeID string) ([]domaingraph.Edge, error) {
	parentID, err := parseUUID[domaingraph.NodeID](parentNodeID, "parent_node_id")
	if err != nil {
		return nil, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphRead(tx.SpaceID); err != nil {
		return nil, err
	} else if forward {
		req := raftReadRequest("list_children", tx)
		req.ID = parentNodeID
		var res raftGraphEdgesResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return nil, err
		}
		return res.Edges, nil
	}
	edges, _, err := m.ListEdges(ctx, tx, 0, "")
	if err != nil {
		return nil, err
	}
	out := []domaingraph.Edge{}
	for _, edge := range edges {
		if edge.Kind == domaingraph.EdgeKindContains && edge.FromID == parentID {
			out = append(out, edge)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return edgeOrder(out[i], i) < edgeOrder(out[j], j) })
	return out, nil
}

func (m *Module) GetParent(ctx context.Context, tx daemonsession.GraphTransaction, childNodeID string) (*domaingraph.Edge, error) {
	childID, err := parseUUID[domaingraph.NodeID](childNodeID, "child_node_id")
	if err != nil {
		return nil, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphRead(tx.SpaceID); err != nil {
		return nil, err
	} else if forward {
		req := raftReadRequest("get_parent", tx)
		req.ID = childNodeID
		var res raftGraphOptionalEdgeResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return nil, err
		}
		return res.Edge, nil
	}
	return m.parentEdge(ctx, tx, childID)
}

func (m *Module) MoveSubtree(ctx context.Context, tx daemonsession.GraphTransaction, nodeID string, newParentNodeID string, order *int32) (domaingraph.Edge, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Edge{}, err
	}
	childID, err := parseUUID[domaingraph.NodeID](nodeID, "node_id")
	if err != nil {
		return domaingraph.Edge{}, err
	}
	parentID, err := parseUUID[domaingraph.NodeID](newParentNodeID, "new_parent_node_id")
	if err != nil {
		return domaingraph.Edge{}, err
	}
	if childID == parentID || m.isDescendant(ctx, tx, parentID, childID) {
		return domaingraph.Edge{}, fmt.Errorf("%w: move would create a cycle", ErrInvalidInput)
	}
	if _, err := m.node(ctx, tx, childID); err != nil {
		return domaingraph.Edge{}, err
	}
	if _, err := m.node(ctx, tx, parentID); err != nil {
		return domaingraph.Edge{}, err
	}
	props := map[string]any{}
	edgeID := domaingraph.EdgeID(uuid.Nil)
	if existing, err := m.parentEdge(ctx, tx, childID); err != nil {
		return domaingraph.Edge{}, err
	} else if existing != nil {
		edgeID = existing.ID
		props = cloneProps(existing.Props)
		m.stageEdgeDelete(tx.ID, existing.ID)
	}
	if edgeID == uuid.Nil {
		newID, err := graphstorage.NewUUIDv7()
		if err != nil {
			return domaingraph.Edge{}, err
		}
		edgeID = newID
	}
	if order != nil {
		props["order"] = int(*order)
	} else if _, ok := props["order"]; !ok {
		children, _ := m.ListChildren(ctx, tx, parentID.String())
		props["order"] = len(children) * childOrderStep
	}
	e := domaingraph.Edge{ID: edgeID, FromID: parentID, ToID: childID, Kind: domaingraph.EdgeKindContains, Props: props}
	m.stageEdge(tx.ID, e)
	return cloneEdge(e), nil
}

func (m *Module) ReorderChildren(ctx context.Context, tx daemonsession.GraphTransaction, parentNodeID string, childNodeIDs []string) ([]domaingraph.Edge, error) {
	if err := ensureWritable(tx); err != nil {
		return nil, err
	}
	children, err := m.ListChildren(ctx, tx, parentNodeID)
	if err != nil {
		return nil, err
	}
	byChild := map[string]domaingraph.Edge{}
	for _, edge := range children {
		byChild[edge.ToID.String()] = edge
	}
	if len(byChild) != len(childNodeIDs) {
		return nil, fmt.Errorf("%w: child_node_ids must include every existing child exactly once", ErrInvalidInput)
	}
	out := []domaingraph.Edge{}
	for i, childID := range childNodeIDs {
		edge, ok := byChild[childID]
		if !ok {
			return nil, fmt.Errorf("%w: child_node_ids must include every existing child exactly once", ErrInvalidInput)
		}
		edge.Props = cloneProps(edge.Props)
		edge.Props["order"] = i * childOrderStep
		m.stageEdge(tx.ID, edge)
		out = append(out, cloneEdge(edge))
	}
	return out, nil
}

func (m *Module) CurrentRevision(ctx context.Context, spaceID string) (int64, error) {
	store, err := m.store(ctx, spaceID)
	if err != nil {
		return 0, err
	}
	return int64(store.Revision()), nil
}

func (m *Module) CommitTransactionGraph(ctx context.Context, tx daemonsession.GraphTransaction) (CommitResult, error) {
	if tx.Mode == daemonsession.TransactionModeReadOnly {
		return CommitResult{}, nil
	}
	release, err := m.enterWrite(ctx)
	if err != nil {
		return CommitResult{}, err
	}
	defer release()
	m.mu.Lock()
	o := m.overlays[tx.ID]
	if o == nil || o.opCount == 0 {
		delete(m.overlays, tx.ID)
		m.mu.Unlock()
		return CommitResult{}, nil
	}
	snapshot := o.clone()
	delete(m.overlays, tx.ID)
	m.mu.Unlock()
	store, err := m.store(ctx, tx.SpaceID)
	if err != nil {
		return CommitResult{}, err
	}
	changes, err := m.overlayChanges(ctx, store, snapshot)
	if err != nil {
		return CommitResult{}, err
	}
	graphEvent, err := m.graphChangeEvent(ctx, tx, store, snapshot)
	if err != nil {
		return CommitResult{}, err
	}
	record := graphCommitRecordFromSnapshot(tx, snapshot)
	var committedRevision int64
	var info graphstorage.CommitInfo
	if m.raftGroups != nil {
		cmd, err := m.buildGraphCommitRaftCommand(record, m.raftPartitionCount, graphRaftCommandID(ctx, tx.ID))
		if err != nil {
			return CommitResult{}, err
		}
		if err := m.proposeGraphRaftCommand(ctx, cmd); err != nil {
			return CommitResult{}, err
		}
		committedRevision, err = m.CurrentRevision(ctx, tx.SpaceID)
		if err != nil {
			return CommitResult{}, err
		}
		info = graphstorage.CommitInfo{TxnID: uuid.New(), NextRevision: uint64(committedRevision)}
	} else if m.wal == nil {
		storageTx, err := store.Begin(ctx)
		if err != nil {
			return CommitResult{}, mapStorageError(err)
		}
		storageTx.ExpectRevision(uint64(tx.BaseRevision))
		for _, node := range record.PutNodes {
			if err := storageTx.PutNode(node); err != nil {
				_ = storageTx.Rollback()
				return CommitResult{}, mapStorageError(err)
			}
		}
		for _, edge := range record.PutEdges {
			if err := storageTx.PutEdge(edge); err != nil {
				_ = storageTx.Rollback()
				return CommitResult{}, mapStorageError(err)
			}
		}
		for _, id := range record.DeleteNodeIDs {
			if err := storageTx.DeleteNode(id); err != nil {
				_ = storageTx.Rollback()
				return CommitResult{}, mapStorageError(err)
			}
		}
		for _, id := range record.DeleteEdgeIDs {
			if err := storageTx.DeleteEdge(id); err != nil {
				_ = storageTx.Rollback()
				return CommitResult{}, mapStorageError(err)
			}
		}
		info, err = storageTx.CommitWithInfo()
		if err != nil {
			return CommitResult{}, mapStorageError(err)
		}
		committedRevision = int64(store.Revision())
	} else {
		payload, err := json.Marshal(record)
		if err != nil {
			return CommitResult{}, err
		}
		lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeGraphCommit, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
		if err != nil {
			return CommitResult{}, err
		}
		if err := m.wal.Sync(ctx, lsn); err != nil {
			return CommitResult{}, err
		}
		committedRevision, _, err = m.applyGraphCommitRecord(ctx, record)
		if err != nil {
			return CommitResult{}, err
		}
		if err := m.markWALApplied(ctx, lsn); err != nil {
			return CommitResult{}, err
		}
		info = graphstorage.CommitInfo{TxnID: uuid.New(), NextRevision: uint64(committedRevision)}
	}
	m.notifyGraphChangeSink(ctx, info, graphEvent)
	return CommitResult{OperationCount: snapshot.opCount, CommittedRevision: committedRevision, Changes: changes}, nil
}

func (m *Module) enterWrite(ctx context.Context) (func(), error) {
	if m.raftGroups == nil {
		if err := m.requireLocalWriteAllowed(); err != nil {
			return nil, err
		}
	}
	if m.gate == nil {
		return func() {}, nil
	}
	release, err := m.gate.Enter(ctx)
	if err != nil {
		return nil, quiesce.GRPCError(err)
	}
	return release, nil
}

func (m *Module) requireLocalWriteAllowed() error {
	if m.writeAllowed == nil {
		return nil
	}
	return m.writeAllowed()
}

func (m *Module) notifyGraphChangeSink(ctx context.Context, info graphstorage.CommitInfo, event graphchange.CommittedEvent) {
	if event.Empty() {
		return
	}
	m.mu.Lock()
	sink := m.changeSink
	m.mu.Unlock()
	if sink == nil {
		return
	}
	event.TxnID = info.TxnID
	event.GraphRevision = info.NextRevision
	if err := sink.OnGraphCommitted(ctx, event); err != nil {
		m.mu.Lock()
		m.lastGraphChangeSinkErr = err
		m.mu.Unlock()
		return
	}
	m.mu.Lock()
	m.lastGraphChangeSinkErr = nil
	m.mu.Unlock()
}

func (m *Module) graphChangeEvent(ctx context.Context, tx daemonsession.GraphTransaction, store *graphstorage.LocalStore, snapshot *overlay) (graphchange.CommittedEvent, error) {
	spaceID, err := uuid.Parse(tx.SpaceID)
	if err != nil || spaceID == uuid.Nil {
		return graphchange.CommittedEvent{}, fmt.Errorf("%w: space_id must be a UUID", ErrInvalidInput)
	}
	domainID := mustDomainID(tx.DomainID)
	event := graphchange.CommittedEvent{
		SpaceID:           domainspace.SpaceID(spaceID),
		DomainIDs:         []domaingraph.DomainID{domainID},
		CreatedNodeIDs:    []domaingraph.NodeID{},
		UpdatedNodeIDs:    []domaingraph.NodeID{},
		DeletedNodeIDs:    []domaingraph.NodeID{},
		ChangedEdges:      []graphchange.EdgeChange{},
		OldParentByNodeID: map[domaingraph.NodeID]domaingraph.NodeID{},
		NewParentByNodeID: map[domaingraph.NodeID]domaingraph.NodeID{},
		OldDomainByNodeID: map[domaingraph.NodeID]domaingraph.DomainID{},
		NewDomainByNodeID: map[domaingraph.NodeID]domaingraph.DomainID{},
		CommittedAt:       time.Now().UTC(),
	}
	for _, node := range sortedNodes(snapshot.putNodes) {
		old, err := store.GetNode(ctx, node.ID)
		if err == nil {
			event.UpdatedNodeIDs = append(event.UpdatedNodeIDs, node.ID)
			event.OldDomainByNodeID[node.ID] = old.DomainID
		} else if errors.Is(err, graphstorage.ErrNotFound) {
			event.CreatedNodeIDs = append(event.CreatedNodeIDs, node.ID)
		} else {
			return graphchange.CommittedEvent{}, mapStorageError(err)
		}
		event.NewDomainByNodeID[node.ID] = node.DomainID
		if parent, err := store.Parent(ctx, node.ID); err == nil && parent != nil {
			event.OldParentByNodeID[node.ID] = parent.FromID
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return graphchange.CommittedEvent{}, mapStorageError(err)
		}
	}
	for _, id := range sortedNodeIDs(snapshot.deleteNodes) {
		event.DeletedNodeIDs = append(event.DeletedNodeIDs, id)
		old, err := store.GetNode(ctx, id)
		if err == nil {
			event.OldDomainByNodeID[id] = old.DomainID
		} else if !errors.Is(err, graphstorage.ErrNotFound) {
			return graphchange.CommittedEvent{}, mapStorageError(err)
		}
		if parent, err := store.Parent(ctx, id); err == nil && parent != nil {
			event.OldParentByNodeID[id] = parent.FromID
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return graphchange.CommittedEvent{}, mapStorageError(err)
		}
	}
	for _, edge := range sortedEdges(snapshot.putEdges) {
		change := "added"
		if old, err := store.GetEdge(ctx, edge.ID); err == nil {
			change = "updated"
			if old.Kind == domaingraph.EdgeKindContains && old.ToID == edge.ToID && old.FromID != edge.FromID {
				event.OldParentByNodeID[old.ToID] = old.FromID
			}
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return graphchange.CommittedEvent{}, mapStorageError(err)
		}
		if edge.Kind == domaingraph.EdgeKindContains {
			event.NewParentByNodeID[edge.ToID] = edge.FromID
		}
		event.ChangedEdges = append(event.ChangedEdges, graphchange.EdgeChange{EdgeID: edge.ID, Kind: edge.Kind, Change: change, FromID: edge.FromID, ToID: edge.ToID})
	}
	for _, id := range sortedEdgeIDs(snapshot.deleteEdges) {
		edge, err := store.GetEdge(ctx, id)
		if err != nil {
			if errors.Is(err, graphstorage.ErrNotFound) {
				continue
			}
			return graphchange.CommittedEvent{}, mapStorageError(err)
		}
		if edge.Kind == domaingraph.EdgeKindContains {
			event.OldParentByNodeID[edge.ToID] = edge.FromID
		}
		event.ChangedEdges = append(event.ChangedEdges, graphchange.EdgeChange{EdgeID: edge.ID, Kind: edge.Kind, Change: "removed", FromID: edge.FromID, ToID: edge.ToID})
	}
	return event, nil
}

func (m *Module) overlayChanges(ctx context.Context, store *graphstorage.LocalStore, snapshot *overlay) ([]GraphChange, error) {
	changes := []GraphChange{}
	for _, node := range sortedNodes(snapshot.putNodes) {
		changeType := ChangeTypeNodeUpdated
		if _, err := store.GetNode(ctx, node.ID); errors.Is(err, graphstorage.ErrNotFound) {
			changeType = ChangeTypeNodeCreated
		} else if err != nil {
			return nil, mapStorageError(err)
		}
		copy := cloneNode(node)
		changes = append(changes, GraphChange{Type: changeType, Node: &copy, NodeID: node.ID.String()})
	}
	for _, edge := range sortedEdges(snapshot.putEdges) {
		changeType := ChangeTypeEdgeUpdated
		if _, err := store.GetEdge(ctx, edge.ID); errors.Is(err, graphstorage.ErrNotFound) {
			changeType = ChangeTypeEdgeCreated
		} else if err != nil {
			return nil, mapStorageError(err)
		}
		copy := cloneEdge(edge)
		changes = append(changes, GraphChange{Type: changeType, Edge: &copy, EdgeID: edge.ID.String()})
	}
	for _, id := range sortedNodeIDs(snapshot.deleteNodes) {
		changes = append(changes, GraphChange{Type: ChangeTypeNodeDeleted, NodeID: id.String()})
	}
	for _, id := range sortedEdgeIDs(snapshot.deleteEdges) {
		changes = append(changes, GraphChange{Type: ChangeTypeEdgeDeleted, EdgeID: id.String()})
	}
	return changes, nil
}

func (m *Module) DiscardTransactionGraph(ctx context.Context, transactionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.overlays, transactionID)
}

func (m *Module) BlobRefCount(ctx context.Context, spaceID string, blobID string) (int, error) {
	id := domaingraph.BlobID(strings.TrimSpace(blobID))
	if _, err := id.Bytes(); err != nil {
		return 0, fmt.Errorf("%w: invalid blob_id", ErrInvalidInput)
	}
	store, err := m.store(ctx, spaceID)
	if err != nil {
		return 0, err
	}
	count, err := store.BlobRefCount(ctx, id)
	if err != nil {
		return 0, mapStorageError(err)
	}
	return count, nil
}

func (m *Module) store(ctx context.Context, spaceID string) (*graphstorage.LocalStore, error) {
	if strings.TrimSpace(spaceID) == "" {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if store := m.stores[spaceID]; store != nil {
		return store, nil
	}
	store, err := graphstorage.Open(ctx, filepath.Join(m.dataDir, spaceID))
	if err != nil {
		return nil, err
	}
	m.stores[spaceID] = store
	return store, nil
}

func (m *Module) node(ctx context.Context, tx daemonsession.GraphTransaction, id domaingraph.NodeID) (domaingraph.Node, error) {
	m.mu.Lock()
	o := m.overlays[tx.ID]
	if o != nil {
		if _, deleted := o.deleteNodes[id]; deleted {
			m.mu.Unlock()
			return domaingraph.Node{}, ErrNotFound
		}
		if node, ok := o.putNodes[id]; ok {
			m.mu.Unlock()
			return cloneNode(node), nil
		}
	}
	m.mu.Unlock()
	store, err := m.store(ctx, tx.SpaceID)
	if err != nil {
		return domaingraph.Node{}, err
	}
	node, err := store.GetNode(ctx, id)
	if err != nil {
		return domaingraph.Node{}, mapStorageError(err)
	}
	if node.DomainID != mustDomainID(tx.DomainID) {
		return domaingraph.Node{}, ErrNotFound
	}
	return cloneNode(node), nil
}

func (m *Module) edge(ctx context.Context, tx daemonsession.GraphTransaction, id domaingraph.EdgeID) (domaingraph.Edge, error) {
	m.mu.Lock()
	o := m.overlays[tx.ID]
	if o != nil {
		if _, deleted := o.deleteEdges[id]; deleted {
			m.mu.Unlock()
			return domaingraph.Edge{}, ErrNotFound
		}
		if edge, ok := o.putEdges[id]; ok {
			m.mu.Unlock()
			return cloneEdge(edge), nil
		}
	}
	m.mu.Unlock()
	store, err := m.store(ctx, tx.SpaceID)
	if err != nil {
		return domaingraph.Edge{}, err
	}
	edge, err := store.GetEdge(ctx, id)
	if err != nil {
		return domaingraph.Edge{}, mapStorageError(err)
	}
	return cloneEdge(edge), nil
}

func (m *Module) parentEdge(ctx context.Context, tx daemonsession.GraphTransaction, childID domaingraph.NodeID) (*domaingraph.Edge, error) {
	edges, _, err := m.ListEdges(ctx, tx, 0, "")
	if err != nil {
		return nil, err
	}
	for _, edge := range edges {
		if edge.Kind == domaingraph.EdgeKindContains && edge.ToID == childID {
			copy := cloneEdge(edge)
			return &copy, nil
		}
	}
	return nil, nil
}

func (m *Module) stageNode(txID string, node domaingraph.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(txID)
	delete(o.deleteNodes, node.ID)
	o.putNodes[node.ID] = cloneNode(node)
	o.opCount++
}

func (m *Module) stageNodeDelete(txID string, id domaingraph.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(txID)
	delete(o.putNodes, id)
	o.deleteNodes[id] = struct{}{}
	o.opCount++
}

func (m *Module) stageEdge(txID string, edge domaingraph.Edge) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(txID)
	delete(o.deleteEdges, edge.ID)
	o.putEdges[edge.ID] = cloneEdge(edge)
	o.opCount++
}

func (m *Module) stageEdgeDelete(txID string, id domaingraph.EdgeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(txID)
	delete(o.putEdges, id)
	o.deleteEdges[id] = struct{}{}
	o.opCount++
}

func (m *Module) overlay(txID string) *overlay {
	o := m.overlays[txID]
	if o == nil {
		o = &overlay{putNodes: map[domaingraph.NodeID]domaingraph.Node{}, deleteNodes: map[domaingraph.NodeID]struct{}{}, putEdges: map[domaingraph.EdgeID]domaingraph.Edge{}, deleteEdges: map[domaingraph.EdgeID]struct{}{}}
		m.overlays[txID] = o
	}
	return o
}

func (o *overlay) clone() *overlay {
	out := &overlay{putNodes: map[domaingraph.NodeID]domaingraph.Node{}, deleteNodes: map[domaingraph.NodeID]struct{}{}, putEdges: map[domaingraph.EdgeID]domaingraph.Edge{}, deleteEdges: map[domaingraph.EdgeID]struct{}{}, opCount: o.opCount}
	for id, node := range o.putNodes {
		out.putNodes[id] = cloneNode(node)
	}
	for id := range o.deleteNodes {
		out.deleteNodes[id] = struct{}{}
	}
	for id, edge := range o.putEdges {
		out.putEdges[id] = cloneEdge(edge)
	}
	for id := range o.deleteEdges {
		out.deleteEdges[id] = struct{}{}
	}
	return out
}

func ensureReadable(tx daemonsession.GraphTransaction) error {
	if tx.State != daemonsession.TransactionStateActive {
		return ErrInvalidState
	}
	return nil
}

func ensureWritable(tx daemonsession.GraphTransaction) error {
	if err := ensureReadable(tx); err != nil {
		return err
	}
	if tx.Mode != daemonsession.TransactionModeReadWrite {
		return ErrReadOnly
	}
	return nil
}

func mapStorageError(err error) error {
	if errors.Is(err, graphstorage.ErrNotFound) {
		return ErrNotFound
	}
	if errors.Is(err, graphstorage.ErrConflict) {
		return ErrConflict
	}
	if errors.Is(err, graphstorage.ErrTxnClosed) || errors.Is(err, graphstorage.ErrClosed) {
		return ErrInvalidState
	}
	return err
}

func parseUUID[T ~[16]byte](value string, name string) (T, error) {
	id, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil {
		var zero T
		return zero, fmt.Errorf("%w: %s must be a UUID", ErrInvalidInput, name)
	}
	return T(id), nil
}

func optionalUUID[T ~[16]byte](value string, name string) (T, error) {
	if strings.TrimSpace(value) == "" {
		var zero T
		return zero, nil
	}
	return parseUUID[T](value, name)
}

func optionalTemplateID(value string) (*domaingraph.TemplateID, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	id, err := parseUUID[domaingraph.TemplateID](value, "template_id")
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func mustDomainID(value string) domaingraph.DomainID {
	id, _ := uuid.Parse(value)
	return domaingraph.DomainID(id)
}

func cloneNode(n domaingraph.Node) domaingraph.Node { n.Props = cloneProps(n.Props); return n }
func cloneEdge(e domaingraph.Edge) domaingraph.Edge { e.Props = cloneProps(e.Props); return e }
func cloneProps(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mergeNodes(base []domaingraph.Node, o *overlay, domainID domaingraph.DomainID) []domaingraph.Node {
	byID := map[domaingraph.NodeID]domaingraph.Node{}
	for _, node := range base {
		if node.DomainID == domainID {
			byID[node.ID] = cloneNode(node)
		}
	}
	if o != nil {
		for id := range o.deleteNodes {
			delete(byID, id)
		}
		for id, node := range o.putNodes {
			if node.DomainID == domainID {
				byID[id] = cloneNode(node)
			}
		}
	}
	out := make([]domaingraph.Node, 0, len(byID))
	for _, node := range byID {
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out
}

func mergeEdges(base []domaingraph.Edge, o *overlay) []domaingraph.Edge {
	byID := map[domaingraph.EdgeID]domaingraph.Edge{}
	for _, edge := range base {
		byID[edge.ID] = cloneEdge(edge)
	}
	if o != nil {
		for id := range o.deleteEdges {
			delete(byID, id)
		}
		for id, edge := range o.putEdges {
			byID[id] = cloneEdge(edge)
		}
	}
	out := make([]domaingraph.Edge, 0, len(byID))
	for _, edge := range byID {
		out = append(out, edge)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out
}

func paginateNodes(nodes []domaingraph.Node, pageSize int, pageToken string) ([]domaingraph.Node, string, error) {
	start, err := pageStart(pageToken)
	if err != nil {
		return nil, "", err
	}
	if pageSize <= 0 || pageSize > 500 {
		pageSize = 500
	}
	if start >= len(nodes) {
		return []domaingraph.Node{}, "", nil
	}
	end := start + pageSize
	if end > len(nodes) {
		end = len(nodes)
	}
	next := ""
	if end < len(nodes) {
		next = strconv.Itoa(end)
	}
	return nodes[start:end], next, nil
}

func paginateEdges(edges []domaingraph.Edge, pageSize int, pageToken string) ([]domaingraph.Edge, string, error) {
	start, err := pageStart(pageToken)
	if err != nil {
		return nil, "", err
	}
	if pageSize <= 0 || pageSize > 500 {
		pageSize = 500
	}
	if start >= len(edges) {
		return []domaingraph.Edge{}, "", nil
	}
	end := start + pageSize
	if end > len(edges) {
		end = len(edges)
	}
	next := ""
	if end < len(edges) {
		next = strconv.Itoa(end)
	}
	return edges[start:end], next, nil
}

func pageStart(pageToken string) (int, error) {
	if strings.TrimSpace(pageToken) == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(pageToken)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%w: invalid page_token", ErrInvalidInput)
	}
	return value, nil
}

func maskSet(paths []string) map[string]bool {
	out := map[string]bool{}
	for _, path := range paths {
		out[path] = true
	}
	return out
}

func edgeOrder(edge domaingraph.Edge, fallback int) int {
	if value, ok := intProp(edge.Props["order"]); ok {
		return value
	}
	return fallback * childOrderStep
}

func intProp(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case float32:
		return int(v), true
	default:
		return 0, false
	}
}

func (m *Module) isDescendant(ctx context.Context, tx daemonsession.GraphTransaction, candidate domaingraph.NodeID, ancestor domaingraph.NodeID) bool {
	children, err := m.ListChildren(ctx, tx, ancestor.String())
	if err != nil {
		return false
	}
	for _, child := range children {
		if child.ToID == candidate || m.isDescendant(ctx, tx, candidate, child.ToID) {
			return true
		}
	}
	return false
}

func sortedNodes(in map[domaingraph.NodeID]domaingraph.Node) []domaingraph.Node {
	out := make([]domaingraph.Node, 0, len(in))
	for _, node := range in {
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out
}
func sortedEdges(in map[domaingraph.EdgeID]domaingraph.Edge) []domaingraph.Edge {
	out := make([]domaingraph.Edge, 0, len(in))
	for _, edge := range in {
		out = append(out, edge)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out
}
func sortedNodeIDs(in map[domaingraph.NodeID]struct{}) []domaingraph.NodeID {
	out := make([]domaingraph.NodeID, 0, len(in))
	for id := range in {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
func sortedEdgeIDs(in map[domaingraph.EdgeID]struct{}) []domaingraph.EdgeID {
	out := make([]domaingraph.EdgeID, 0, len(in))
	for id := range in {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
