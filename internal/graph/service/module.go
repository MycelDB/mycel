package service

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
	"github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/graph/storage"
	runtime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const childOrderStep = 1000

type Module struct {
	mu                     sync.Mutex
	dataDir                string
	stores                 map[string]*graphstorage.LocalStore
	overlays               map[string]*overlay
	changeSink             graphchange.Sink
	schemaManager          schemaservice.Manager
	blobRefs               BlobReferenceChecker
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

type BlobReferenceChecker interface {
	EnsureBlobReference(ctx context.Context, spaceID string, blobID string) error
}

func (m *Module) SetSchemaManager(manager schemaservice.Manager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schemaManager = manager
}

func (m *Module) SetBlobReferenceChecker(checker BlobReferenceChecker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobRefs = checker
}

func (m *Module) validateBlobReferences(ctx context.Context, spaceID string, nodes []domaingraph.Node) error {
	m.mu.Lock()
	checker := m.blobRefs
	m.mu.Unlock()
	if checker == nil {
		return nil
	}
	seen := map[domaingraph.BlobID]struct{}{}
	for _, node := range nodes {
		if node.BlobRef == nil || strings.TrimSpace(string(*node.BlobRef)) == "" {
			continue
		}
		if _, ok := seen[*node.BlobRef]; ok {
			continue
		}
		seen[*node.BlobRef] = struct{}{}
		if err := checker.EnsureBlobReference(ctx, spaceID, string(*node.BlobRef)); err != nil {
			return status.Errorf(codes.FailedPrecondition, "blob reference %s is not available cluster-wide: %v", *node.BlobRef, err)
		}
	}
	return nil
}

func (m *Module) validateSchemaNode(ctx context.Context, node domaingraph.Node) error {
	m.mu.Lock()
	manager := m.schemaManager
	m.mu.Unlock()
	if manager == nil {
		return nil
	}
	result, err := manager.ValidateNode(ctx, node.DomainID, node)
	if err != nil {
		return err
	}
	if result.Valid() {
		return nil
	}
	return fmt.Errorf("%w: schema validation failed: %s", ErrInvalidInput, formatSchemaIssues(result.Issues))
}

func (m *Module) hierarchyEdgeLabelsForMutation(ctx context.Context, domainID domaingraph.DomainID) ([]string, error) {
	m.mu.Lock()
	manager := m.schemaManager
	m.mu.Unlock()
	if manager != nil {
		schema, err := manager.GetDomainSchema(ctx, domainID)
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

func (m *Module) hierarchyPolicyForLabels(ctx context.Context, domainID domaingraph.DomainID, labels []string) (*schemamodel.HierarchyPolicy, error) {
	m.mu.Lock()
	manager := m.schemaManager
	m.mu.Unlock()
	if manager == nil {
		if hasEdgeLabel(labels, "contains") {
			return &schemamodel.HierarchyPolicy{Enabled: true, Acyclic: true, SingleParent: true, SameDomain: true}, nil
		}
		return nil, nil
	}
	matchedSchema := false
	for _, label := range labels {
		types, err := manager.ResolveEdgeLabel(ctx, domainID, label)
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

func (m *Module) validateSchemaEdge(ctx context.Context, edge domaingraph.Edge, from domaingraph.Node, to domaingraph.Node) error {
	m.mu.Lock()
	manager := m.schemaManager
	m.mu.Unlock()
	if manager == nil {
		return nil
	}
	result, err := manager.ValidateEdge(ctx, edge.DomainID, edge, from, to)
	if err != nil {
		return err
	}
	if result.Valid() {
		return nil
	}
	return fmt.Errorf("%w: schema validation failed: %s", ErrInvalidInput, formatSchemaIssues(result.Issues))
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

func (m *Module) Init(ctx context.Context, host runtime.Host) runtime.InitResult {
	m.dataDir = filepath.Join(host.DataDir(), "graphs")
	if err := os.MkdirAll(m.dataDir, 0o700); err != nil {
		return runtime.Abort(ModuleName, "storage", "create graph data directory", err)
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
	if lookup, ok := host.(runtime.ServiceLookup); ok {
		if schemaSvc, ok := lookup.Service(schemaservice.ModuleName); ok {
			if manager, ok := schemaSvc.(schemaservice.Manager); ok {
				m.schemaManager = manager
			}
		}
	}
	if provider, ok := host.(runtime.WALProvider); ok {
		m.wal = provider.WALManager()
		m.walProgress = provider.WALProgressStore()
		m.walWaiter = provider.WALWaiterStore()
	}
	m.writeAllowed = func() error { return nil }
	if gate, ok := host.(runtime.LocalWriteGate); ok {
		m.writeAllowed = gate.RequireLocalWriteAllowed
	}
	if provider, ok := host.(runtime.WALProvider); ok {
		if registry := provider.WALRegistryStore(); registry != nil {
			if err := registry.Register(recordTypeGraphCommit, wal.ApplierFunc(m.applyGraphCommit)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register graph commit WAL applier", err)
			}
		}
	}
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if _, ok := host.(runtime.QuiesceRegistrar); ok {
		if err := host.(runtime.QuiesceRegistrar).RegisterQuiesceParticipant(m.gate); err != nil {
			return runtime.Abort(ModuleName, "quiesce", "register graph quiesce participant", err)
		}
	}
	if logger := host.Log(); logger != nil {
		logger.Info("graph module initialized", "storage", "file", "path", m.dataDir)
	}
	return runtime.OK(ModuleName)
}

func (m *Module) GetNode(ctx context.Context, tx daemonsession.GraphTransaction, nodeID string) (domaingraph.Node, error) {
	if err := ensureReadable(tx); err != nil {
		return domaingraph.Node{}, err
	}
	id, err := parseUUID[domaingraph.NodeID](nodeID, "node_id")
	if err != nil {
		return domaingraph.Node{}, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
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
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return domaingraph.Node{}, err
	}
	return m.node(ctx, tx, id)
}

func (m *Module) ListNodes(ctx context.Context, tx daemonsession.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Node, string, error) {
	if err := ensureReadable(tx); err != nil {
		return nil, "", err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
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
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return nil, "", err
	}
	return m.listNodesLocal(ctx, tx, pageSize, pageToken)
}

func (m *Module) CreateNode(ctx context.Context, tx daemonsession.GraphTransaction, input NodeInput) (domaingraph.Node, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Node{}, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
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
	properties := cloneProps(input.Properties)
	if properties == nil && input.Props != nil {
		properties = cloneProps(input.Props)
	}
	payload := cloneProps(input.Payload)
	if payload == nil {
		payload = map[string]any{}
	}
	if strings.TrimSpace(input.Content) != "" {
		payload["text"] = input.Content
	}
	if blobRef != nil {
		payload["blob_id"] = string(*blobRef)
	}
	n := domaingraph.Node{ID: id, DomainID: mustDomainID(tx.DomainID), Labels: append([]string(nil), input.Labels...), Properties: properties, Payload: payload, Meta: cloneProps(input.Meta), BlobRef: blobRef, Content: input.Content, Props: cloneProps(input.Props), CreatedAt: now, UpdatedAt: now}
	if err := m.validateSchemaNode(ctx, n); err != nil {
		return domaingraph.Node{}, err
	}
	if err := m.stageNode(ctx, tx, n); err != nil {
		return domaingraph.Node{}, err
	}
	return cloneNode(n), nil
}

func (m *Module) UpdateNode(ctx context.Context, tx daemonsession.GraphTransaction, input UpdateNodeInput) (domaingraph.Node, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Node{}, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
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
	if input.Labels != nil && (len(paths) == 0 || paths["labels"]) {
		n.Labels = append([]string(nil), input.Labels...)
	}
	if input.Properties != nil && (len(paths) == 0 || paths["properties"]) {
		n.Properties = cloneProps(input.Properties)
	}
	if input.Payload != nil && (len(paths) == 0 || paths["payload"]) {
		n.Payload = cloneProps(input.Payload)
	}
	if input.Meta != nil && (len(paths) == 0 || paths["meta"]) {
		n.Meta = cloneProps(input.Meta)
	}
	if input.Content != nil && (len(paths) == 0 || paths["content"]) {
		if n.BlobRef != nil && *input.Content != "" {
			return domaingraph.Node{}, fmt.Errorf("%w: blob nodes cannot have inline content", ErrInvalidInput)
		}
		n.Content = *input.Content
		if n.Payload == nil {
			n.Payload = map[string]any{}
		}
		n.Payload["text"] = *input.Content
	}
	if input.Props != nil && (len(paths) == 0 || paths["props"]) {
		n.Props = cloneProps(input.Props)
		if n.Properties == nil {
			n.Properties = cloneProps(input.Props)
		}
	}
	n.UpdatedAt = time.Now().UTC()
	if err := m.validateSchemaNode(ctx, n); err != nil {
		return domaingraph.Node{}, err
	}
	if err := m.stageNode(ctx, tx, n); err != nil {
		return domaingraph.Node{}, err
	}
	return cloneNode(n), nil
}

func (m *Module) UpsertNode(ctx context.Context, tx daemonsession.GraphTransaction, input NodeInput) (domaingraph.Node, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Node{}, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return domaingraph.Node{}, err
	}
	if strings.TrimSpace(input.NodeID) == "" {
		return m.CreateNode(ctx, tx, input)
	}
	id, err := parseUUID[domaingraph.NodeID](input.NodeID, "node_id")
	if err != nil {
		return domaingraph.Node{}, err
	}
	if _, err := m.node(ctx, tx, id); err == nil {
		content := input.Content
		return m.UpdateNode(ctx, tx, UpdateNodeInput{NodeID: input.NodeID, Labels: input.Labels, Properties: input.Properties, Payload: input.Payload, Meta: input.Meta, Content: &content, Props: input.Props})
	} else if !errors.Is(err, ErrNotFound) {
		return domaingraph.Node{}, err
	}
	return m.CreateNode(ctx, tx, input)
}

func (m *Module) DeleteNode(ctx context.Context, tx daemonsession.GraphTransaction, nodeID string, recursive bool) ([]string, []string, error) {
	if err := ensureWritable(tx); err != nil {
		return nil, nil, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
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
			if err := m.stageEdgeDelete(ctx, tx, edge.ID); err != nil {
				return nil, nil, err
			}
			deletedEdges = append(deletedEdges, edge.ID.String())
		}
	}
	deletedNodes := []string{}
	for _, id := range nodesToDelete {
		if err := m.stageNodeDelete(ctx, tx, id); err != nil {
			return nil, nil, err
		}
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
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
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
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return domaingraph.Edge{}, err
	}
	return m.edge(ctx, tx, id)
}

func (m *Module) ListEdges(ctx context.Context, tx daemonsession.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Edge, string, error) {
	if err := ensureReadable(tx); err != nil {
		return nil, "", err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
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
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return nil, "", err
	}
	return m.listEdgesLocal(ctx, tx, pageSize, pageToken)
}

func (m *Module) CreateEdge(ctx context.Context, tx daemonsession.GraphTransaction, input EdgeInput) (domaingraph.Edge, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Edge{}, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
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
	labels := normalizeLabels(input.Labels)
	from, err := m.node(ctx, tx, fromID)
	if err != nil {
		return domaingraph.Edge{}, fmt.Errorf("%w: from node: %v", ErrInvalidInput, err)
	}
	to, err := m.node(ctx, tx, toID)
	if err != nil {
		return domaingraph.Edge{}, fmt.Errorf("%w: to node: %v", ErrInvalidInput, err)
	}
	if from.DomainID != mustDomainID(tx.DomainID) || to.DomainID != mustDomainID(tx.DomainID) {
		return domaingraph.Edge{}, fmt.Errorf("%w: edge endpoints must be in transaction domain", ErrInvalidInput)
	}
	if _, err := m.edge(ctx, tx, id); err == nil {
		return domaingraph.Edge{}, fmt.Errorf("%w: edge already exists", ErrInvalidInput)
	} else if !errors.Is(err, ErrNotFound) {
		return domaingraph.Edge{}, err
	}
	hierarchy, err := m.hierarchyPolicyForLabels(ctx, mustDomainID(tx.DomainID), labels)
	if err != nil {
		return domaingraph.Edge{}, err
	}
	if hierarchy != nil {
		if hierarchy.SameDomain && (from.DomainID != mustDomainID(tx.DomainID) || to.DomainID != mustDomainID(tx.DomainID)) {
			return domaingraph.Edge{}, fmt.Errorf("%w: hierarchy edge endpoints must be in transaction domain", ErrInvalidInput)
		}
		if hierarchy.SingleParent {
			if existing, err := m.parentEdge(ctx, tx, toID); err != nil {
				return domaingraph.Edge{}, err
			} else if existing != nil {
				return domaingraph.Edge{}, fmt.Errorf("%w: child already has a hierarchy parent", ErrInvalidInput)
			}
		}
		if hierarchy.Acyclic {
			if hasPath, err := m.hierarchyPathExists(ctx, tx, toID, fromID); err != nil {
				return domaingraph.Edge{}, err
			} else if hasPath {
				return domaingraph.Edge{}, fmt.Errorf("%w: hierarchy edge would create a cycle", ErrInvalidInput)
			}
		}
	}
	now := time.Now().UTC()
	e := domaingraph.Edge{ID: id, DomainID: mustDomainID(tx.DomainID), FromID: fromID, ToID: toID, Labels: labels, Properties: cloneProps(input.Properties), Payload: cloneProps(input.Payload), Meta: cloneProps(input.Meta), CreatedAt: now, UpdatedAt: now}
	if err := m.validateSchemaEdge(ctx, e, from, to); err != nil {
		return domaingraph.Edge{}, err
	}
	if err := m.stageEdge(ctx, tx, e); err != nil {
		return domaingraph.Edge{}, err
	}
	return cloneEdge(e), nil
}

func (m *Module) UpdateEdge(ctx context.Context, tx daemonsession.GraphTransaction, input UpdateEdgeInput) (domaingraph.Edge, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Edge{}, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
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
	if input.Labels != nil && (len(paths) == 0 || paths["labels"]) {
		e.Labels = normalizeLabels(input.Labels)
	}
	if input.Properties != nil && (len(paths) == 0 || paths["properties"]) {
		e.Properties = cloneProps(input.Properties)
	}
	if input.Payload != nil && (len(paths) == 0 || paths["payload"]) {
		e.Payload = cloneProps(input.Payload)
	}
	if input.Meta != nil && (len(paths) == 0 || paths["meta"]) {
		e.Meta = cloneProps(input.Meta)
	}
	e.UpdatedAt = time.Now().UTC()
	from, err := m.node(ctx, tx, e.FromID)
	if err != nil {
		return domaingraph.Edge{}, fmt.Errorf("%w: from node: %v", ErrInvalidInput, err)
	}
	to, err := m.node(ctx, tx, e.ToID)
	if err != nil {
		return domaingraph.Edge{}, fmt.Errorf("%w: to node: %v", ErrInvalidInput, err)
	}
	if err := m.validateSchemaEdge(ctx, e, from, to); err != nil {
		return domaingraph.Edge{}, err
	}
	if err := m.stageEdge(ctx, tx, e); err != nil {
		return domaingraph.Edge{}, err
	}
	return cloneEdge(e), nil
}

func (m *Module) DeleteEdge(ctx context.Context, tx daemonsession.GraphTransaction, edgeID string) (string, error) {
	if err := ensureWritable(tx); err != nil {
		return "", err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return "", err
	}
	id, err := parseUUID[domaingraph.EdgeID](edgeID, "edge_id")
	if err != nil {
		return "", err
	}
	if _, err := m.edge(ctx, tx, id); err != nil {
		return "", err
	}
	if err := m.stageEdgeDelete(ctx, tx, id); err != nil {
		return "", err
	}
	return id.String(), nil
}

func (m *Module) ListChildren(ctx context.Context, tx daemonsession.GraphTransaction, parentNodeID string) ([]domaingraph.Edge, error) {
	if err := ensureReadable(tx); err != nil {
		return nil, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
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
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return nil, err
	}
	return m.listChildrenLocal(ctx, tx, parentNodeID)
}

func (m *Module) GetParent(ctx context.Context, tx daemonsession.GraphTransaction, childNodeID string) (*domaingraph.Edge, error) {
	if err := ensureReadable(tx); err != nil {
		return nil, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
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
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return nil, err
	}
	return m.parentEdgeLocal(ctx, tx, childNodeID)
}

func (m *Module) MoveSubtree(ctx context.Context, tx daemonsession.GraphTransaction, nodeID string, newParentNodeID string, order *int32) (domaingraph.Edge, error) {
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Edge{}, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
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
		props = cloneProps(existing.Properties)
		if err := m.stageEdgeDelete(ctx, tx, existing.ID); err != nil {
			return domaingraph.Edge{}, err
		}
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
	labels, err := m.hierarchyEdgeLabelsForMutation(ctx, mustDomainID(tx.DomainID))
	if err != nil {
		return domaingraph.Edge{}, err
	}
	now := time.Now().UTC()
	e := domaingraph.Edge{ID: edgeID, DomainID: mustDomainID(tx.DomainID), FromID: parentID, ToID: childID, Labels: labels, Properties: props, CreatedAt: now, UpdatedAt: now}
	if err := m.stageEdge(ctx, tx, e); err != nil {
		return domaingraph.Edge{}, err
	}
	return cloneEdge(e), nil
}

func (m *Module) ReorderChildren(ctx context.Context, tx daemonsession.GraphTransaction, parentNodeID string, childNodeIDs []string) ([]domaingraph.Edge, error) {
	if err := ensureWritable(tx); err != nil {
		return nil, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
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
		edge.Properties = cloneProps(edge.Properties)
		edge.Properties["order"] = i * childOrderStep
		if err := m.stageEdge(ctx, tx, edge); err != nil {
			return nil, err
		}
		out = append(out, cloneEdge(edge))
	}
	return out, nil
}

func (m *Module) CurrentRevision(ctx context.Context, spaceID string) (int64, error) {
	if leader, forward, err := m.shouldForwardRaftGraphRead(spaceID); err != nil {
		return 0, err
	} else if forward {
		req := raftGraphReadRequest{Op: "current_revision", SpaceID: spaceID}
		var res raftGraphRevisionResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return 0, err
		}
		return res.Revision, nil
	}
	read, err := m.strongGraphRead(ctx, spaceID)
	if err != nil {
		return 0, err
	}
	store, err := m.store(ctx, spaceID)
	if err != nil {
		return 0, err
	}
	revision := int64(store.Revision())
	if read != nil {
		read.ObservedRevision = revision
		RecordStrongReadContext(ctx, read)
	}
	return revision, nil
}

func (m *Module) CommitTransactionGraph(ctx context.Context, tx daemonsession.GraphTransaction) (CommitResult, error) {
	if tx.Mode == daemonsession.TransactionModeReadOnly {
		return CommitResult{}, nil
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return CommitResult{}, err
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
	restoreOverlay := true
	defer func() {
		if !restoreOverlay {
			return
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.overlays[tx.ID] == nil {
			m.overlays[tx.ID] = snapshot.clone()
		}
	}()
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
	if err := m.validateBlobReferences(ctx, tx.SpaceID, record.PutNodes); err != nil {
		return CommitResult{}, err
	}
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
		// proposeGraphRaftCommand returns only after the local partition leader has
		// applied the committed graph command. Reading the local store revision here
		// avoids a second leader/read-index check that can fail during an immediate
		// post-commit leader change even though the write is already committed and
		// applied locally.
		committedRevision = int64(store.Revision())
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
	restoreOverlay = false
	graphEvent.Changes = changes
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
	event.TransactionID = info.TxnID
	event.GraphRevision = info.NextRevision
	event.Revision = info.NextRevision
	event.Normalize()
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
		DomainID:          domainID,
		DomainIDs:         []domaingraph.DomainID{domainID},
		Origin:            tx.Origin,
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
		if parent, err := m.storeHierarchyParent(ctx, store, domainID, node.ID); err != nil {
			return graphchange.CommittedEvent{}, err
		} else if parent != nil {
			event.OldParentByNodeID[node.ID] = parent.FromID
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
		if parent, err := m.storeHierarchyParent(ctx, store, domainID, id); err != nil {
			return graphchange.CommittedEvent{}, err
		} else if parent != nil {
			event.OldParentByNodeID[id] = parent.FromID
		}
	}
	for _, edge := range sortedEdges(snapshot.putEdges) {
		change := "added"
		if old, err := store.GetEdge(ctx, edge.ID); err == nil {
			change = "updated"
			oldHierarchy, err := m.hierarchyPolicyForLabels(ctx, domainID, old.Labels)
			if err != nil {
				return graphchange.CommittedEvent{}, err
			}
			if oldHierarchy != nil && old.ToID == edge.ToID && old.FromID != edge.FromID {
				event.OldParentByNodeID[old.ToID] = old.FromID
			}
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return graphchange.CommittedEvent{}, mapStorageError(err)
		}
		hierarchy, err := m.hierarchyPolicyForLabels(ctx, domainID, edge.Labels)
		if err != nil {
			return graphchange.CommittedEvent{}, err
		}
		if hierarchy != nil {
			event.NewParentByNodeID[edge.ToID] = edge.FromID
		}
		event.ChangedEdges = append(event.ChangedEdges, graphchange.EdgeChange{EdgeID: edge.ID, Labels: append([]string(nil), edge.Labels...), Change: change, FromID: edge.FromID, ToID: edge.ToID})
	}
	for _, id := range sortedEdgeIDs(snapshot.deleteEdges) {
		edge, err := store.GetEdge(ctx, id)
		if err != nil {
			if errors.Is(err, graphstorage.ErrNotFound) {
				continue
			}
			return graphchange.CommittedEvent{}, mapStorageError(err)
		}
		hierarchy, err := m.hierarchyPolicyForLabels(ctx, domainID, edge.Labels)
		if err != nil {
			return graphchange.CommittedEvent{}, err
		}
		if hierarchy != nil {
			event.OldParentByNodeID[edge.ToID] = edge.FromID
		}
		event.ChangedEdges = append(event.ChangedEdges, graphchange.EdgeChange{EdgeID: edge.ID, Labels: append([]string(nil), edge.Labels...), Change: "removed", FromID: edge.FromID, ToID: edge.ToID})
	}
	return event, nil
}

func (m *Module) overlayChanges(ctx context.Context, store *graphstorage.LocalStore, snapshot *overlay) ([]GraphChange, error) {
	changes := []GraphChange{}
	for _, node := range sortedNodes(snapshot.putNodes) {
		changeType := ChangeTypeNodeUpdated
		var oldCopy *domaingraph.Node
		if old, err := store.GetNode(ctx, node.ID); errors.Is(err, graphstorage.ErrNotFound) {
			changeType = ChangeTypeNodeCreated
		} else if err != nil {
			return nil, mapStorageError(err)
		} else {
			copy := cloneNode(old)
			oldCopy = &copy
		}
		copy := cloneNode(node)
		changes = append(changes, GraphChange{Type: changeType, Node: &copy, OldNode: oldCopy, NodeID: node.ID.String(), AffectedNodeIDs: []string{node.ID.String()}})
	}
	for _, edge := range sortedEdges(snapshot.putEdges) {
		changeType := ChangeTypeEdgeUpdated
		var oldCopy *domaingraph.Edge
		affectedNodeIDs := []string{edge.FromID.String(), edge.ToID.String()}
		if old, err := store.GetEdge(ctx, edge.ID); errors.Is(err, graphstorage.ErrNotFound) {
			changeType = ChangeTypeEdgeCreated
		} else if err != nil {
			return nil, mapStorageError(err)
		} else {
			copy := cloneEdge(old)
			oldCopy = &copy
			affectedNodeIDs = appendUniqueStrings(affectedNodeIDs, old.FromID.String(), old.ToID.String())
		}
		copy := cloneEdge(edge)
		changes = append(changes, GraphChange{Type: changeType, Edge: &copy, OldEdge: oldCopy, EdgeID: edge.ID.String(), AffectedNodeIDs: affectedNodeIDs, AffectedEdgeIDs: []string{edge.ID.String()}})
	}
	for _, id := range sortedNodeIDs(snapshot.deleteNodes) {
		var oldCopy *domaingraph.Node
		if old, err := store.GetNode(ctx, id); err == nil {
			copy := cloneNode(old)
			oldCopy = &copy
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return nil, mapStorageError(err)
		}
		changes = append(changes, GraphChange{Type: ChangeTypeNodeDeleted, OldNode: oldCopy, NodeID: id.String(), AffectedNodeIDs: []string{id.String()}})
	}
	for _, id := range sortedEdgeIDs(snapshot.deleteEdges) {
		change := GraphChange{Type: ChangeTypeEdgeDeleted, EdgeID: id.String(), AffectedEdgeIDs: []string{id.String()}}
		if old, err := store.GetEdge(ctx, id); err == nil {
			copy := cloneEdge(old)
			change.OldEdge = &copy
			change.AffectedNodeIDs = appendUniqueStrings(change.AffectedNodeIDs, old.FromID.String(), old.ToID.String())
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return nil, mapStorageError(err)
		}
		changes = append(changes, change)
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
	if leader, forward, err := m.shouldForwardRaftGraphRead(spaceID); err != nil {
		return 0, err
	} else if forward {
		req := raftGraphReadRequest{Op: "blob_ref_count", SpaceID: spaceID, ID: blobID}
		var res raftGraphCountResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return 0, err
		}
		return res.Count, nil
	}
	read, err := m.strongGraphRead(ctx, spaceID)
	if err != nil {
		return 0, err
	}
	store, err := m.store(ctx, spaceID)
	if err != nil {
		return 0, err
	}
	if read != nil {
		read.ObservedRevision = int64(store.Revision())
		RecordStrongReadContext(ctx, read)
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

type transactionEdgeView struct {
	store   *graphstorage.LocalStore
	overlay *overlay
}

func (m *Module) transactionEdgeView(ctx context.Context, tx daemonsession.GraphTransaction) (transactionEdgeView, error) {
	store, err := m.store(ctx, tx.SpaceID)
	if err != nil {
		return transactionEdgeView{}, err
	}
	m.mu.Lock()
	var snapshot *overlay
	if o := m.overlays[tx.ID]; o != nil {
		snapshot = o.clone()
	}
	m.mu.Unlock()
	return transactionEdgeView{store: store, overlay: snapshot}, nil
}

func (v transactionEdgeView) incoming(ctx context.Context, nodeID domaingraph.NodeID) ([]domaingraph.Edge, error) {
	base, err := v.store.IncomingEdges(ctx, nodeID)
	if err != nil {
		return nil, mapStorageError(err)
	}
	return v.mergeEndpointEdges(base, func(edge domaingraph.Edge) bool { return edge.ToID == nodeID }), nil
}

func (v transactionEdgeView) outgoing(ctx context.Context, nodeID domaingraph.NodeID) ([]domaingraph.Edge, error) {
	base, err := v.store.OutgoingEdges(ctx, nodeID)
	if err != nil {
		return nil, mapStorageError(err)
	}
	return v.mergeEndpointEdges(base, func(edge domaingraph.Edge) bool { return edge.FromID == nodeID }), nil
}

func (v transactionEdgeView) mergeEndpointEdges(base []domaingraph.Edge, includeOverlay func(domaingraph.Edge) bool) []domaingraph.Edge {
	if v.overlay == nil {
		return base
	}
	out := make([]domaingraph.Edge, 0, len(base)+len(v.overlay.putEdges))
	for _, edge := range base {
		if _, deleted := v.overlay.deleteEdges[edge.ID]; deleted {
			continue
		}
		if _, replaced := v.overlay.putEdges[edge.ID]; replaced {
			continue
		}
		out = append(out, edge)
	}
	for _, edge := range v.overlay.putEdges {
		if includeOverlay(edge) {
			out = append(out, cloneEdge(edge))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out
}

func (m *Module) isHierarchyEdge(ctx context.Context, domainID domaingraph.DomainID, edge domaingraph.Edge) (bool, error) {
	if edge.DomainID != uuid.Nil && edge.DomainID != domainID {
		return false, nil
	}
	policy, err := m.hierarchyPolicyForLabels(ctx, domainID, edge.Labels)
	if err != nil {
		return false, err
	}
	return policy != nil, nil
}

func (m *Module) storeHierarchyParent(ctx context.Context, store *graphstorage.LocalStore, domainID domaingraph.DomainID, childID domaingraph.NodeID) (*domaingraph.Edge, error) {
	edges, err := store.IncomingEdges(ctx, childID)
	if err != nil {
		return nil, mapStorageError(err)
	}
	for _, edge := range edges {
		hierarchy, err := m.isHierarchyEdge(ctx, domainID, edge)
		if err != nil {
			return nil, err
		}
		if hierarchy {
			copy := cloneEdge(edge)
			return &copy, nil
		}
	}
	return nil, nil
}

func (m *Module) hierarchyPathExists(ctx context.Context, tx daemonsession.GraphTransaction, from domaingraph.NodeID, target domaingraph.NodeID) (bool, error) {
	view, err := m.transactionEdgeView(ctx, tx)
	if err != nil {
		return false, err
	}
	domainID := mustDomainID(tx.DomainID)
	seen := map[domaingraph.NodeID]struct{}{}
	queue := []domaingraph.NodeID{from}
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
		edges, err := view.outgoing(ctx, id)
		if err != nil {
			return false, err
		}
		for _, edge := range edges {
			hierarchy, err := m.isHierarchyEdge(ctx, domainID, edge)
			if err != nil {
				return false, err
			}
			if !hierarchy {
				continue
			}
			queue = append(queue, edge.ToID)
		}
	}
	return false, nil
}

func (m *Module) listNodesLocal(ctx context.Context, tx daemonsession.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Node, string, error) {
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

func (m *Module) listEdgesLocal(ctx context.Context, tx daemonsession.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Edge, string, error) {
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

func (m *Module) listChildrenLocal(ctx context.Context, tx daemonsession.GraphTransaction, parentNodeID string) ([]domaingraph.Edge, error) {
	parentID, err := parseUUID[domaingraph.NodeID](parentNodeID, "parent_node_id")
	if err != nil {
		return nil, err
	}
	view, err := m.transactionEdgeView(ctx, tx)
	if err != nil {
		return nil, err
	}
	edges, err := view.outgoing(ctx, parentID)
	if err != nil {
		return nil, err
	}
	domainID := mustDomainID(tx.DomainID)
	out := []domaingraph.Edge{}
	for _, edge := range edges {
		hierarchy, err := m.isHierarchyEdge(ctx, domainID, edge)
		if err != nil {
			return nil, err
		}
		if hierarchy {
			out = append(out, edge)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return edgeOrder(out[i], i) < edgeOrder(out[j], j) })
	return out, nil
}

func (m *Module) parentEdge(ctx context.Context, tx daemonsession.GraphTransaction, childID domaingraph.NodeID) (*domaingraph.Edge, error) {
	return m.parentEdgeLocal(ctx, tx, childID.String())
}

func (m *Module) parentEdgeLocal(ctx context.Context, tx daemonsession.GraphTransaction, childNodeID string) (*domaingraph.Edge, error) {
	childID, err := parseUUID[domaingraph.NodeID](childNodeID, "child_node_id")
	if err != nil {
		return nil, err
	}
	view, err := m.transactionEdgeView(ctx, tx)
	if err != nil {
		return nil, err
	}
	edges, err := view.incoming(ctx, childID)
	if err != nil {
		return nil, err
	}
	domainID := mustDomainID(tx.DomainID)
	for _, edge := range edges {
		hierarchy, err := m.isHierarchyEdge(ctx, domainID, edge)
		if err != nil {
			return nil, err
		}
		if hierarchy {
			copy := cloneEdge(edge)
			return &copy, nil
		}
	}
	return nil, nil
}

func (m *Module) stageNode(ctx context.Context, tx daemonsession.GraphTransaction, node domaingraph.Node) error {
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(tx.ID)
	delete(o.deleteNodes, node.ID)
	o.putNodes[node.ID] = cloneNode(node)
	o.opCount++
	return nil
}

func (m *Module) stageNodeDelete(ctx context.Context, tx daemonsession.GraphTransaction, id domaingraph.NodeID) error {
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(tx.ID)
	delete(o.putNodes, id)
	o.deleteNodes[id] = struct{}{}
	o.opCount++
	return nil
}

func (m *Module) stageEdge(ctx context.Context, tx daemonsession.GraphTransaction, edge domaingraph.Edge) error {
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(tx.ID)
	delete(o.deleteEdges, edge.ID)
	o.putEdges[edge.ID] = cloneEdge(edge)
	o.opCount++
	return nil
}

func (m *Module) stageEdgeDelete(ctx context.Context, tx daemonsession.GraphTransaction, id domaingraph.EdgeID) error {
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(tx.ID)
	delete(o.putEdges, id)
	o.deleteEdges[id] = struct{}{}
	o.opCount++
	return nil
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
	if errors.Is(err, graphstorage.ErrTxnClosed) || errors.Is(err, graphstorage.ErrClosed) || errors.Is(err, graphstorage.ErrIndexUnavailable) {
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

func mustDomainID(value string) domaingraph.DomainID {
	id, _ := uuid.Parse(value)
	return domaingraph.DomainID(id)
}

func cloneNode(n domaingraph.Node) domaingraph.Node {
	n.Labels = append([]string(nil), n.Labels...)
	n.Properties = cloneProps(n.Properties)
	n.Payload = cloneProps(n.Payload)
	n.Meta = cloneProps(n.Meta)
	n.Props = cloneProps(n.Props)
	return n
}
func cloneEdge(e domaingraph.Edge) domaingraph.Edge {
	e.Labels = append([]string(nil), e.Labels...)
	e.Properties = cloneProps(e.Properties)
	e.Payload = cloneProps(e.Payload)
	e.Meta = cloneProps(e.Meta)
	return e
}

func appendUniqueStrings(values []string, candidates ...string) []string {
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		seen := false
		for _, existing := range values {
			if existing == candidate {
				seen = true
				break
			}
		}
		if !seen {
			values = append(values, candidate)
		}
	}
	return values
}

func normalizeLabels(labels []string) []string {
	out := make([]string, 0, len(labels))
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label != "" {
			out = append(out, label)
		}
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
	if value, ok := intProp(edge.Properties["order"]); ok {
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

func (m *Module) ConfigureIndexes(ctx context.Context, tx daemonsession.GraphTransaction, schemaHash string, indexes []schemamodel.IndexDefinition) error {
	if err := ensureReadable(tx); err != nil {
		return err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
		return err
	} else if forward {
		req := raftReadRequest("configure_indexes", tx)
		req.SchemaHash = schemaHash
		req.Indexes = indexes
		var res raftGraphOKResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return err
		}
		return nil
	}
	store, err := m.store(ctx, tx.SpaceID)
	if err != nil {
		return err
	}
	return mapStorageError(store.ConfigureIndexes(ctx, mustDomainID(tx.DomainID), schemaHash, indexes))
}

func (m *Module) ScanNodePropertyOrdered(ctx context.Context, tx daemonsession.GraphTransaction, scan OrderedNodePropertyScan) ([]domaingraph.Node, string, IndexedReadStats, error) {
	stats := IndexedReadStats{Plan: "OrderedNodePropertyIndexScan", IndexName: scan.IndexName, NextCursorKind: "index_key"}
	if err := ensureReadable(tx); err != nil {
		return nil, "", stats, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
		return nil, "", stats, err
	} else if forward {
		req := raftReadRequest("scan_node_property_ordered", tx)
		req.OrderedNodePropertyScan = scan
		var res raftGraphIndexedNodesResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return nil, "", stats, err
		}
		return res.Nodes, res.NextPageToken, res.Stats, nil
	}
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return nil, "", stats, err
	}
	store, err := m.store(ctx, tx.SpaceID)
	if err != nil {
		return nil, "", stats, err
	}
	m.mu.Lock()
	o := m.overlays[tx.ID]
	var overlaySnapshot *overlay
	if o != nil {
		overlaySnapshot = o.clone()
	}
	m.mu.Unlock()
	extra := 0
	if overlaySnapshot != nil {
		extra = len(overlaySnapshot.putNodes)
	}
	storageLimit := scan.Limit
	if storageLimit > 0 {
		storageLimit += extra
	}
	entries, next, err := store.ScanNodePropertyOrdered(ctx, graphstorage.OrderedNodePropertyScan{DomainID: mustDomainID(tx.DomainID), IndexName: scan.IndexName, Direction: scan.Direction, Limit: storageLimit, Cursor: scan.Cursor, HasLow: scan.HasLow, Low: scan.Low, LowExclusive: scan.LowExclusive, HasHigh: scan.HasHigh, High: scan.High, HighExclusive: scan.HighExclusive})
	if err != nil {
		return nil, "", stats, mapStorageError(err)
	}
	stats.IndexEntriesScanned = len(entries)
	items := make([]indexedNodeResult, 0, len(entries)+extra)
	for _, entry := range entries {
		if overlaySnapshot != nil {
			if _, deleted := overlaySnapshot.deleteNodes[entry.NodeID]; deleted {
				continue
			}
			if _, replaced := overlaySnapshot.putNodes[entry.NodeID]; replaced {
				continue
			}
		}
		node, err := store.GetNode(ctx, entry.NodeID)
		if err != nil {
			return nil, "", stats, mapStorageError(err)
		}
		stats.NodesLoaded++
		key, _ := graphstorage.EncodeOrderedNodeKey(entry.Value, node.ID)
		items = append(items, indexedNodeResult{node: node, key: key})
	}
	if overlaySnapshot != nil {
		cursorKey, err := graphstorage.DecodeIndexCursor(scan.Cursor)
		if err != nil {
			return nil, "", stats, mapStorageError(err)
		}
		for _, node := range overlaySnapshot.putNodes {
			if node.DomainID != mustDomainID(tx.DomainID) || !nodeMatchesConfiguredIndex(node, scan.IndexName, store) {
				continue
			}
			value, ok := domaingraph.Property(node, indexedFieldName(scan.IndexName, store, mustDomainID(tx.DomainID)))
			if !ok {
				continue
			}
			key, err := graphstorage.EncodeOrderedNodeKey(value, node.ID)
			if err != nil {
				continue
			}
			if !indexedValueInBounds(key, scan) {
				continue
			}
			if cursorKey != "" {
				if scan.Direction == schemamodel.IndexSortDirectionDesc {
					if key >= cursorKey {
						continue
					}
				} else if key <= cursorKey {
					continue
				}
			}
			items = append(items, indexedNodeResult{node: cloneNode(node), key: key})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if scan.Direction == schemamodel.IndexSortDirectionDesc {
			return items[i].key > items[j].key
		}
		return items[i].key < items[j].key
	})
	limit := scan.Limit
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	out := make([]domaingraph.Node, 0, limit)
	for _, item := range items[:limit] {
		out = append(out, cloneNode(item.node))
	}
	stats.NodesLoaded = len(out)
	if limit < len(items) && limit > 0 {
		next = graphstorage.EncodeIndexCursor(items[limit-1].key)
	}
	return out, next, stats, nil
}

type indexedNodeResult struct {
	node domaingraph.Node
	key  string
}

func indexedValueInBounds(key string, scan OrderedNodePropertyScan) bool {
	valueKey := key
	if idx := strings.LastIndex(key, "\x00"); idx >= 0 {
		valueKey = key[:idx]
	}
	if scan.HasLow {
		low, err := graphstorage.EncodeSortableValue(scan.Low)
		if err != nil || valueKey < low || scan.LowExclusive && valueKey == low {
			return false
		}
	}
	if scan.HasHigh {
		high, err := graphstorage.EncodeSortableValue(scan.High)
		if err != nil || valueKey > high || scan.HighExclusive && valueKey == high {
			return false
		}
	}
	return true
}

func nodeMatchesConfiguredIndex(node domaingraph.Node, indexName string, store *graphstorage.LocalStore) bool {
	statuses, err := store.IndexStatuses(context.Background())
	if err != nil {
		return false
	}
	for _, status := range statuses {
		if status.Name == indexName && status.DomainID == node.DomainID {
			if len(status.Labels) == 0 {
				return false
			}
			return nodeHasAnyLabelForIndexedRead(node, status.Labels)
		}
	}
	return false
}

func indexedFieldName(indexName string, store *graphstorage.LocalStore, domainID domaingraph.DomainID) string {
	statuses, err := store.IndexStatuses(context.Background())
	if err != nil {
		return ""
	}
	for _, status := range statuses {
		if status.Name == indexName && status.DomainID == domainID {
			return status.Field.Name
		}
	}
	return ""
}

func nodeHasAnyLabelForIndexedRead(node domaingraph.Node, labels []string) bool {
	seen := map[string]struct{}{}
	for _, label := range node.Labels {
		seen[label] = struct{}{}
	}
	for _, label := range labels {
		if _, ok := seen[label]; ok {
			return true
		}
	}
	return false
}
