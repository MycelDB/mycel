package service

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/myceldb/mycel/internal/fsperm"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/encryption"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	"github.com/myceldb/mycel/internal/graph/writetrace"
	runtime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	childOrderStep      = 1000
	nodePageTokenPrefix = "nodes-v1:"
)

type nodePageCursor struct {
	TransactionID string `json:"transaction_id"`
	BaseRevision  int64  `json:"base_revision"`
	OverlayOps    int32  `json:"overlay_ops"`
	AfterNodeID   string `json:"after_node_id"`
	Checksum      string `json:"checksum"`
}

type nodeOverlayPage struct {
	operations int32
	putNodes   []domaingraph.Node
	replaced   map[domaingraph.NodeID]struct{}
	deleted    map[domaingraph.NodeID]struct{}
}

type Module struct {
	mu                             sync.Mutex
	dataDir                        string
	stores                         map[domainStoreKey]*graphstorage.LocalStore
	overlays                       map[string]*overlay
	transactionMutationLocks       map[string]*transactionMutationLock
	idempotencyScopeLocks          [64]sync.Mutex
	activeMutationScopes           map[string]*mutationScope
	finalizedTransactions          map[string]transactionFinalization
	transactionValidator           TransactionValidator
	changeSink                     graphchange.Sink
	raftApplyChangeSink            graphchange.Sink
	schemaValidator                SchemaValidator
	blobRefs                       BlobReferenceChecker
	automationOutputFenceValidator AutomationOutputFenceValidator
	lastGraphChangeSinkErr         error
	gate                           *quiesce.Gate
	wal                            *wal.Manager
	walProgress                    wal.AppliedLSNStore
	walWaiter                      *wal.ApplyWaiter
	encryption                     *encryption.Service
	writeAllowed                   func() error
	checkpointPolicy               CheckpointPolicyConfig
	checkpointStates               map[domainStoreKey]checkpointRuntimeState
	checkpointWorkerCancel         context.CancelFunc
	checkpointWorkerDone           chan struct{}
	raftGroups                     *consensus.MultiGroup
	raftPartitionCount             uint32
	raftLocalNode                  consensus.NodeID
	raftNodeAddrs                  []string
	raftBackendAuthToken           string
	raftAppliedCommands            map[string]struct{}
	raftAppliedCommandsPersistMu   sync.Mutex
	logger                         *slog.Logger
	writeTrace                     writetrace.Config
	idempotencyReplayHits          atomic.Uint64
	idempotencyReplayMisses        atomic.Uint64
	idempotencyMismatches          atomic.Uint64
	idempotencyCapacityRejections  atomic.Uint64
}

type domainStoreKey struct {
	SpaceID  string
	DomainID string
}

type overlay struct {
	putNodes             map[domaingraph.NodeID]domaingraph.Node
	deleteNodes          map[domaingraph.NodeID]struct{}
	putEdges             map[domaingraph.EdgeID]domaingraph.Edge
	deleteEdges          map[domaingraph.EdgeID]struct{}
	automationFenceNodes map[domaingraph.NodeID]struct{}
	automationFenceEdges map[domaingraph.EdgeID]struct{}
	opCount              int32
}

func NewModule() *Module {
	return &Module{
		stores: map[domainStoreKey]*graphstorage.LocalStore{}, overlays: map[string]*overlay{},
		transactionMutationLocks: map[string]*transactionMutationLock{}, activeMutationScopes: map[string]*mutationScope{}, finalizedTransactions: map[string]transactionFinalization{},
		checkpointStates: map[domainStoreKey]checkpointRuntimeState{}, gate: quiesce.NewGate(ModuleName),
	}
}

func (m *Module) WithCheckpointPolicy(cfg CheckpointPolicyConfig) *Module {
	m.checkpointPolicy = normalizeCheckpointPolicyConfig(cfg)
	return m
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) SetChangeSink(sink graphchange.Sink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changeSink = sink
}

func (m *Module) SetRaftApplyChangeSink(sink graphchange.Sink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.raftApplyChangeSink = sink
}

func (m *Module) LastGraphChangeSinkError() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastGraphChangeSinkErr
}

type BlobReferenceChecker interface {
	EnsureBlobReference(ctx context.Context, spaceID string, blobID string) error
}

func (m *Module) SetTransactionValidator(validator TransactionValidator) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.transactionValidator = validator
}

func (m *Module) SetSchemaValidator(validator SchemaValidator) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schemaValidator = validator
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
	manager := m.schemaValidator
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
	manager := m.schemaValidator
	m.mu.Unlock()
	if manager != nil {
		schema, err := manager.GetDomainSchema(ctx, domainID)
		if err != nil && !errors.Is(err, schemamodel.ErrNotFound) {
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
	manager := m.schemaValidator
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
		if errors.Is(err, schemamodel.ErrNotFound) {
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
	manager := m.schemaValidator
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

func formatSchemaIssues(issues []schemamodel.ValidationIssue) string {
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
	m.logger = host.Log()
	m.writeTrace = writetrace.FromEnv()
	m.dataDir = filepath.Join(host.DataDir(), "graphs")
	if err := os.MkdirAll(m.dataDir, fsperm.PrivateDir); err != nil {
		return runtime.Abort(ModuleName, "storage", "create graph data directory", err)
	}
	if m.stores == nil {
		m.stores = map[domainStoreKey]*graphstorage.LocalStore{}
	}
	if m.overlays == nil {
		m.overlays = map[string]*overlay{}
	}
	if m.transactionMutationLocks == nil {
		m.transactionMutationLocks = map[string]*transactionMutationLock{}
	}
	if m.activeMutationScopes == nil {
		m.activeMutationScopes = map[string]*mutationScope{}
	}
	if m.finalizedTransactions == nil {
		m.finalizedTransactions = map[string]transactionFinalization{}
	}
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	if m.checkpointStates == nil {
		m.checkpointStates = map[domainStoreKey]checkpointRuntimeState{}
	}
	m.checkpointPolicy = normalizeCheckpointPolicyConfig(m.checkpointPolicy)
	m.loadRaftAppliedCommands()
	if provider, ok := host.(runtime.WALProvider); ok {
		m.wal = provider.WALManager()
		m.walProgress = provider.WALProgressStore()
		m.walWaiter = provider.WALWaiterStore()
	}
	if provider, ok := host.(runtime.EncryptionProvider); ok {
		m.encryption = provider.EncryptionService()
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
		logger.Info("graph module initialized", "storage", "file", "path", m.dataDir, "write_trace", m.writeTrace.Enabled, "write_trace_threshold", m.writeTrace.Threshold.String())
	}
	return runtime.OK(ModuleName)
}

func (m *Module) GetNode(ctx context.Context, tx sessionmodel.GraphTransaction, nodeID string) (domaingraph.Node, error) {
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

func (m *Module) ListNodes(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Node, string, error) {
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
	nodes, next, _, err := m.scanNodesLocal(ctx, tx, graphquery.NodeScan{Limit: pageSize, Cursor: pageToken, ExamineLimit: pageSize})
	return nodes, next, err
}

// ScanNodes exposes the finite node cursor together with examined-entry work
// so the query runtime can charge revision-history progress to its budget.
func (m *Module) ScanNodes(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.NodeScan) ([]domaingraph.Node, string, graphquery.IndexedReadStats, error) {
	stats := graphquery.IndexedReadStats{Plan: "NodeScan", IndexName: "node_id", FullScan: true, NextCursorKind: "node_key"}
	if err := ensureReadable(tx); err != nil {
		return nil, "", stats, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
		return nil, "", stats, err
	} else if forward {
		req := raftReadRequest("scan_nodes", tx)
		req.NodeScan = scan
		var res raftGraphNodesResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return nil, "", stats, err
		}
		return res.Nodes, res.NextPageToken, res.Stats, nil
	}
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return nil, "", stats, err
	}
	return m.scanNodesLocal(ctx, tx, scan)
}

// ReadViewIdentity identifies the mutable transaction overlay observed by query
// execution. BaseRevision and transaction identity are already bound by query
// cursors; the operation count changes for every staged node or edge mutation.
func (m *Module) ReadViewIdentity(ctx context.Context, tx sessionmodel.GraphTransaction) (string, error) {
	if err := ensureReadable(tx); err != nil {
		return "", err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
		return "", err
	} else if forward {
		req := raftReadRequest("read_view_identity", tx)
		var res raftGraphReadViewResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return "", err
		}
		return res.Identity, nil
	}
	return m.readViewIdentityLocal(tx), ctx.Err()
}

func (m *Module) readViewIdentityLocal(tx sessionmodel.GraphTransaction) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if overlay := m.overlays[tx.ID]; overlay != nil {
		return strconv.FormatInt(int64(overlay.opCount), 10)
	}
	return "0"
}

type transactionFinalizationKind uint8

const (
	transactionCommitted transactionFinalizationKind = iota + 1
	transactionDiscarded
)

type transactionFinalization struct {
	kind   transactionFinalizationKind
	commit graphchange.CommitResult
}

type transactionMutationLock struct {
	token chan struct{}
	refs  int
}

type transactionMutationLockOwner struct {
	module        *Module
	transactionID string
}

type transactionMutationLockContextKey struct{}

type nodeMutationUndo struct {
	put     domaingraph.Node
	hadPut  bool
	deleted bool
	fence   bool
}

type edgeMutationUndo struct {
	put     domaingraph.Edge
	hadPut  bool
	deleted bool
	fence   bool
}

type mutationScope struct {
	module         *Module
	transactionID  string
	release        func()
	overlayExisted bool
	initialOpCount int32
	nodes          map[domaingraph.NodeID]nodeMutationUndo
	edges          map[domaingraph.EdgeID]edgeMutationUndo
}

func (m *Module) lockTransactionMutation(ctx context.Context, transactionID string) (context.Context, func(), error) {
	if owner, _ := ctx.Value(transactionMutationLockContextKey{}).(transactionMutationLockOwner); owner.module == m && owner.transactionID == transactionID {
		return ctx, func() {}, nil
	}
	m.mu.Lock()
	if m.transactionMutationLocks == nil {
		m.transactionMutationLocks = map[string]*transactionMutationLock{}
	}
	lock := m.transactionMutationLocks[transactionID]
	if lock == nil {
		lock = &transactionMutationLock{token: make(chan struct{}, 1)}
		lock.token <- struct{}{}
		m.transactionMutationLocks[transactionID] = lock
	}
	lock.refs++
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		m.releaseTransactionMutationLockRef(transactionID, lock)
		return ctx, nil, ctx.Err()
	case <-lock.token:
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			lock.token <- struct{}{}
			m.releaseTransactionMutationLockRef(transactionID, lock)
		})
	}
	owner := transactionMutationLockOwner{module: m, transactionID: transactionID}
	return context.WithValue(ctx, transactionMutationLockContextKey{}, owner), release, nil
}

func (m *Module) lockActiveTransactionMutation(ctx context.Context, tx sessionmodel.GraphTransaction) (context.Context, func(), error) {
	lockedCtx, release, err := m.lockTransactionMutation(ctx, tx.ID)
	if err != nil {
		return ctx, nil, err
	}
	if err := m.validateTransactionActive(lockedCtx, tx); err != nil {
		release()
		return ctx, nil, err
	}
	return lockedCtx, release, nil
}

func (m *Module) validateTransactionActive(ctx context.Context, tx sessionmodel.GraphTransaction) error {
	m.mu.Lock()
	finalized := m.finalizedTransactions[tx.ID]
	validator := m.transactionValidator
	m.mu.Unlock()
	if finalized.kind != 0 {
		return fmt.Errorf("%w: transaction graph is finalized", ErrInvalidState)
	}
	if err := ensureReadable(tx); err != nil {
		return err
	}
	if validator != nil {
		if err := validator.ValidateTransactionActive(ctx, tx); err != nil {
			return fmt.Errorf("%w: transaction is no longer active: %v", ErrInvalidState, err)
		}
	}
	return nil
}

func (m *Module) finalizeTransactionLocked(transactionID string, finalization transactionFinalization) error {
	if m.finalizedTransactions == nil {
		m.finalizedTransactions = map[string]transactionFinalization{}
	}
	if existing := m.finalizedTransactions[transactionID]; existing.kind != 0 {
		if existing.kind == finalization.kind {
			return nil
		}
		return fmt.Errorf("%w: transaction graph is already finalized", ErrInvalidState)
	}
	m.finalizedTransactions[transactionID] = finalization
	return nil
}

func (m *Module) releaseTransactionMutationLockRef(transactionID string, lock *transactionMutationLock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock.refs--
	if lock.refs == 0 && m.transactionMutationLocks[transactionID] == lock {
		delete(m.transactionMutationLocks, transactionID)
	}
}

// BeginMutationStatement acquires the transaction mutation lease and starts a
// graph-owned undo journal. The journal stores only pre-existing entries first
// touched by this statement rather than cloning the accumulated overlay.
func (m *Module) BeginMutationStatement(ctx context.Context, tx sessionmodel.GraphTransaction) (context.Context, any, error) {
	if err := ensureWritable(tx); err != nil {
		return ctx, nil, err
	}
	lockedCtx, release, err := m.lockTransactionMutation(ctx, tx.ID)
	if err != nil {
		return ctx, nil, err
	}
	if err := m.validateTransactionActive(lockedCtx, tx); err != nil {
		release()
		return ctx, nil, err
	}
	scope := &mutationScope{
		module: m, transactionID: tx.ID, release: release,
		nodes: map[domaingraph.NodeID]nodeMutationUndo{}, edges: map[domaingraph.EdgeID]edgeMutationUndo{},
	}
	m.mu.Lock()
	if m.activeMutationScopes == nil {
		m.activeMutationScopes = map[string]*mutationScope{}
	}
	if m.activeMutationScopes[tx.ID] != nil {
		m.mu.Unlock()
		release()
		return ctx, nil, fmt.Errorf("%w: mutation statement is already active", ErrInvalidState)
	}
	if current := m.overlays[tx.ID]; current != nil {
		scope.overlayExisted = true
		scope.initialOpCount = current.opCount
	}
	m.activeMutationScopes[tx.ID] = scope
	m.mu.Unlock()
	return lockedCtx, scope, nil
}

// FinishMutationStatement releases the transaction mutation lease. Failed
// statements restore only entries recorded by the bounded undo journal.
func (m *Module) FinishMutationStatement(ctx context.Context, tx sessionmodel.GraphTransaction, captured any, commit bool) error {
	scope, ok := captured.(*mutationScope)
	if !ok || scope == nil || scope.module != m || scope.transactionID != tx.ID {
		return fmt.Errorf("%w: invalid mutation scope", ErrInvalidInput)
	}
	var validationErr error
	if commit {
		validationErr = m.validateTransactionActive(ctx, tx)
	}
	m.mu.Lock()
	if m.activeMutationScopes[tx.ID] != scope {
		m.mu.Unlock()
		return fmt.Errorf("%w: mutation scope is not active", ErrInvalidState)
	}
	if !commit || validationErr != nil {
		m.restoreMutationScopeLocked(scope)
	}
	delete(m.activeMutationScopes, tx.ID)
	m.mu.Unlock()
	scope.release()
	return validationErr
}

func (m *Module) restoreMutationScopeLocked(scope *mutationScope) {
	if !scope.overlayExisted {
		delete(m.overlays, scope.transactionID)
		return
	}
	o := m.overlay(scope.transactionID)
	for id, undo := range scope.nodes {
		if undo.hadPut {
			o.putNodes[id] = undo.put
		} else {
			delete(o.putNodes, id)
		}
		if undo.deleted {
			o.deleteNodes[id] = struct{}{}
		} else {
			delete(o.deleteNodes, id)
		}
		if undo.fence {
			o.automationFenceNodes[id] = struct{}{}
		} else {
			delete(o.automationFenceNodes, id)
		}
	}
	for id, undo := range scope.edges {
		if undo.hadPut {
			o.putEdges[id] = undo.put
		} else {
			delete(o.putEdges, id)
		}
		if undo.deleted {
			o.deleteEdges[id] = struct{}{}
		} else {
			delete(o.deleteEdges, id)
		}
		if undo.fence {
			o.automationFenceEdges[id] = struct{}{}
		} else {
			delete(o.automationFenceEdges, id)
		}
	}
	o.opCount = scope.initialOpCount
}

func (m *Module) recordNodeMutationLocked(transactionID string, o *overlay, id domaingraph.NodeID) {
	scope := m.activeMutationScopes[transactionID]
	if scope == nil || !scope.overlayExisted {
		return
	}
	if _, recorded := scope.nodes[id]; recorded {
		return
	}
	put, hadPut := o.putNodes[id]
	_, deleted := o.deleteNodes[id]
	_, fence := o.automationFenceNodes[id]
	scope.nodes[id] = nodeMutationUndo{put: put, hadPut: hadPut, deleted: deleted, fence: fence}
}

func (m *Module) recordEdgeMutationLocked(transactionID string, o *overlay, id domaingraph.EdgeID) {
	scope := m.activeMutationScopes[transactionID]
	if scope == nil || !scope.overlayExisted {
		return
	}
	if _, recorded := scope.edges[id]; recorded {
		return
	}
	put, hadPut := o.putEdges[id]
	_, deleted := o.deleteEdges[id]
	_, fence := o.automationFenceEdges[id]
	scope.edges[id] = edgeMutationUndo{put: put, hadPut: hadPut, deleted: deleted, fence: fence}
}

func (m *Module) CreateNode(ctx context.Context, tx sessionmodel.GraphTransaction, input domaingraph.NodeInput) (domaingraph.Node, error) {
	lockedCtx, releaseMutation, lockErr := m.lockActiveTransactionMutation(ctx, tx)
	if lockErr != nil {
		return domaingraph.Node{}, lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
	traceStart := time.Now()
	var timing graphStageTiming
	stepStart := time.Now()
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Node{}, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return domaingraph.Node{}, err
	}
	timing.RouteCheck = time.Since(stepStart)
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
	stepStart = time.Now()
	if _, err := m.node(ctx, tx, id); err == nil {
		return domaingraph.Node{}, fmt.Errorf("%w: node already exists", ErrInvalidInput)
	} else if !errors.Is(err, ErrNotFound) {
		return domaingraph.Node{}, err
	}
	timing.NodeLookup = time.Since(stepStart)
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
	stepStart = time.Now()
	if err := m.validateSchemaNode(ctx, n); err != nil {
		return domaingraph.Node{}, err
	}
	timing.SchemaValidate = time.Since(stepStart)
	stepStart = time.Now()
	if err := m.stageNodeWithFence(ctx, tx, n, input.AutomationOutputFence); err != nil {
		return domaingraph.Node{}, err
	}
	timing.Stage = time.Since(stepStart)
	timing.Total = time.Since(traceStart)
	m.logGraphStageTiming("create_node", tx, map[string]int{"put_nodes": 1, "labels": len(n.Labels), "properties": len(n.Properties), "payload_fields": len(n.Payload)}, timing)
	return cloneNode(n), nil
}

func (m *Module) UpdateNode(ctx context.Context, tx sessionmodel.GraphTransaction, input domaingraph.UpdateNodeInput) (domaingraph.Node, error) {
	lockedCtx, releaseMutation, lockErr := m.lockActiveTransactionMutation(ctx, tx)
	if lockErr != nil {
		return domaingraph.Node{}, lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
	traceStart := time.Now()
	var timing graphStageTiming
	stepStart := time.Now()
	if err := ensureWritable(tx); err != nil {
		return domaingraph.Node{}, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return domaingraph.Node{}, err
	}
	timing.RouteCheck = time.Since(stepStart)
	id, err := parseUUID[domaingraph.NodeID](input.NodeID, "node_id")
	if err != nil {
		return domaingraph.Node{}, err
	}
	stepStart = time.Now()
	n, err := m.node(ctx, tx, id)
	if err != nil {
		return domaingraph.Node{}, err
	}
	timing.NodeLookup = time.Since(stepStart)
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
	stepStart = time.Now()
	if err := m.validateSchemaNode(ctx, n); err != nil {
		return domaingraph.Node{}, err
	}
	timing.SchemaValidate = time.Since(stepStart)
	stepStart = time.Now()
	if err := m.stageNodeWithFence(ctx, tx, n, input.AutomationOutputFence); err != nil {
		return domaingraph.Node{}, err
	}
	timing.Stage = time.Since(stepStart)
	timing.UpdateMask = append([]string(nil), input.UpdateMask...)
	timing.Total = time.Since(traceStart)
	m.logGraphStageTiming("update_node", tx, map[string]int{"put_nodes": 1, "labels": len(n.Labels), "properties": len(n.Properties), "payload_fields": len(n.Payload)}, timing)
	return cloneNode(n), nil
}

func (m *Module) UpsertNode(ctx context.Context, tx sessionmodel.GraphTransaction, input domaingraph.NodeInput) (domaingraph.Node, error) {
	lockedCtx, releaseMutation, lockErr := m.lockActiveTransactionMutation(ctx, tx)
	if lockErr != nil {
		return domaingraph.Node{}, lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
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
		return m.UpdateNode(ctx, tx, domaingraph.UpdateNodeInput{NodeID: input.NodeID, Labels: input.Labels, Properties: input.Properties, Payload: input.Payload, Meta: input.Meta, Content: &content, Props: input.Props, AutomationOutputFence: input.AutomationOutputFence})
	} else if !errors.Is(err, ErrNotFound) {
		return domaingraph.Node{}, err
	}
	return m.CreateNode(ctx, tx, input)
}

func (m *Module) DeleteNode(ctx context.Context, tx sessionmodel.GraphTransaction, nodeID string, recursive bool) ([]string, []string, error) {
	lockedCtx, releaseMutation, lockErr := m.lockActiveTransactionMutation(ctx, tx)
	if lockErr != nil {
		return nil, nil, lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
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

func (m *Module) GetEdge(ctx context.Context, tx sessionmodel.GraphTransaction, edgeID string) (domaingraph.Edge, error) {
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

func (m *Module) ListEdges(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Edge, string, error) {
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

func (m *Module) CreateEdge(ctx context.Context, tx sessionmodel.GraphTransaction, input domaingraph.EdgeInput) (domaingraph.Edge, error) {
	lockedCtx, releaseMutation, lockErr := m.lockActiveTransactionMutation(ctx, tx)
	if lockErr != nil {
		return domaingraph.Edge{}, lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
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
	if err := m.stageEdgeWithFence(ctx, tx, e, input.AutomationOutputFence); err != nil {
		return domaingraph.Edge{}, err
	}
	return cloneEdge(e), nil
}

func (m *Module) UpdateEdge(ctx context.Context, tx sessionmodel.GraphTransaction, input domaingraph.UpdateEdgeInput) (domaingraph.Edge, error) {
	lockedCtx, releaseMutation, lockErr := m.lockActiveTransactionMutation(ctx, tx)
	if lockErr != nil {
		return domaingraph.Edge{}, lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
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
	if err := m.stageEdgeWithFence(ctx, tx, e, input.AutomationOutputFence); err != nil {
		return domaingraph.Edge{}, err
	}
	return cloneEdge(e), nil
}

func (m *Module) DeleteEdge(ctx context.Context, tx sessionmodel.GraphTransaction, edgeID string) (string, error) {
	lockedCtx, releaseMutation, lockErr := m.lockActiveTransactionMutation(ctx, tx)
	if lockErr != nil {
		return "", lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
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

func (m *Module) ReplaceReferences(ctx context.Context, tx sessionmodel.GraphTransaction, input domaingraph.ReplaceReferencesInput) (domaingraph.ReplaceReferencesResult, error) {
	lockedCtx, releaseMutation, lockErr := m.lockActiveTransactionMutation(ctx, tx)
	if lockErr != nil {
		return domaingraph.ReplaceReferencesResult{}, lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
	if err := ensureWritable(tx); err != nil {
		return domaingraph.ReplaceReferencesResult{}, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return domaingraph.ReplaceReferencesResult{}, err
	}
	sourceID, err := parseUUID[domaingraph.NodeID](input.SourceNodeID, "source_node_id")
	if err != nil {
		return domaingraph.ReplaceReferencesResult{}, err
	}
	source, err := m.node(ctx, tx, sourceID)
	if err != nil {
		return domaingraph.ReplaceReferencesResult{}, fmt.Errorf("%w: source node: %v", ErrInvalidInput, err)
	}
	if source.DomainID != mustDomainID(tx.DomainID) {
		return domaingraph.ReplaceReferencesResult{}, fmt.Errorf("%w: source node must be in transaction domain", ErrInvalidInput)
	}
	labels := normalizeLabels(input.Labels)
	if len(labels) == 0 {
		return domaingraph.ReplaceReferencesResult{}, fmt.Errorf("%w: reference labels are required", ErrInvalidInput)
	}
	mode := input.Mode
	if mode == "" {
		mode = domaingraph.ReferenceReplacementModeReplace
	}
	if mode != domaingraph.ReferenceReplacementModeReplace && mode != domaingraph.ReferenceReplacementModeAdd && mode != domaingraph.ReferenceReplacementModeRemove {
		return domaingraph.ReplaceReferencesResult{}, fmt.Errorf("%w: unsupported reference replacement mode %q", ErrInvalidInput, input.Mode)
	}

	targets := make(map[domaingraph.NodeID]domaingraph.ReferenceTargetInput, len(input.Targets))
	targetOrder := make([]domaingraph.NodeID, 0, len(input.Targets))
	for i, target := range input.Targets {
		targetID, err := parseUUID[domaingraph.NodeID](target.TargetNodeID, fmt.Sprintf("targets[%d].target_node_id", i))
		if err != nil {
			return domaingraph.ReplaceReferencesResult{}, err
		}
		if _, duplicate := targets[targetID]; duplicate {
			return domaingraph.ReplaceReferencesResult{}, fmt.Errorf("%w: duplicate reference target %s", ErrInvalidInput, targetID)
		}
		targets[targetID] = target
		targetOrder = append(targetOrder, targetID)
	}

	view, err := m.transactionEdgeView(ctx, tx)
	if err != nil {
		return domaingraph.ReplaceReferencesResult{}, err
	}
	existingOutgoing, err := view.outgoing(ctx, sourceID)
	if err != nil {
		return domaingraph.ReplaceReferencesResult{}, err
	}
	existingByTarget := map[domaingraph.NodeID][]domaingraph.Edge{}
	for _, edge := range existingOutgoing {
		if edge.DomainID != mustDomainID(tx.DomainID) || !domaingraph.EdgeHasLabels(edge, labels) {
			continue
		}
		existingByTarget[edge.ToID] = append(existingByTarget[edge.ToID], edge)
	}
	for targetID := range existingByTarget {
		sort.Slice(existingByTarget[targetID], func(i, j int) bool {
			return existingByTarget[targetID][i].ID.String() < existingByTarget[targetID][j].ID.String()
		})
	}

	var result domaingraph.ReplaceReferencesResult
	deleteEdge := func(edge domaingraph.Edge) error {
		deleted, err := m.DeleteEdge(ctx, tx, edge.ID.String())
		if err != nil {
			return err
		}
		result.DeletedEdgeIDs = append(result.DeletedEdgeIDs, deleted)
		return nil
	}

	if mode == domaingraph.ReferenceReplacementModeReplace {
		for targetID, edges := range existingByTarget {
			if _, desired := targets[targetID]; desired {
				continue
			}
			for _, edge := range edges {
				if err := deleteEdge(edge); err != nil {
					return domaingraph.ReplaceReferencesResult{}, err
				}
			}
		}
	}
	if mode == domaingraph.ReferenceReplacementModeRemove {
		if len(targets) == 0 {
			for _, edges := range existingByTarget {
				for _, edge := range edges {
					if err := deleteEdge(edge); err != nil {
						return domaingraph.ReplaceReferencesResult{}, err
					}
				}
			}
			return result, nil
		}
		for targetID := range targets {
			for _, edge := range existingByTarget[targetID] {
				if err := deleteEdge(edge); err != nil {
					return domaingraph.ReplaceReferencesResult{}, err
				}
			}
		}
		return result, nil
	}

	for _, targetID := range targetOrder {
		target := targets[targetID]
		existing := existingByTarget[targetID]
		if len(existing) == 0 {
			created, err := m.CreateEdge(ctx, tx, domaingraph.EdgeInput{EdgeID: target.EdgeID, FromNodeID: sourceID.String(), ToNodeID: targetID.String(), Labels: labels, Properties: target.Properties, Payload: target.Payload, Meta: target.Meta})
			if err != nil {
				return domaingraph.ReplaceReferencesResult{}, err
			}
			result.AddedEdges = append(result.AddedEdges, created)
			continue
		}
		kept := existing[0]
		for _, duplicate := range existing[1:] {
			if err := deleteEdge(duplicate); err != nil {
				return domaingraph.ReplaceReferencesResult{}, err
			}
		}
		update := domaingraph.UpdateEdgeInput{EdgeID: kept.ID.String()}
		if target.HasProps && !reflect.DeepEqual(kept.Properties, target.Properties) {
			update.Properties = target.Properties
			update.UpdateMask = append(update.UpdateMask, "properties")
		}
		if target.HasPayload && !reflect.DeepEqual(kept.Payload, target.Payload) {
			update.Payload = target.Payload
			update.UpdateMask = append(update.UpdateMask, "payload")
		}
		if target.HasMeta && !reflect.DeepEqual(kept.Meta, target.Meta) {
			update.Meta = target.Meta
			update.UpdateMask = append(update.UpdateMask, "meta")
		}
		if len(update.UpdateMask) == 0 {
			continue
		}
		updated, err := m.UpdateEdge(ctx, tx, update)
		if err != nil {
			return domaingraph.ReplaceReferencesResult{}, err
		}
		result.UpdatedEdges = append(result.UpdatedEdges, updated)
	}
	return result, nil
}

func (m *Module) ListChildren(ctx context.Context, tx sessionmodel.GraphTransaction, parentNodeID string) ([]domaingraph.Edge, error) {
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

func (m *Module) GetParent(ctx context.Context, tx sessionmodel.GraphTransaction, childNodeID string) (*domaingraph.Edge, error) {
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

func (m *Module) MoveSubtree(ctx context.Context, tx sessionmodel.GraphTransaction, nodeID string, newParentNodeID string, order *int32) (domaingraph.Edge, error) {
	lockedCtx, releaseMutation, lockErr := m.lockActiveTransactionMutation(ctx, tx)
	if lockErr != nil {
		return domaingraph.Edge{}, lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
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

func (m *Module) ReorderChildren(ctx context.Context, tx sessionmodel.GraphTransaction, parentNodeID string, childNodeIDs []string) ([]domaingraph.Edge, error) {
	lockedCtx, releaseMutation, lockErr := m.lockActiveTransactionMutation(ctx, tx)
	if lockErr != nil {
		return nil, lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
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
	stores, err := m.domainStoresForSpace(ctx, spaceID)
	if err != nil {
		return 0, err
	}
	var revision int64
	for _, store := range stores {
		if rev := int64(store.Revision()); rev > revision {
			revision = rev
		}
	}
	if read != nil {
		read.ObservedRevision = revision
		RecordStrongReadContext(ctx, read)
	}
	return revision, nil
}

func (m *Module) CurrentDomainRevision(ctx context.Context, spaceID string, domainID string) (int64, error) {
	if leader, forward, err := m.shouldForwardRaftGraphRead(spaceID); err != nil {
		return 0, err
	} else if forward {
		req := raftGraphReadRequest{Op: "current_revision", SpaceID: spaceID, DomainID: domainID}
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
	store, err := m.store(ctx, spaceID, domainID)
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

func (m *Module) CommitTransactionGraph(ctx context.Context, tx sessionmodel.GraphTransaction) (graphchange.CommitResult, error) {
	lockedCtx, releaseMutation, lockErr := m.lockTransactionMutation(ctx, tx.ID)
	if lockErr != nil {
		return graphchange.CommitResult{}, lockErr
	}
	ctx = lockedCtx
	defer releaseMutation()
	m.mu.Lock()
	finalized := m.finalizedTransactions[tx.ID]
	m.mu.Unlock()
	if finalized.kind == transactionCommitted {
		return finalized.commit, nil
	}
	if finalized.kind == transactionDiscarded {
		return graphchange.CommitResult{}, fmt.Errorf("%w: transaction graph was discarded", ErrInvalidState)
	}
	if err := m.validateTransactionActive(ctx, tx); err != nil {
		return graphchange.CommitResult{}, err
	}
	if tx.Mode == sessionmodel.TransactionModeReadOnly {
		m.mu.Lock()
		err := m.finalizeTransactionLocked(tx.ID, transactionFinalization{kind: transactionCommitted})
		m.mu.Unlock()
		return graphchange.CommitResult{}, err
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return graphchange.CommitResult{}, err
	}
	traceStart := time.Now()
	var timing graphCommitTiming
	stepStart := time.Now()
	release, err := m.enterWrite(ctx)
	if err != nil {
		return graphchange.CommitResult{}, err
	}
	timing.EnterWrite = time.Since(stepStart)
	defer release()
	stepStart = time.Now()
	m.mu.Lock()
	o := m.overlays[tx.ID]
	if o == nil || o.opCount == 0 {
		if tx.IdempotencyKey != "" {
			m.mu.Unlock()
			return graphchange.CommitResult{}, fmt.Errorf("%w: idempotent commit requires at least one graph mutation", ErrInvalidInput)
		}
		delete(m.overlays, tx.ID)
		err := m.finalizeTransactionLocked(tx.ID, transactionFinalization{kind: transactionCommitted})
		m.mu.Unlock()
		return graphchange.CommitResult{}, err
	}
	snapshot := o.clone()
	delete(m.overlays, tx.ID)
	m.mu.Unlock()
	timing.OverlaySnapshot = time.Since(stepStart)
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
	stepStart = time.Now()
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return graphchange.CommitResult{}, err
	}
	timing.StoreOpen = time.Since(stepStart)
	stepStart = time.Now()
	record := graphCommitRecordFromSnapshot(tx, snapshot)
	idempotency, err := newGraphCommitIdempotency(tx, record)
	if err != nil {
		return graphchange.CommitResult{}, err
	}
	record.Idempotency = idempotency
	timing.RecordBuild = time.Since(stepStart)
	releaseIdempotency := m.lockGraphCommitIdempotencyScope(tx)
	idempotencyLocked := true
	defer func() {
		if idempotencyLocked {
			releaseIdempotency()
		}
	}()
	if idempotency != nil {
		candidate, err := idempotency.storageRecord()
		if err != nil {
			return graphchange.CommitResult{}, err
		}
		existing, ok, err := store.CheckIdempotencyRecord(ctx, candidate)
		if err != nil {
			mappedErr := mapStorageError(err)
			return graphchange.CommitResult{}, m.observeIdempotencyError(idempotency, mappedErr)
		}
		if !ok {
			m.idempotencyReplayMisses.Add(1)
		} else {
			m.idempotencyReplayHits.Add(1)
			_ = m.deliverIdempotencyEvent(ctx, store, existing)
			commitResult, err := graphCommitResultFromIdempotency(existing, true)
			if err != nil {
				return graphchange.CommitResult{}, err
			}
			restoreOverlay = false
			if err := m.finalizeCommittedTransactionGraph(tx.ID, commitResult); err != nil {
				return graphchange.CommitResult{}, err
			}
			return commitResult, nil
		}
	}
	stepStart = time.Now()
	changes, err := m.overlayChanges(ctx, store, snapshot)
	if err != nil {
		return graphchange.CommitResult{}, err
	}
	timing.OverlayChanges = time.Since(stepStart)
	stepStart = time.Now()
	graphEvent, err := m.graphChangeEvent(ctx, tx, store, snapshot)
	if err != nil {
		return graphchange.CommitResult{}, err
	}
	if err := attachIdempotencyEvent(record.Idempotency, graphEvent, changes); err != nil {
		return graphchange.CommitResult{}, err
	}
	timing.GraphEvent = time.Since(stepStart)
	stepStart = time.Now()
	if err := m.validateBlobReferences(ctx, tx.SpaceID, record.PutNodes); err != nil {
		return graphchange.CommitResult{}, err
	}
	timing.BlobReferenceValidate = time.Since(stepStart)
	var committedRevision int64
	var info graphstorage.CommitInfo
	if m.raftGroups != nil {
		timing.StorageMode = "raft"
		stepStart = time.Now()
		cmd, err := m.buildGraphCommitRaftCommand(record, m.raftPartitionCount, graphRaftCommandIDForRecord(ctx, tx.ID, record))
		if err != nil {
			return graphchange.CommitResult{}, err
		}
		timing.RaftBuild = time.Since(stepStart)
		stepStart = time.Now()
		proposal, err := m.proposeGraphRaftCommand(ctx, cmd)
		if err != nil {
			return graphchange.CommitResult{}, m.observeIdempotencyError(record.Idempotency, err)
		}
		timing.RaftPropose = time.Since(stepStart)
		timing.RaftProposal = proposal.Timing
		// proposeGraphRaftCommand returns only after the local partition leader has
		// applied the committed graph command. Reading the local store revision here
		// avoids a second leader/read-index check that can fail during an immediate
		// post-commit leader change even though the write is already committed and
		// applied locally.
		committedRevision = int64(store.Revision())
		info = graphstorage.CommitInfo{TxnID: uuid.New(), NextRevision: uint64(committedRevision)}
		if record.Idempotency != nil {
			idempotencyRecord, ok, err := store.GetIdempotencyRecord(ctx, record.Idempotency.Scope, record.Idempotency.Key)
			if err != nil {
				return graphchange.CommitResult{}, mapStorageError(err)
			}
			if !ok {
				return graphchange.CommitResult{}, m.observeIdempotencyError(record.Idempotency, ErrIdempotencyCapacity)
			}
			if idempotencyRecord.Digest != record.Idempotency.Digest {
				return graphchange.CommitResult{}, m.observeIdempotencyError(record.Idempotency, ErrIdempotencyConflict)
			}
			storedResult, err := graphCommitResultFromIdempotency(idempotencyRecord, false)
			if err != nil {
				return graphchange.CommitResult{}, err
			}
			info.Idempotency = &idempotencyRecord
			info.IdempotentReplay = storedResult.CommitID != record.Idempotency.Metadata.CommitID
			committedRevision = int64(idempotencyRecord.CommittedRevision)
		}
	} else if m.wal == nil {
		timing.StorageMode = "direct"
		stepStart = time.Now()
		storageTx, err := store.Begin(ctx)
		if err != nil {
			return graphchange.CommitResult{}, mapStorageError(err)
		}
		if record.Idempotency != nil {
			idempotencyRecord, err := record.Idempotency.storageRecord()
			if err != nil {
				_ = storageTx.Rollback()
				return graphchange.CommitResult{}, err
			}
			storageTx.SetIdempotency(idempotencyRecord)
		}
		storageTx.ExpectRevision(uint64(tx.BaseRevision))
		for _, node := range record.PutNodes {
			if err := storageTx.PutNode(node); err != nil {
				_ = storageTx.Rollback()
				return graphchange.CommitResult{}, mapStorageError(err)
			}
		}
		for _, edge := range record.PutEdges {
			if err := storageTx.PutEdge(edge); err != nil {
				_ = storageTx.Rollback()
				return graphchange.CommitResult{}, mapStorageError(err)
			}
		}
		for _, id := range record.DeleteNodeIDs {
			if err := storageTx.DeleteNode(id); err != nil {
				_ = storageTx.Rollback()
				return graphchange.CommitResult{}, mapStorageError(err)
			}
		}
		for _, id := range record.DeleteEdgeIDs {
			if err := storageTx.DeleteEdge(id); err != nil {
				_ = storageTx.Rollback()
				return graphchange.CommitResult{}, mapStorageError(err)
			}
		}
		info, err = storageTx.CommitWithInfo()
		if err != nil {
			mappedErr := mapStorageError(err)
			return graphchange.CommitResult{}, m.observeIdempotencyError(record.Idempotency, mappedErr)
		}
		committedRevision = int64(store.Revision())
		timing.DirectStorage = time.Since(stepStart)
		timing.StorageCommit = info.Timing
	} else {
		timing.StorageMode = "wal"
		stepStart = time.Now()
		payload, err := json.Marshal(record)
		if err != nil {
			return graphchange.CommitResult{}, err
		}
		timing.WALMarshal = time.Since(stepStart)
		stepStart = time.Now()
		lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeGraphCommit, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
		if err != nil {
			return graphchange.CommitResult{}, err
		}
		timing.WALAppend = time.Since(stepStart)
		stepStart = time.Now()
		if err := m.wal.Sync(ctx, lsn); err != nil {
			return graphchange.CommitResult{}, err
		}
		timing.WALSync = time.Since(stepStart)
		stepStart = time.Now()
		committedRevision, _, info, err = m.applyGraphCommitRecord(ctx, record)
		if err != nil {
			return graphchange.CommitResult{}, m.observeIdempotencyError(record.Idempotency, err)
		}
		timing.WALApply = time.Since(stepStart)
		timing.StorageCommit = info.Timing
		stepStart = time.Now()
		if err := m.markWALApplied(ctx, lsn); err != nil {
			return graphchange.CommitResult{}, err
		}
		timing.WALMarkApplied = time.Since(stepStart)
		info.TxnID = uuid.New()
		info.NextRevision = uint64(committedRevision)
	}
	restoreOverlay = false
	commitResult := graphchange.CommitResult{OperationCount: snapshot.opCount, CommittedRevision: committedRevision, Changes: changes}
	if info.Idempotency != nil {
		if info.IdempotentReplay {
			m.idempotencyReplayHits.Add(1)
		}
		commitResult, err = graphCommitResultFromIdempotency(*info.Idempotency, info.IdempotentReplay)
		if err != nil {
			return graphchange.CommitResult{}, err
		}
		if !info.IdempotentReplay {
			commitResult.Changes = changes
		}
	}
	if err := m.finalizeCommittedTransactionGraph(tx.ID, commitResult); err != nil {
		return graphchange.CommitResult{}, err
	}
	if info.Idempotency != nil {
		stepStart = time.Now()
		recorder := &graphSinkTimingRecorder{}
		_ = m.deliverIdempotencyEvent(withGraphSinkTimingRecorder(ctx, recorder), store, *info.Idempotency)
		timing.ChangeSink = time.Since(stepStart)
		timing.SinkTimings = recorder.timings
	} else if !info.IdempotentReplay {
		graphEvent.Changes = changes
		stepStart = time.Now()
		recorder := &graphSinkTimingRecorder{}
		m.notifyGraphChangeSink(withGraphSinkTimingRecorder(ctx, recorder), info, graphEvent)
		timing.ChangeSink = time.Since(stepStart)
		timing.SinkTimings = recorder.timings
	}
	releaseIdempotency()
	idempotencyLocked = false
	timing.Total = time.Since(traceStart)
	m.logGraphCommitTiming(tx, map[string]int{"operation_count": int(snapshot.opCount), "put_nodes": len(record.PutNodes), "put_edges": len(record.PutEdges), "delete_nodes": len(record.DeleteNodeIDs), "delete_edges": len(record.DeleteEdgeIDs), "changes": len(changes)}, committedRevision, timing)
	return commitResult, nil
}

func (m *Module) finalizeCommittedTransactionGraph(transactionID string, commit graphchange.CommitResult) error {
	cached := commit
	cached.Changes = nil
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.finalizeTransactionLocked(transactionID, transactionFinalization{kind: transactionCommitted, commit: cached})
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

func (m *Module) notifyGraphChangeSink(ctx context.Context, info graphstorage.CommitInfo, event graphchange.CommittedEvent) (bool, error) {
	m.mu.Lock()
	sink := m.changeSink
	m.mu.Unlock()
	return m.notifyChangeSink(ctx, sink, info, event)
}

func (m *Module) notifyRaftApplyChangeSink(ctx context.Context, info graphstorage.CommitInfo, event graphchange.CommittedEvent) (bool, error) {
	m.mu.Lock()
	sink := m.raftApplyChangeSink
	m.mu.Unlock()
	return m.notifyChangeSink(ctx, sink, info, event)
}

func (m *Module) notifyChangeSink(ctx context.Context, sink graphchange.Sink, info graphstorage.CommitInfo, event graphchange.CommittedEvent) (bool, error) {
	if event.Empty() {
		return true, nil
	}
	if sink == nil {
		return false, nil
	}
	if event.TxnID == uuid.Nil {
		event.TxnID = info.TxnID
	}
	if event.TransactionID == uuid.Nil {
		event.TransactionID = event.TxnID
	}
	if event.GraphRevision == 0 {
		event.GraphRevision = info.NextRevision
	}
	if event.Revision == 0 {
		event.Revision = info.NextRevision
	}
	event.Normalize()
	if err := sink.OnGraphCommitted(ctx, event); err != nil {
		m.mu.Lock()
		m.lastGraphChangeSinkErr = err
		m.mu.Unlock()
		return false, err
	}
	m.mu.Lock()
	m.lastGraphChangeSinkErr = nil
	m.mu.Unlock()
	return true, nil
}

func (m *Module) graphChangeEvent(ctx context.Context, tx sessionmodel.GraphTransaction, store *graphstorage.LocalStore, snapshot *overlay) (graphchange.CommittedEvent, error) {
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

func (m *Module) overlayChanges(ctx context.Context, store *graphstorage.LocalStore, snapshot *overlay) ([]graphchange.Change, error) {
	changes := []graphchange.Change{}
	for _, node := range sortedNodes(snapshot.putNodes) {
		changeType := graphchange.ChangeTypeNodeUpdated
		var oldCopy *domaingraph.Node
		if old, err := store.GetNode(ctx, node.ID); errors.Is(err, graphstorage.ErrNotFound) {
			changeType = graphchange.ChangeTypeNodeCreated
		} else if err != nil {
			return nil, mapStorageError(err)
		} else {
			copy := cloneNode(old)
			oldCopy = &copy
		}
		copy := cloneNode(node)
		changes = append(changes, graphchange.Change{Type: changeType, Node: &copy, OldNode: oldCopy, NodeID: node.ID.String(), AffectedNodeIDs: []string{node.ID.String()}})
	}
	for _, edge := range sortedEdges(snapshot.putEdges) {
		changeType := graphchange.ChangeTypeEdgeUpdated
		var oldCopy *domaingraph.Edge
		affectedNodeIDs := []string{edge.FromID.String(), edge.ToID.String()}
		if old, err := store.GetEdge(ctx, edge.ID); errors.Is(err, graphstorage.ErrNotFound) {
			changeType = graphchange.ChangeTypeEdgeCreated
		} else if err != nil {
			return nil, mapStorageError(err)
		} else {
			copy := cloneEdge(old)
			oldCopy = &copy
			affectedNodeIDs = appendUniqueStrings(affectedNodeIDs, old.FromID.String(), old.ToID.String())
		}
		copy := cloneEdge(edge)
		changes = append(changes, graphchange.Change{Type: changeType, Edge: &copy, OldEdge: oldCopy, EdgeID: edge.ID.String(), AffectedNodeIDs: affectedNodeIDs, AffectedEdgeIDs: []string{edge.ID.String()}})
	}
	for _, id := range sortedNodeIDs(snapshot.deleteNodes) {
		var oldCopy *domaingraph.Node
		if old, err := store.GetNode(ctx, id); err == nil {
			copy := cloneNode(old)
			oldCopy = &copy
		} else if err != nil && !errors.Is(err, graphstorage.ErrNotFound) {
			return nil, mapStorageError(err)
		}
		changes = append(changes, graphchange.Change{Type: graphchange.ChangeTypeNodeDeleted, OldNode: oldCopy, NodeID: id.String(), AffectedNodeIDs: []string{id.String()}})
	}
	for _, id := range sortedEdgeIDs(snapshot.deleteEdges) {
		change := graphchange.Change{Type: graphchange.ChangeTypeEdgeDeleted, EdgeID: id.String(), AffectedEdgeIDs: []string{id.String()}}
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

func (m *Module) DiscardTransactionGraph(ctx context.Context, transactionID string) error {
	_, releaseMutation, err := m.lockTransactionMutation(context.WithoutCancel(ctx), transactionID)
	if err != nil {
		return err
	}
	defer releaseMutation()
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.finalizeTransactionLocked(transactionID, transactionFinalization{kind: transactionDiscarded}); err != nil {
		return err
	}
	delete(m.overlays, transactionID)
	return nil
}

// AcknowledgeTransactionFinalization removes the short-lived terminal race
// barrier only after the authoritative session state is no longer active.
func (m *Module) AcknowledgeTransactionFinalization(ctx context.Context, tx sessionmodel.GraphTransaction) error {
	lockedCtx, releaseMutation, err := m.lockTransactionMutation(context.WithoutCancel(ctx), tx.ID)
	if err != nil {
		return err
	}
	defer releaseMutation()
	m.mu.Lock()
	_, finalized := m.finalizedTransactions[tx.ID]
	validator := m.transactionValidator
	m.mu.Unlock()
	if !finalized {
		return nil
	}
	if validator != nil {
		if err := validator.ValidateTransactionFinalized(lockedCtx, tx); err != nil {
			return fmt.Errorf("%w: transaction finalization is not authoritative: %v", ErrInvalidState, err)
		}
	}
	m.mu.Lock()
	delete(m.finalizedTransactions, tx.ID)
	m.mu.Unlock()
	return nil
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
	stores, err := m.domainStoresForSpace(ctx, spaceID)
	if err != nil {
		return 0, err
	}
	var observedRevision int64
	count := 0
	for _, store := range stores {
		if rev := int64(store.Revision()); rev > observedRevision {
			observedRevision = rev
		}
		storeCount, err := store.BlobRefCount(ctx, id)
		if err != nil {
			return 0, mapStorageError(err)
		}
		count += storeCount
	}
	if read != nil {
		read.ObservedRevision = observedRevision
		RecordStrongReadContext(ctx, read)
	}
	return count, nil
}

func (m *Module) store(ctx context.Context, spaceID string, domainID string) (*graphstorage.LocalStore, error) {
	key, err := newDomainStoreKey(spaceID, domainID)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if store := m.stores[key]; store != nil {
		return store, nil
	}
	store, err := graphstorage.OpenWithOptions(ctx, m.domainStorePath(key), graphstorage.Options{Encryption: m.encryption})
	if err != nil {
		return nil, err
	}
	m.stores[key] = store
	return store, nil
}

func (m *Module) existingDomainStore(ctx context.Context, key domainStoreKey) (*graphstorage.LocalStore, error) {
	m.mu.Lock()
	if store := m.stores[key]; store != nil {
		m.mu.Unlock()
		return store, nil
	}
	m.mu.Unlock()
	storePath := m.domainStorePath(key)
	if err := validateExistingGraphStoreForReadOnlyOpen(storePath); err != nil {
		return nil, err
	}
	store, err := graphstorage.OpenWithOptions(ctx, storePath, graphstorage.Options{Encryption: m.encryption})
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if existing := m.stores[key]; existing != nil {
		m.mu.Unlock()
		_ = store.Close()
		return existing, nil
	}
	m.stores[key] = store
	m.mu.Unlock()
	return store, nil
}

func (m *Module) domainStoresForSpace(ctx context.Context, spaceID string) ([]*graphstorage.LocalStore, error) {
	spaceID = strings.TrimSpace(spaceID)
	if spaceID == "" {
		return nil, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	keys := map[domainStoreKey]struct{}{}
	m.mu.Lock()
	for key := range m.stores {
		if key.SpaceID == spaceID {
			keys[key] = struct{}{}
		}
	}
	m.mu.Unlock()
	domainsDir := filepath.Join(m.dataDir, spaceID, "domains")
	entries, err := os.ReadDir(domainsDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			key, keyErr := newDomainStoreKey(spaceID, entry.Name())
			if keyErr != nil {
				continue
			}
			if _, statErr := os.Stat(filepath.Join(m.domainStorePath(key), "manifest.mycel")); statErr == nil {
				keys[key] = struct{}{}
			} else if !os.IsNotExist(statErr) {
				return nil, statErr
			}
		}
	}
	stores := make([]*graphstorage.LocalStore, 0, len(keys))
	for key := range keys {
		store, err := m.existingDomainStore(ctx, key)
		if err != nil {
			return nil, err
		}
		stores = append(stores, store)
	}
	return stores, nil
}

func (m *Module) domainStorePath(key domainStoreKey) string {
	return filepath.Join(m.dataDir, key.SpaceID, "domains", key.DomainID)
}

func newDomainStoreKey(spaceID string, domainID string) (domainStoreKey, error) {
	spaceID = strings.TrimSpace(spaceID)
	domainID = strings.TrimSpace(domainID)
	if spaceID == "" {
		return domainStoreKey{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	if domainID == "" {
		return domainStoreKey{}, fmt.Errorf("%w: domain_id is required", ErrInvalidInput)
	}
	parsedDomain, err := uuid.Parse(domainID)
	if err != nil || parsedDomain == uuid.Nil {
		return domainStoreKey{}, fmt.Errorf("%w: domain_id must be a UUID", ErrInvalidInput)
	}
	return domainStoreKey{SpaceID: spaceID, DomainID: parsedDomain.String()}, nil
}

func (m *Module) node(ctx context.Context, tx sessionmodel.GraphTransaction, id domaingraph.NodeID) (domaingraph.Node, error) {
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
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return domaingraph.Node{}, err
	}
	var node domaingraph.Node
	if revision, ok := snapshotRevision(tx); ok {
		node, err = store.GetNodeAt(ctx, id, revision)
	} else {
		node, err = store.GetNode(ctx, id)
	}
	if err != nil {
		return domaingraph.Node{}, mapStorageError(err)
	}
	if node.DomainID != mustDomainID(tx.DomainID) {
		return domaingraph.Node{}, ErrNotFound
	}
	return cloneNode(node), nil
}

func (m *Module) edge(ctx context.Context, tx sessionmodel.GraphTransaction, id domaingraph.EdgeID) (domaingraph.Edge, error) {
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
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return domaingraph.Edge{}, err
	}
	var edge domaingraph.Edge
	if revision, ok := snapshotRevision(tx); ok {
		edge, err = store.GetEdgeAt(ctx, id, revision)
	} else {
		edge, err = store.GetEdge(ctx, id)
	}
	if err != nil {
		return domaingraph.Edge{}, mapStorageError(err)
	}
	return cloneEdge(edge), nil
}

type transactionEdgeView struct {
	store      *graphstorage.LocalStore
	overlay    *overlay
	revision   uint64
	snapshoted bool
}

func (m *Module) transactionEdgeView(ctx context.Context, tx sessionmodel.GraphTransaction) (transactionEdgeView, error) {
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return transactionEdgeView{}, err
	}
	m.mu.Lock()
	var snapshot *overlay
	if o := m.overlays[tx.ID]; o != nil {
		snapshot = o.clone()
	}
	m.mu.Unlock()
	revision, snapshoted := snapshotRevision(tx)
	return transactionEdgeView{store: store, overlay: snapshot, revision: revision, snapshoted: snapshoted}, nil
}

func (v transactionEdgeView) incoming(ctx context.Context, nodeID domaingraph.NodeID) ([]domaingraph.Edge, error) {
	var base []domaingraph.Edge
	var err error
	if v.snapshoted {
		base, err = v.store.IncomingEdgesAt(ctx, nodeID, v.revision)
	} else {
		base, err = v.store.IncomingEdges(ctx, nodeID)
	}
	if err != nil {
		return nil, mapStorageError(err)
	}
	return v.mergeEndpointEdges(base, func(edge domaingraph.Edge) bool { return edge.ToID == nodeID }), nil
}

func (v transactionEdgeView) outgoing(ctx context.Context, nodeID domaingraph.NodeID) ([]domaingraph.Edge, error) {
	var base []domaingraph.Edge
	var err error
	if v.snapshoted {
		base, err = v.store.OutgoingEdgesAt(ctx, nodeID, v.revision)
	} else {
		base, err = v.store.OutgoingEdges(ctx, nodeID)
	}
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

func (m *Module) hierarchyPathExists(ctx context.Context, tx sessionmodel.GraphTransaction, from domaingraph.NodeID, target domaingraph.NodeID) (bool, error) {
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

func (m *Module) scanNodesLocal(ctx context.Context, tx sessionmodel.GraphTransaction, request graphquery.NodeScan) ([]domaingraph.Node, string, graphquery.IndexedReadStats, error) {
	stats := graphquery.IndexedReadStats{Plan: "NodeScan", IndexName: "node_id", FullScan: true, NextCursorKind: "node_key"}
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return nil, "", stats, err
	}
	pageSize := boundedGraphPageSize(request.Limit)
	pageToken := strings.TrimSpace(request.Cursor)
	examineLimit := request.ExamineLimit
	if examineLimit <= 0 {
		examineLimit = pageSize
	} else if examineLimit > 500 {
		examineLimit = 500
	}
	// Numeric offsets remain readable as a compatibility path for tokens issued
	// before the cursor-driven node scan. New scans always emit nodes-v1 tokens.
	if pageToken != "" && !strings.HasPrefix(pageToken, nodePageTokenPrefix) {
		if _, err := pageStart(pageToken); err != nil {
			return nil, "", stats, err
		}
		m.mu.Lock()
		overlay := m.overlays[tx.ID]
		mutableView := overlay != nil && overlay.opCount != 0
		m.mu.Unlock()
		if mutableView {
			return nil, "", stats, fmt.Errorf("%w: legacy page_token is not bound to the transaction overlay", ErrInvalidInput)
		}
		var base []domaingraph.Node
		if revision, ok := snapshotRevision(tx); ok {
			base, err = store.ListNodesByDomainAt(ctx, mustDomainID(tx.DomainID), revision)
		} else {
			base, err = store.ListNodesByDomain(ctx, mustDomainID(tx.DomainID))
		}
		if err != nil {
			return nil, "", stats, mapStorageError(err)
		}
		m.mu.Lock()
		overlay = m.overlays[tx.ID]
		if overlay != nil && overlay.opCount != 0 {
			m.mu.Unlock()
			return nil, "", stats, fmt.Errorf("%w: legacy page_token is not bound to the transaction overlay", ErrInvalidInput)
		}
		merged := mergeNodes(base, overlay, mustDomainID(tx.DomainID))
		m.mu.Unlock()
		out, next, pageErr := paginateNodes(merged, pageSize, pageToken)
		stats.IndexEntriesScanned = len(base)
		stats.NodesLoaded = len(out)
		return out, next, stats, pageErr
	}

	cursor, err := decodeNodePageCursor(tx, pageToken)
	if err != nil {
		return nil, "", stats, err
	}
	overlay, err := m.nodeOverlayPageSnapshot(tx, cursor, pageSize+1)
	if err != nil {
		return nil, "", stats, err
	}
	scan := graphstorage.DomainNodeScan{
		DomainID: mustDomainID(tx.DomainID), Limit: pageSize, Cursor: cursor.AfterNodeID, ExamineLimit: examineLimit,
	}
	if revision, ok := snapshotRevision(tx); ok {
		scan.Snapshot = true
		scan.Revision = revision
	}
	if len(overlay.replaced) == 0 && len(overlay.deleted) == 0 {
		items, storageNext, examined, scanErr := store.ScanNodesByDomain(ctx, scan)
		stats.IndexEntriesScanned += examined
		stats.NodesLoaded = len(items)
		if scanErr != nil {
			return nil, "", stats, mapStorageError(scanErr)
		}
		if storageNext == "" {
			return items, "", stats, nil
		}
		next := encodeNodePageCursor(tx, nodePageCursor{OverlayOps: overlay.operations, AfterNodeID: storageNext})
		return items, next, stats, nil
	}

	// A mutable node overlay requires a finite merge lookahead. The current
	// committed order excludes tombstones; overlay identity prevents
	// continuation if the merge input changes.
	scan.Limit = pageSize + 1
	scan.ExamineLimit = min(500, pageSize+1+len(overlay.replaced)+len(overlay.deleted))
	items := make([]domaingraph.Node, 0, pageSize+1+len(overlay.putNodes))
	storageCursor := scan.Cursor
	for len(items) < pageSize+1 {
		scan.Cursor = storageCursor
		scan.Limit = min(500, pageSize+1-len(items))
		base, next, examined, scanErr := store.ScanNodesByDomain(ctx, scan)
		stats.IndexEntriesScanned += examined
		stats.NodesLoaded += len(base)
		if scanErr != nil {
			return nil, "", stats, mapStorageError(scanErr)
		}
		for _, node := range base {
			if _, deleted := overlay.deleted[node.ID]; deleted {
				continue
			}
			if _, replaced := overlay.replaced[node.ID]; replaced {
				continue
			}
			items = append(items, node)
		}
		if next == "" {
			break
		}
		storageCursor = next
	}
	items = append(items, overlay.putNodes...)
	stats.IndexEntriesScanned += len(overlay.replaced) + len(overlay.deleted)
	stats.NodesLoaded += len(overlay.putNodes)
	sort.Slice(items, func(i, j int) bool { return items[i].ID.String() < items[j].ID.String() })
	if len(items) <= pageSize {
		return items, "", stats, nil
	}
	items = items[:pageSize]
	next := encodeNodePageCursor(tx, nodePageCursor{OverlayOps: overlay.operations, AfterNodeID: items[len(items)-1].ID.String()})
	return items, next, stats, nil
}

func boundedGraphPageSize(pageSize int) int {
	if pageSize <= 0 || pageSize > 500 {
		return 500
	}
	return pageSize
}

func (m *Module) nodeOverlayPageSnapshot(tx sessionmodel.GraphTransaction, cursor nodePageCursor, limit int) (nodeOverlayPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	page := nodeOverlayPage{replaced: map[domaingraph.NodeID]struct{}{}, deleted: map[domaingraph.NodeID]struct{}{}}
	overlay := m.overlays[tx.ID]
	if overlay == nil {
		if cursor.AfterNodeID != "" && cursor.OverlayOps != 0 {
			return nodeOverlayPage{}, fmt.Errorf("%w: page_token transaction overlay changed", ErrInvalidInput)
		}
		return page, nil
	}
	page.operations = overlay.opCount
	if cursor.AfterNodeID != "" && cursor.OverlayOps != overlay.opCount {
		return nodeOverlayPage{}, fmt.Errorf("%w: page_token transaction overlay changed", ErrInvalidInput)
	}
	ids := make([]domaingraph.NodeID, 0, len(overlay.putNodes))
	domainID := mustDomainID(tx.DomainID)
	for id, node := range overlay.putNodes {
		page.replaced[id] = struct{}{}
		if node.DomainID == domainID && (cursor.AfterNodeID == "" || id.String() > cursor.AfterNodeID) {
			ids = append(ids, id)
		}
	}
	for id := range overlay.deleteNodes {
		page.deleted[id] = struct{}{}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	if len(ids) > limit {
		ids = ids[:limit]
	}
	page.putNodes = make([]domaingraph.Node, 0, len(ids))
	for _, id := range ids {
		page.putNodes = append(page.putNodes, cloneNode(overlay.putNodes[id]))
	}
	return page, nil
}

func decodeNodePageCursor(tx sessionmodel.GraphTransaction, token string) (nodePageCursor, error) {
	if token == "" {
		return nodePageCursor{}, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, nodePageTokenPrefix))
	if err != nil {
		return nodePageCursor{}, fmt.Errorf("%w: invalid page_token", ErrInvalidInput)
	}
	var cursor nodePageCursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return nodePageCursor{}, fmt.Errorf("%w: invalid page_token", ErrInvalidInput)
	}
	want := nodePageCursorChecksum(cursor)
	if subtle.ConstantTimeCompare([]byte(cursor.Checksum), []byte(want)) != 1 || cursor.TransactionID != tx.ID || cursor.BaseRevision != tx.BaseRevision {
		return nodePageCursor{}, fmt.Errorf("%w: page_token does not match transaction view", ErrInvalidInput)
	}
	if _, err := uuid.Parse(cursor.AfterNodeID); err != nil {
		return nodePageCursor{}, fmt.Errorf("%w: invalid page_token", ErrInvalidInput)
	}
	return cursor, nil
}

func encodeNodePageCursor(tx sessionmodel.GraphTransaction, cursor nodePageCursor) string {
	cursor.TransactionID = tx.ID
	cursor.BaseRevision = tx.BaseRevision
	cursor.Checksum = nodePageCursorChecksum(cursor)
	payload, _ := json.Marshal(cursor)
	return nodePageTokenPrefix + base64.RawURLEncoding.EncodeToString(payload)
}

func nodePageCursorChecksum(cursor nodePageCursor) string {
	cursor.Checksum = ""
	payload, _ := json.Marshal(cursor)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (m *Module) listEdgesLocal(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Edge, string, error) {
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return nil, "", err
	}
	var base []domaingraph.Edge
	if revision, ok := snapshotRevision(tx); ok {
		base, err = store.ListEdgesAt(ctx, revision)
	} else {
		base, err = store.ListEdges(ctx)
	}
	if err != nil {
		return nil, "", mapStorageError(err)
	}
	m.mu.Lock()
	o := m.overlays[tx.ID]
	merged := mergeEdges(base, o)
	m.mu.Unlock()
	return paginateEdges(merged, pageSize, pageToken)
}

func (m *Module) listChildrenLocal(ctx context.Context, tx sessionmodel.GraphTransaction, parentNodeID string) ([]domaingraph.Edge, error) {
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

func (m *Module) parentEdge(ctx context.Context, tx sessionmodel.GraphTransaction, childID domaingraph.NodeID) (*domaingraph.Edge, error) {
	return m.parentEdgeLocal(ctx, tx, childID.String())
}

func (m *Module) parentEdgeLocal(ctx context.Context, tx sessionmodel.GraphTransaction, childNodeID string) (*domaingraph.Edge, error) {
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

func (m *Module) stageNode(ctx context.Context, tx sessionmodel.GraphTransaction, node domaingraph.Node) error {
	return m.stageNodeWithFence(ctx, tx, node, false)
}

func (m *Module) stageNodeWithFence(ctx context.Context, tx sessionmodel.GraphTransaction, node domaingraph.Node, automationOutputFence bool) error {
	if node.DomainID.String() != strings.TrimSpace(tx.DomainID) {
		return fmt.Errorf("%w: node domain_id %s does not match transaction domain_id %s", ErrInvalidInput, node.DomainID, tx.DomainID)
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(tx.ID)
	m.recordNodeMutationLocked(tx.ID, o, node.ID)
	delete(o.deleteNodes, node.ID)
	o.putNodes[node.ID] = cloneNode(node)
	if automationOutputFence {
		o.automationFenceNodes[node.ID] = struct{}{}
	}
	o.opCount++
	return nil
}

func (m *Module) stageNodeDelete(ctx context.Context, tx sessionmodel.GraphTransaction, id domaingraph.NodeID) error {
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(tx.ID)
	m.recordNodeMutationLocked(tx.ID, o, id)
	delete(o.putNodes, id)
	delete(o.automationFenceNodes, id)
	o.deleteNodes[id] = struct{}{}
	o.opCount++
	return nil
}

func (m *Module) stageEdge(ctx context.Context, tx sessionmodel.GraphTransaction, edge domaingraph.Edge) error {
	return m.stageEdgeWithFence(ctx, tx, edge, false)
}

func (m *Module) stageEdgeWithFence(ctx context.Context, tx sessionmodel.GraphTransaction, edge domaingraph.Edge, automationOutputFence bool) error {
	if edge.DomainID.String() != strings.TrimSpace(tx.DomainID) {
		return fmt.Errorf("%w: edge domain_id %s does not match transaction domain_id %s", ErrInvalidInput, edge.DomainID, tx.DomainID)
	}
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(tx.ID)
	m.recordEdgeMutationLocked(tx.ID, o, edge.ID)
	delete(o.deleteEdges, edge.ID)
	o.putEdges[edge.ID] = cloneEdge(edge)
	if automationOutputFence {
		o.automationFenceEdges[edge.ID] = struct{}{}
	}
	o.opCount++
	return nil
}

func (m *Module) stageEdgeDelete(ctx context.Context, tx sessionmodel.GraphTransaction, id domaingraph.EdgeID) error {
	if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.overlay(tx.ID)
	m.recordEdgeMutationLocked(tx.ID, o, id)
	delete(o.putEdges, id)
	delete(o.automationFenceEdges, id)
	o.deleteEdges[id] = struct{}{}
	o.opCount++
	return nil
}

func (m *Module) overlay(txID string) *overlay {
	o := m.overlays[txID]
	if o == nil {
		o = newOverlay()
		m.overlays[txID] = o
	}
	return o
}

func newOverlay() *overlay {
	return &overlay{putNodes: map[domaingraph.NodeID]domaingraph.Node{}, deleteNodes: map[domaingraph.NodeID]struct{}{}, putEdges: map[domaingraph.EdgeID]domaingraph.Edge{}, deleteEdges: map[domaingraph.EdgeID]struct{}{}, automationFenceNodes: map[domaingraph.NodeID]struct{}{}, automationFenceEdges: map[domaingraph.EdgeID]struct{}{}}
}

func (m *Module) boundedOrderedNodeOverlay(ctx context.Context, txID string, requestedLimit int, requestedExamine int) (*overlay, int, error) {
	pageLimit, examineLimit := boundedOverlayLimits(requestedLimit, requestedExamine)
	m.mu.Lock()
	defer m.mu.Unlock()
	source := m.overlays[txID]
	if source == nil || len(source.putNodes)+len(source.deleteNodes) == 0 {
		return nil, pageLimit, nil
	}
	affected := len(source.putNodes) + len(source.deleteNodes)
	storageLimit := pageLimit + affected
	requiredExamine := storageLimit + affected
	if storageLimit > 500 || requiredExamine > examineLimit {
		return nil, 0, fmt.Errorf("%w: ordered node overlay requires examining %d entries (limit %d)", graphquery.ErrScanBudgetExceeded, requiredExamine, examineLimit)
	}
	out := newOverlay()
	for id, node := range source.putNodes {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		// Staged values are immutable snapshots replaced atomically under m.mu;
		// copy the struct here and clone only rows selected for output.
		out.putNodes[id] = node
	}
	for id := range source.deleteNodes {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		out.deleteNodes[id] = struct{}{}
	}
	return out, storageLimit, nil
}

func (m *Module) boundedAdjacencyOverlay(ctx context.Context, txID string, requestedLimit int, requestedExamine int) (*overlay, int, error) {
	pageLimit, examineLimit := boundedOverlayLimits(requestedLimit, requestedExamine)
	m.mu.Lock()
	defer m.mu.Unlock()
	source := m.overlays[txID]
	if source == nil || len(source.putEdges)+len(source.deleteEdges) == 0 {
		return nil, pageLimit, nil
	}
	affected := len(source.putEdges) + len(source.deleteEdges)
	storageLimit := pageLimit + affected
	requiredExamine := storageLimit + affected
	if storageLimit > 500 || requiredExamine > examineLimit {
		return nil, 0, fmt.Errorf("%w: adjacency overlay requires examining %d entries (limit %d)", graphquery.ErrScanBudgetExceeded, requiredExamine, examineLimit)
	}
	out := newOverlay()
	for id, edge := range source.putEdges {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		// Staged values are immutable snapshots replaced atomically under m.mu;
		// copy the struct here and clone only rows selected for output.
		out.putEdges[id] = edge
	}
	for id := range source.deleteEdges {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		out.deleteEdges[id] = struct{}{}
	}
	return out, storageLimit, nil
}

func boundedOverlayLimits(requestedLimit int, requestedExamine int) (int, int) {
	pageLimit := requestedLimit
	if pageLimit <= 0 || pageLimit > 500 {
		pageLimit = 500
	}
	if requestedExamine <= 0 {
		requestedExamine = pageLimit * 16
	}
	if pageLimit > requestedExamine {
		pageLimit = requestedExamine
	}
	return pageLimit, requestedExamine
}

func (o *overlay) clone() *overlay {
	out := newOverlay()
	out.opCount = o.opCount
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
	for id := range o.automationFenceNodes {
		out.automationFenceNodes[id] = struct{}{}
	}
	for id := range o.automationFenceEdges {
		out.automationFenceEdges[id] = struct{}{}
	}
	return out
}

func snapshotRevision(tx sessionmodel.GraphTransaction) (uint64, bool) {
	if tx.Mode != sessionmodel.TransactionModeReadOnly {
		return 0, false
	}
	if tx.BaseRevision <= 0 {
		return 0, true
	}
	return uint64(tx.BaseRevision), true
}

func ensureReadable(tx sessionmodel.GraphTransaction) error {
	if tx.State != sessionmodel.TransactionStateActive {
		return ErrInvalidState
	}
	return nil
}

func ensureWritable(tx sessionmodel.GraphTransaction) error {
	if err := ensureReadable(tx); err != nil {
		return err
	}
	if tx.Mode != sessionmodel.TransactionModeReadWrite {
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
	if errors.Is(err, graphstorage.ErrIdempotencyConflict) {
		return ErrIdempotencyConflict
	}
	if errors.Is(err, graphstorage.ErrIdempotencyCapacity) {
		return ErrIdempotencyCapacity
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

func (m *Module) isDescendant(ctx context.Context, tx sessionmodel.GraphTransaction, candidate domaingraph.NodeID, ancestor domaingraph.NodeID) bool {
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

func (m *Module) ConfigureIndexes(ctx context.Context, tx sessionmodel.GraphTransaction, schemaHash string, indexes []schemamodel.IndexDefinition) error {
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
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return err
	}
	return mapStorageError(store.ConfigureIndexes(ctx, mustDomainID(tx.DomainID), schemaHash, indexes))
}

func (m *Module) ScanLabel(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.LabelScan) ([]domaingraph.Node, string, graphquery.IndexedReadStats, error) {
	label := strings.TrimSpace(scan.Label)
	stats := graphquery.IndexedReadStats{Plan: "LabelIndexScan", IndexName: "label:" + label, NextCursorKind: "label_key"}
	if err := ensureReadable(tx); err != nil {
		return nil, "", stats, err
	}
	if label == "" {
		return nil, "", stats, fmt.Errorf("%w: label is required", ErrInvalidInput)
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
		return nil, "", stats, err
	} else if forward {
		req := raftReadRequest("scan_label", tx)
		req.LabelScan = scan
		var res raftGraphIndexedNodesResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return nil, "", stats, err
		}
		return res.Nodes, res.NextPageToken, res.Stats, nil
	}
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return nil, "", stats, err
	}
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return nil, "", stats, err
	}
	if revision, ok := snapshotRevision(tx); ok {
		out, next, scanned, err := m.scanSnapshotLabel(ctx, store, tx, label, scan, revision)
		if err != nil {
			return nil, "", stats, err
		}
		stats.IndexEntriesScanned = scanned
		stats.NodesLoaded = len(out)
		return out, next, stats, nil
	}
	overlaySnapshot := m.overlaySnapshot(tx.ID)
	storageLimit := scan.Limit + overlayLimitExtra(scan.Limit, overlaySnapshot)
	nodeIDs, next, err := store.ScanLabel(ctx, graphstorage.LabelScan{DomainID: mustDomainID(tx.DomainID), Label: label, Limit: storageLimit, Cursor: scan.Cursor})
	if err != nil {
		return nil, "", stats, mapStorageError(err)
	}
	stats.IndexEntriesScanned = len(nodeIDs)
	out, next, err := indexedNodesWithOverlay(ctx, store, mustDomainID(tx.DomainID), nodeIDs, next, scan.Cursor, scan.Limit, overlaySnapshot, func(node domaingraph.Node) bool {
		return nodeHasAnyLabelForIndexedRead(node, []string{label})
	})
	if err != nil {
		return nil, "", stats, err
	}
	stats.NodesLoaded = len(out)
	return out, next, stats, nil
}

func (m *Module) ScanTag(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.TagScan) ([]domaingraph.Node, string, graphquery.IndexedReadStats, error) {
	tag, err := domaingraph.NormalizeTag(scan.Tag)
	stats := graphquery.IndexedReadStats{Plan: "TagIndexScan", IndexName: "tag:" + tag, NextCursorKind: "tag_key"}
	if err := ensureReadable(tx); err != nil {
		return nil, "", stats, err
	}
	if err != nil {
		return nil, "", stats, fmt.Errorf("%w: invalid tag", ErrInvalidInput)
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
		return nil, "", stats, err
	} else if forward {
		req := raftReadRequest("scan_tag", tx)
		req.TagScan = scan
		var res raftGraphIndexedNodesResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return nil, "", stats, err
		}
		return res.Nodes, res.NextPageToken, res.Stats, nil
	}
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return nil, "", stats, err
	}
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return nil, "", stats, err
	}
	if revision, ok := snapshotRevision(tx); ok {
		out, next, scanned, err := m.scanSnapshotTag(ctx, store, tx, tag, scan, revision)
		if err != nil {
			return nil, "", stats, err
		}
		stats.IndexEntriesScanned = scanned
		stats.NodesLoaded = len(out)
		return out, next, stats, nil
	}
	overlaySnapshot := m.overlaySnapshot(tx.ID)
	storageLimit := scan.Limit + overlayLimitExtra(scan.Limit, overlaySnapshot)
	nodeIDs, next, err := store.ScanTag(ctx, graphstorage.TagScan{DomainID: mustDomainID(tx.DomainID), Tag: tag, Limit: storageLimit, Cursor: scan.Cursor})
	if err != nil {
		return nil, "", stats, mapStorageError(err)
	}
	stats.IndexEntriesScanned = len(nodeIDs)
	out, next, err := indexedNodesWithOverlay(ctx, store, mustDomainID(tx.DomainID), nodeIDs, next, scan.Cursor, scan.Limit, overlaySnapshot, func(node domaingraph.Node) bool {
		return nodeHasTagForIndexedRead(node, tag)
	})
	if err != nil {
		return nil, "", stats, err
	}
	stats.NodesLoaded = len(out)
	return out, next, stats, nil
}

func (m *Module) overlaySnapshot(txID string) *overlay {
	m.mu.Lock()
	defer m.mu.Unlock()
	if o := m.overlays[txID]; o != nil {
		return o.clone()
	}
	return nil
}

func overlayLimitExtra(limit int, overlaySnapshot *overlay) int {
	if limit <= 0 || overlaySnapshot == nil {
		return 0
	}
	return len(overlaySnapshot.putNodes)
}

func indexedNodesWithOverlay(ctx context.Context, store *graphstorage.LocalStore, domainID domaingraph.DomainID, nodeIDs []domaingraph.NodeID, next string, cursor string, limit int, overlaySnapshot *overlay, include func(domaingraph.Node) bool) ([]domaingraph.Node, string, error) {
	items := make([]domaingraph.Node, 0, len(nodeIDs)+overlayLimitExtra(1, overlaySnapshot))
	for _, nodeID := range nodeIDs {
		if overlaySnapshot != nil {
			if _, deleted := overlaySnapshot.deleteNodes[nodeID]; deleted {
				continue
			}
			if _, replaced := overlaySnapshot.putNodes[nodeID]; replaced {
				continue
			}
		}
		node, err := store.GetNode(ctx, nodeID)
		if err != nil {
			return nil, "", mapStorageError(err)
		}
		items = append(items, cloneNode(node))
	}
	if overlaySnapshot != nil {
		cursorKey, err := graphstorage.DecodeIndexCursor(cursor)
		if err != nil {
			return nil, "", mapStorageError(err)
		}
		for _, node := range overlaySnapshot.putNodes {
			if node.DomainID != domainID || !include(node) || cursorKey != "" && node.ID.String() <= cursorKey {
				continue
			}
			items = append(items, cloneNode(node))
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID.String() < items[j].ID.String() })
	out := limitSnapshotNodes(items, limit)
	if cursor := snapshotNodeNext(items, limit); cursor != "" {
		next = cursor
	}
	return out, next, nil
}

func nodeHasTagForIndexedRead(node domaingraph.Node, tag string) bool {
	value := any(nil)
	if node.Properties != nil {
		value = node.Properties[domaingraph.NodePropTags]
	}
	if value == nil && node.Props != nil {
		value = node.Props[domaingraph.NodePropTags]
	}
	tags, err := domaingraph.NormalizeTagsValue(value)
	if err != nil {
		return false
	}
	for _, got := range tags {
		if got == tag {
			return true
		}
	}
	return false
}

func (m *Module) ScanNodePropertyOrdered(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.OrderedNodePropertyScan) ([]domaingraph.Node, string, graphquery.IndexedReadStats, error) {
	stats := graphquery.IndexedReadStats{Plan: "OrderedNodePropertyIndexScan", IndexName: scan.IndexName, NextCursorKind: "index_key"}
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
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return nil, "", stats, err
	}
	if revision, ok := snapshotRevision(tx); ok {
		out, next, scanned, err := m.scanSnapshotNodePropertyOrdered(ctx, store, tx, scan, revision)
		if err != nil {
			return nil, "", stats, err
		}
		stats.IndexEntriesScanned = scanned
		stats.NodesLoaded = len(out)
		return out, next, stats, nil
	}
	overlaySnapshot, storageLimit, err := m.boundedOrderedNodeOverlay(ctx, tx.ID, scan.Limit, scan.ExamineLimit)
	if err != nil {
		return nil, "", stats, err
	}
	var indexStatus graphstorage.IndexMetadata
	if overlaySnapshot != nil {
		indexStatus, err = readyNodeIndexMetadata(ctx, store, mustDomainID(tx.DomainID), scan.IndexName)
		if err != nil {
			return nil, "", stats, err
		}
	}
	entries, next, scanned, err := store.ScanNodePropertyOrderedBounded(ctx, graphstorage.OrderedNodePropertyScan{
		DomainID: mustDomainID(tx.DomainID), IndexName: scan.IndexName, Direction: scan.Direction,
		Limit: storageLimit, Cursor: scan.Cursor, ExamineLimit: storageLimit,
		HasLow: scan.HasLow, Low: scan.Low, LowExclusive: scan.LowExclusive,
		HasHigh: scan.HasHigh, High: scan.High, HighExclusive: scan.HighExclusive,
	})
	if err != nil {
		return nil, "", stats, mapStorageError(err)
	}
	stats.IndexEntriesScanned = scanned
	extra := 0
	if overlaySnapshot != nil {
		extra = len(overlaySnapshot.putNodes)
		stats.IndexEntriesScanned += len(overlaySnapshot.deleteNodes)
	}
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
		storageBoundary := ""
		if next != "" {
			storageBoundary, err = graphstorage.DecodeIndexCursor(next)
			if err != nil {
				return nil, "", stats, mapStorageError(err)
			}
		}
		for _, node := range overlaySnapshot.putNodes {
			if err := ctx.Err(); err != nil {
				return nil, "", stats, err
			}
			stats.IndexEntriesScanned++
			stats.NodesLoaded++
			if node.DomainID != mustDomainID(tx.DomainID) || !nodeHasAnyLabelForIndexedRead(node, indexStatus.Labels) {
				continue
			}
			value, ok := domaingraph.Property(node, indexStatus.Field.Name)
			if !ok {
				continue
			}
			key, err := graphstorage.EncodeOrderedNodeKey(value, node.ID)
			if err != nil || !indexedValueInBounds(key, scan) {
				continue
			}
			if scan.Direction == schemamodel.IndexSortDirectionDesc {
				if cursorKey != "" && key >= cursorKey || storageBoundary != "" && key < storageBoundary {
					continue
				}
			} else if cursorKey != "" && key <= cursorKey || storageBoundary != "" && key > storageBoundary {
				continue
			}
			items = append(items, indexedNodeResult{node: cloneNode(node), key: key})
		}
	}
	out, next := finishIndexedNodeResults(items, scan.Limit, scan.Direction, next)
	return out, next, stats, nil
}

func (m *Module) ScanAdjacency(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.AdjacencyScan) ([]domaingraph.Edge, string, graphquery.IndexedReadStats, error) {
	stats := graphquery.IndexedReadStats{Plan: "EdgeAdjacencyIndexScan", IndexName: string(scan.Direction) + ":" + scan.Label, NextCursorKind: "adjacency_key"}
	if err := ensureReadable(tx); err != nil {
		return nil, "", stats, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
		return nil, "", stats, err
	} else if forward {
		req := raftReadRequest("scan_adjacency", tx)
		req.AdjacencyScan = scan
		var res raftGraphIndexedEdgesResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return nil, "", stats, err
		}
		return res.Edges, res.NextPageToken, res.Stats, nil
	}
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return nil, "", stats, err
	}
	id, err := parseUUID[domaingraph.NodeID](scan.NodeID, "node_id")
	if err != nil {
		return nil, "", stats, err
	}
	label := strings.TrimSpace(scan.Label)
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return nil, "", stats, err
	}
	storageDirection := graphstorage.AdjacencyDirectionOut
	if scan.Direction == graphquery.AdjacencyDirectionIn {
		storageDirection = graphstorage.AdjacencyDirectionIn
	}
	limit, examineLimit := boundedOverlayLimits(scan.Limit, scan.ExamineLimit)
	storageScan := graphstorage.AdjacencyScan{DomainID: mustDomainID(tx.DomainID), NodeID: id, Label: label, Direction: storageDirection, Limit: limit, Cursor: scan.Cursor}
	snapshotRev, snapshoted := snapshotRevision(tx)
	if snapshoted {
		edgeIDs, next, examined, scanErr := store.ScanAdjacencyAt(ctx, storageScan, snapshotRev, examineLimit)
		stats.IndexEntriesScanned = examined
		if scanErr != nil {
			return nil, "", stats, mapStorageError(scanErr)
		}
		out := make([]domaingraph.Edge, 0, len(edgeIDs))
		for _, edgeID := range edgeIDs {
			edge, readErr := store.GetEdgeAt(ctx, edgeID, snapshotRev)
			if readErr != nil {
				return nil, "", stats, mapStorageError(readErr)
			}
			out = append(out, cloneEdge(edge))
		}
		stats.EdgesLoaded = len(out)
		return out, next, stats, nil
	}
	overlaySnapshot, storageLimit, err := m.boundedAdjacencyOverlay(ctx, tx.ID, limit, examineLimit)
	if err != nil {
		return nil, "", stats, err
	}
	if overlaySnapshot != nil {
		out, next, examined, loaded, scanErr := scanAdjacencyWithOverlay(ctx, store, mustDomainID(tx.DomainID), id, label, storageDirection, limit, storageLimit, scan.Cursor, overlaySnapshot)
		stats.IndexEntriesScanned = examined
		stats.EdgesLoaded = loaded
		if scanErr != nil {
			return nil, "", stats, scanErr
		}
		return out, next, stats, nil
	}
	edgeIDs, next, err := store.ScanAdjacency(ctx, storageScan)
	if err != nil {
		return nil, "", stats, mapStorageError(err)
	}
	stats.IndexEntriesScanned = len(edgeIDs)
	out := make([]domaingraph.Edge, 0, len(edgeIDs))
	for _, edgeID := range edgeIDs {
		edge, readErr := store.GetEdge(ctx, edgeID)
		if readErr != nil {
			return nil, "", stats, mapStorageError(readErr)
		}
		out = append(out, cloneEdge(edge))
	}
	stats.EdgesLoaded = len(out)
	return out, next, stats, nil
}

func scanAdjacencyWithOverlay(ctx context.Context, store *graphstorage.LocalStore, domainID domaingraph.DomainID, nodeID domaingraph.NodeID, label string, direction graphstorage.AdjacencyDirection, limit int, storageLimit int, cursor string, overlaySnapshot *overlay) ([]domaingraph.Edge, string, int, int, error) {
	edgeIDs, storageNext, err := store.ScanAdjacency(ctx, graphstorage.AdjacencyScan{
		DomainID: domainID, NodeID: nodeID, Label: label, Direction: direction, Limit: storageLimit, Cursor: cursor,
	})
	if err != nil {
		return nil, "", 0, 0, mapStorageError(err)
	}
	examined := len(edgeIDs) + len(overlaySnapshot.deleteEdges)
	loaded := 0
	items := make([]domaingraph.Edge, 0, len(edgeIDs)+len(overlaySnapshot.putEdges))
	for _, edgeID := range edgeIDs {
		if _, deleted := overlaySnapshot.deleteEdges[edgeID]; deleted {
			continue
		}
		if _, replaced := overlaySnapshot.putEdges[edgeID]; replaced {
			continue
		}
		edge, readErr := store.GetEdge(ctx, edgeID)
		if readErr != nil {
			return nil, "", examined, loaded, mapStorageError(readErr)
		}
		loaded++
		items = append(items, cloneEdge(edge))
	}
	cursorKey, err := graphstorage.DecodeIndexCursor(cursor)
	if err != nil {
		return nil, "", examined, loaded, mapStorageError(err)
	}
	storageBoundary := ""
	if storageNext != "" {
		storageBoundary, err = graphstorage.DecodeIndexCursor(storageNext)
		if err != nil {
			return nil, "", examined, loaded, mapStorageError(err)
		}
	}
	for _, edge := range overlaySnapshot.putEdges {
		if err := ctx.Err(); err != nil {
			return nil, "", examined, loaded, err
		}
		examined++
		if edge.DomainID != domainID || label != "" && !domaingraph.EdgeHasLabels(edge, []string{label}) {
			continue
		}
		if direction == graphstorage.AdjacencyDirectionIn && edge.ToID != nodeID || direction == graphstorage.AdjacencyDirectionOut && edge.FromID != nodeID {
			continue
		}
		if label == "" && direction == graphstorage.AdjacencyDirectionIn && edge.FromID == edge.ToID {
			continue
		}
		key := adjacencyServiceKey(edge)
		if cursorKey != "" && key <= cursorKey || storageBoundary != "" && key > storageBoundary {
			continue
		}
		loaded++
		items = append(items, cloneEdge(edge))
	}
	sort.Slice(items, func(i, j int) bool { return adjacencyServiceKey(items[i]) < adjacencyServiceKey(items[j]) })
	if len(items) > limit {
		items = items[:limit]
		return items, graphstorage.EncodeIndexCursor(adjacencyServiceKey(items[len(items)-1])), examined, loaded, nil
	}
	return items, storageNext, examined, loaded, nil
}

func (m *Module) ScanSubtree(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.SubtreeScan) (graphquery.SubtreeResult, graphquery.IndexedReadStats, error) {
	stats := graphquery.IndexedReadStats{Plan: "EdgeAdjacencyIndexScan", IndexName: string(scan.Direction) + ":" + scan.Label, NextCursorKind: "root_index_key"}
	if err := ensureReadable(tx); err != nil {
		return graphquery.SubtreeResult{}, stats, err
	}
	if leader, forward, err := m.shouldForwardRaftGraphTransactionRead(tx); err != nil {
		return graphquery.SubtreeResult{}, stats, err
	} else if forward {
		req := raftReadRequest("scan_subtree", tx)
		req.SubtreeScan = scan
		var res raftGraphSubtreeResponse
		if err := m.forwardRaftGraphRead(ctx, leader, req, &res); err != nil {
			return graphquery.SubtreeResult{}, stats, err
		}
		return res.Result, res.Stats, nil
	}
	if _, err := m.strongGraphReadForTransaction(ctx, tx); err != nil {
		return graphquery.SubtreeResult{}, stats, err
	}
	label := strings.TrimSpace(scan.Label)
	if label == "" {
		return graphquery.SubtreeResult{}, stats, fmt.Errorf("%w: subtree label is required", ErrInvalidInput)
	}
	if scan.Direction != graphquery.AdjacencyDirectionOut && scan.Direction != graphquery.AdjacencyDirectionIn {
		return graphquery.SubtreeResult{}, stats, fmt.Errorf("%w: subtree direction is required", ErrInvalidInput)
	}
	if scan.MinDepth < 0 || (scan.MaxDepth != -1 && scan.MaxDepth < scan.MinDepth) {
		return graphquery.SubtreeResult{}, stats, fmt.Errorf("%w: invalid subtree depth", ErrInvalidInput)
	}
	store, err := m.store(ctx, tx.SpaceID, tx.DomainID)
	if err != nil {
		return graphquery.SubtreeResult{}, stats, err
	}
	m.mu.Lock()
	o := m.overlays[tx.ID]
	var overlaySnapshot *overlay
	if o != nil {
		overlaySnapshot = o.clone()
	}
	m.mu.Unlock()
	storageDirection := graphstorage.AdjacencyDirectionOut
	if scan.Direction == graphquery.AdjacencyDirectionIn {
		storageDirection = graphstorage.AdjacencyDirectionIn
	}
	snapshotRev, snapshoted := snapshotRevision(tx)
	maxNodes := scan.MaxNodes
	if maxNodes <= 0 {
		maxNodes = int(^uint(0) >> 1)
	}
	maxEdges := scan.MaxEdges
	if maxEdges <= 0 {
		maxEdges = int(^uint(0) >> 1)
	}
	result := graphquery.SubtreeResult{Roots: make([]graphquery.SubtreeRoot, 0, len(scan.Roots))}
	seenGraphNodes := map[string]bool{}
	seenGraphEdges := map[string]bool{}
	addGraphNode := func(node domaingraph.Node) bool {
		id := node.ID.String()
		if seenGraphNodes[id] {
			return true
		}
		if len(seenGraphNodes) >= maxNodes {
			result.Truncated = true
			if result.TruncationReason == "" {
				result.TruncationReason = "max_nodes exceeded"
			}
			return false
		}
		seenGraphNodes[id] = true
		result.GraphNodes = append(result.GraphNodes, cloneNode(node))
		return true
	}
	canAddGraphEdge := func(edge domaingraph.Edge) bool {
		id := edge.ID.String()
		if seenGraphEdges[id] {
			return true
		}
		if len(seenGraphEdges) >= maxEdges {
			result.Truncated = true
			if result.TruncationReason == "" {
				result.TruncationReason = "max_edges exceeded"
			}
			return false
		}
		return true
	}
	addGraphEdge := func(edge domaingraph.Edge) bool {
		if !canAddGraphEdge(edge) {
			return false
		}
		id := edge.ID.String()
		if seenGraphEdges[id] {
			return true
		}
		seenGraphEdges[id] = true
		result.GraphEdges = append(result.GraphEdges, cloneEdge(edge))
		return true
	}
	loadNode := func(id domaingraph.NodeID) (domaingraph.Node, error) {
		if overlaySnapshot != nil {
			if _, deleted := overlaySnapshot.deleteNodes[id]; deleted {
				return domaingraph.Node{}, ErrNotFound
			}
			if node, ok := overlaySnapshot.putNodes[id]; ok {
				stats.NodeReadCalls++
				return cloneNode(node), nil
			}
		}
		var node domaingraph.Node
		var err error
		if snapshoted {
			node, err = store.GetNodeAt(ctx, id, snapshotRev)
		} else {
			node, err = store.GetNode(ctx, id)
		}
		stats.NodeReadCalls++
		if err != nil {
			return domaingraph.Node{}, mapStorageError(err)
		}
		if node.DomainID != mustDomainID(tx.DomainID) {
			return domaingraph.Node{}, ErrNotFound
		}
		return cloneNode(node), nil
	}
	loadAdjacency := func(nodeID domaingraph.NodeID) ([]domaingraph.Edge, error) {
		stats.AdjacencyScanCalls++
		if overlaySnapshot != nil || snapshoted {
			view := transactionEdgeView{store: store, overlay: overlaySnapshot, revision: snapshotRev, snapshoted: snapshoted}
			var endpointEdges []domaingraph.Edge
			var err error
			if scan.Direction == graphquery.AdjacencyDirectionIn {
				endpointEdges, err = view.incoming(ctx, nodeID)
			} else {
				endpointEdges, err = view.outgoing(ctx, nodeID)
			}
			if err != nil {
				return nil, err
			}
			items := make([]domaingraph.Edge, 0, len(endpointEdges))
			for _, edge := range endpointEdges {
				if edge.DomainID == mustDomainID(tx.DomainID) && domaingraph.EdgeHasLabels(edge, []string{label}) {
					items = append(items, cloneEdge(edge))
				}
			}
			sort.Slice(items, func(i, j int) bool { return adjacencyServiceKey(items[i]) < adjacencyServiceKey(items[j]) })
			stats.IndexEntriesScanned += len(items)
			stats.EdgesLoaded += len(items)
			return items, nil
		}
		out := []domaingraph.Edge{}
		cursor := ""
		for {
			edgeIDs, next, err := store.ScanAdjacency(ctx, graphstorage.AdjacencyScan{DomainID: mustDomainID(tx.DomainID), NodeID: nodeID, Label: label, Direction: storageDirection, Limit: 0, Cursor: cursor})
			if err != nil {
				return nil, mapStorageError(err)
			}
			stats.IndexEntriesScanned += len(edgeIDs)
			for _, edgeID := range edgeIDs {
				edge, err := store.GetEdge(ctx, edgeID)
				if err != nil {
					return nil, mapStorageError(err)
				}
				out = append(out, cloneEdge(edge))
			}
			if next == "" {
				break
			}
			cursor = next
		}
		stats.EdgesLoaded += len(out)
		return out, nil
	}
	for _, root := range scan.Roots {
		if result.Truncated {
			break
		}
		root = cloneNode(root)
		if !addGraphNode(root) {
			break
		}
		row := graphquery.SubtreeRoot{Root: root, ParentByChild: map[string]string{}, OrderByChild: map[string]any{}}
		if scan.MinDepth == 0 && subtreeNodeMatchesLabels(root, scan.TargetLabels) {
			row.Nodes = append(row.Nodes, root)
		}
		visited := map[string]bool{root.ID.String(): true}
		frontier := []subtreeServiceFrontier{{node: root, depth: 0}}
		for len(frontier) > 0 && !result.Truncated {
			current := frontier[0]
			frontier = frontier[1:]
			if scan.MaxDepth != -1 && current.depth >= scan.MaxDepth {
				continue
			}
			edges, err := loadAdjacency(current.node.ID)
			if err != nil {
				return graphquery.SubtreeResult{}, stats, err
			}
			for _, edge := range edges {
				if !canAddGraphEdge(edge) {
					break
				}
				endpointID := edge.ToID
				if scan.Direction == graphquery.AdjacencyDirectionIn {
					endpointID = edge.FromID
				}
				if visited[endpointID.String()] {
					if !addGraphEdge(edge) {
						break
					}
					continue
				}
				endpoint, err := loadNode(endpointID)
				if err != nil {
					return graphquery.SubtreeResult{}, stats, err
				}
				stats.NodesLoaded++
				if !addGraphNode(endpoint) {
					break
				}
				if !addGraphEdge(edge) {
					break
				}
				visited[endpointID.String()] = true
				childDepth := current.depth + 1
				if childDepth >= scan.MinDepth && subtreeNodeMatchesLabels(endpoint, scan.TargetLabels) {
					row.Nodes = append(row.Nodes, endpoint)
					if scan.Direction == graphquery.AdjacencyDirectionOut && label == "contains" {
						row.ParentByChild[endpoint.ID.String()] = current.node.ID.String()
						row.OrderByChild[endpoint.ID.String()] = edge.Properties["order"]
					}
				}
				if scan.MaxDepth == -1 || childDepth < scan.MaxDepth {
					frontier = append(frontier, subtreeServiceFrontier{node: endpoint, depth: childDepth})
				}
			}
		}
		result.Roots = append(result.Roots, row)
	}
	return result, stats, nil
}

type subtreeServiceFrontier struct {
	node  domaingraph.Node
	depth int
}

func subtreeNodeMatchesLabels(node domaingraph.Node, labels []string) bool {
	for _, want := range labels {
		found := false
		for _, got := range node.Labels {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func adjacencyServiceKey(edge domaingraph.Edge) string {
	order := int64(0)
	if value, ok := edge.Properties["order"]; ok {
		switch v := value.(type) {
		case int:
			order = int64(v)
		case int32:
			order = int64(v)
		case int64:
			order = v
		case float32:
			order = int64(v)
		case float64:
			order = int64(v)
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				order = parsed
			}
		}
	}
	flipped := uint64(order) ^ (uint64(1) << 63)
	return fmt.Sprintf("u:%020d\x00%s", flipped, edge.ID.String())
}

type indexedNodeResult struct {
	node domaingraph.Node
	key  string
}

func finishIndexedNodeResults(items []indexedNodeResult, limit int, direction schemamodel.IndexSortDirection, fallbackNext string) ([]domaingraph.Node, string) {
	sort.Slice(items, func(i, j int) bool {
		if direction == schemamodel.IndexSortDirectionDesc {
			return items[i].key > items[j].key
		}
		return items[i].key < items[j].key
	})
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	out := make([]domaingraph.Node, 0, limit)
	for _, item := range items[:limit] {
		out = append(out, cloneNode(item.node))
	}
	if limit < len(items) && limit > 0 {
		return out, graphstorage.EncodeIndexCursor(items[limit-1].key)
	}
	return out, fallbackNext
}

func (m *Module) scanSnapshotLabel(ctx context.Context, store *graphstorage.LocalStore, tx sessionmodel.GraphTransaction, label string, scan graphquery.LabelScan, revision uint64) ([]domaingraph.Node, string, int, error) {
	return scanSnapshotNodes(ctx, store, mustDomainID(tx.DomainID), scan.Cursor, scan.Limit, revision, func(node domaingraph.Node) bool {
		return nodeHasAnyLabelForIndexedRead(node, []string{label})
	})
}

func (m *Module) scanSnapshotTag(ctx context.Context, store *graphstorage.LocalStore, tx sessionmodel.GraphTransaction, tag string, scan graphquery.TagScan, revision uint64) ([]domaingraph.Node, string, int, error) {
	return scanSnapshotNodes(ctx, store, mustDomainID(tx.DomainID), scan.Cursor, scan.Limit, revision, func(node domaingraph.Node) bool {
		return nodeHasTagForIndexedRead(node, tag)
	})
}

func scanSnapshotNodes(ctx context.Context, store *graphstorage.LocalStore, domainID domaingraph.DomainID, cursor string, limit int, revision uint64, include func(domaingraph.Node) bool) ([]domaingraph.Node, string, int, error) {
	if limit <= 0 || limit > 500 {
		limit = 500
	}
	cursorKey, err := graphstorage.DecodeIndexCursor(cursor)
	if err != nil {
		return nil, "", 0, mapStorageError(err)
	}
	nodes, next, examined, err := store.ScanNodesByDomain(ctx, graphstorage.DomainNodeScan{
		DomainID: domainID, Limit: limit, Cursor: cursorKey,
		Snapshot: true, Revision: revision, ExamineLimit: limit,
	})
	if err != nil {
		return nil, "", examined, mapStorageError(err)
	}
	out := make([]domaingraph.Node, 0, len(nodes))
	for _, node := range nodes {
		if include(node) {
			out = append(out, cloneNode(node))
		}
	}
	return out, graphstorage.EncodeIndexCursor(next), examined, nil
}

func (m *Module) scanSnapshotNodePropertyOrdered(ctx context.Context, store *graphstorage.LocalStore, tx sessionmodel.GraphTransaction, scan graphquery.OrderedNodePropertyScan, revision uint64) ([]domaingraph.Node, string, int, error) {
	entries, next, examined, err := store.ScanNodePropertyOrderedAt(ctx, graphstorage.OrderedNodePropertyScan{
		DomainID: mustDomainID(tx.DomainID), IndexName: scan.IndexName, Direction: scan.Direction,
		Limit: scan.Limit, Cursor: scan.Cursor, ExamineLimit: scan.ExamineLimit,
		HasLow: scan.HasLow, Low: scan.Low, LowExclusive: scan.LowExclusive,
		HasHigh: scan.HasHigh, High: scan.High, HighExclusive: scan.HighExclusive,
	}, revision)
	if err != nil {
		return nil, "", examined, mapStorageError(err)
	}
	out := make([]domaingraph.Node, 0, len(entries))
	for _, entry := range entries {
		node, readErr := store.GetNodeAt(ctx, entry.NodeID, revision)
		if readErr != nil {
			return nil, "", examined, mapStorageError(readErr)
		}
		out = append(out, cloneNode(node))
	}
	return out, next, examined, nil
}

func limitSnapshotNodes(items []domaingraph.Node, limit int) []domaingraph.Node {
	if limit <= 0 || limit > len(items) {
		limit = len(items)
	}
	return append([]domaingraph.Node(nil), items[:limit]...)
}

func snapshotNodeNext(items []domaingraph.Node, limit int) string {
	if limit <= 0 || limit >= len(items) {
		return ""
	}
	return graphstorage.EncodeIndexCursor(items[limit-1].ID.String())
}

func indexedValueInBounds(key string, scan graphquery.OrderedNodePropertyScan) bool {
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

func readyNodeIndexMetadata(ctx context.Context, store *graphstorage.LocalStore, domainID domaingraph.DomainID, indexName string) (graphstorage.IndexMetadata, error) {
	statuses, err := store.IndexStatuses(ctx)
	if err != nil {
		return graphstorage.IndexMetadata{}, mapStorageError(err)
	}
	for _, status := range statuses {
		if status.Name == indexName && status.DomainID == domainID && status.TargetKind == schemamodel.IndexTargetNode && status.BuildState == graphstorage.IndexBuildStateReady {
			return status, nil
		}
	}
	return graphstorage.IndexMetadata{}, mapStorageError(graphstorage.ErrIndexUnavailable)
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
