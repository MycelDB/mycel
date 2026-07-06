package filesession

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/blob/storage"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/graph/query"
	storetemplate "github.com/myceldb/mycel/internal/graph/template/storage"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
)

type txOverlay struct {
	addedNodes   map[graph.NodeID]graph.Node
	updatedNodes map[graph.NodeID]graph.Node
	deletedNodes map[graph.NodeID]struct{}

	addedEdges   map[graph.EdgeID]graph.Edge
	updatedEdges map[graph.EdgeID]graph.Edge
	deletedEdges map[graph.EdgeID]struct{}
}

type txStagedBlob struct {
	staged   blobstorage.StagedBlob
	blobID   graph.BlobID
	nodeID   graph.NodeID
	existing bool
}

type fileTx struct {
	session      *FileSession
	options      sessionapi.TxOptions
	overlay      txOverlay
	stagedBlobs  []txStagedBlob
	baseRevision uint64
	closed       bool
}

// Begin starts a session transaction backed by an in-memory overlay.
func (s *FileSession) Begin(ctx context.Context, opts sessionapi.TxOptions) (sessionapi.Tx, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if opts.ReadOnly {
		if err := s.ensureRead(); err != nil {
			return nil, err
		}
	} else if err := s.ensureWrite(); err != nil {
		return nil, err
	}
	store, err := s.graphStore()
	if err != nil {
		return nil, err
	}
	return &fileTx{session: s, options: opts, overlay: newTxOverlay(), baseRevision: store.Revision()}, nil
}

// Tx runs fn inside a session transaction. Callback errors rollback the staged
// overlay and are returned unchanged; nil callback errors commit the overlay.
func (s *FileSession) Tx(ctx context.Context, opts sessionapi.TxOptions, fn func(sessionapi.Tx) error) error {
	tx, err := s.Begin(ctx, opts)
	if err != nil {
		return err
	}
	if fn == nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("transaction callback is required")
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func newTxOverlay() txOverlay {
	return txOverlay{
		addedNodes:   map[graph.NodeID]graph.Node{},
		updatedNodes: map[graph.NodeID]graph.Node{},
		deletedNodes: map[graph.NodeID]struct{}{},
		addedEdges:   map[graph.EdgeID]graph.Edge{},
		updatedEdges: map[graph.EdgeID]graph.Edge{},
		deletedEdges: map[graph.EdgeID]struct{}{},
	}
}

func (o txOverlay) empty() bool {
	return len(o.addedNodes) == 0 && len(o.updatedNodes) == 0 && len(o.deletedNodes) == 0 && len(o.addedEdges) == 0 && len(o.updatedEdges) == 0 && len(o.deletedEdges) == 0
}

func (o txOverlay) clone() txOverlay {
	out := newTxOverlay()
	for id, node := range o.addedNodes {
		out.addedNodes[id] = cloneNode(node)
	}
	for id, node := range o.updatedNodes {
		out.updatedNodes[id] = cloneNode(node)
	}
	for id := range o.deletedNodes {
		out.deletedNodes[id] = struct{}{}
	}
	for id, edge := range o.addedEdges {
		out.addedEdges[id] = cloneEdge(edge)
	}
	for id, edge := range o.updatedEdges {
		out.updatedEdges[id] = cloneEdge(edge)
	}
	for id := range o.deletedEdges {
		out.deletedEdges[id] = struct{}{}
	}
	return out
}

func (o txOverlay) delta() ([]graph.Node, []graph.Edge, []graph.NodeID, []graph.EdgeID) {
	putNodes := make([]graph.Node, 0, len(o.addedNodes)+len(o.updatedNodes))
	for _, node := range o.addedNodes {
		putNodes = append(putNodes, cloneNode(node))
	}
	for id, node := range o.updatedNodes {
		if _, added := o.addedNodes[id]; added {
			continue
		}
		putNodes = append(putNodes, cloneNode(node))
	}
	sort.Slice(putNodes, func(i, j int) bool { return putNodes[i].ID.String() < putNodes[j].ID.String() })

	putEdges := make([]graph.Edge, 0, len(o.addedEdges)+len(o.updatedEdges))
	for _, edge := range o.addedEdges {
		putEdges = append(putEdges, cloneEdge(edge))
	}
	for id, edge := range o.updatedEdges {
		if _, added := o.addedEdges[id]; added {
			continue
		}
		putEdges = append(putEdges, cloneEdge(edge))
	}
	sort.Slice(putEdges, func(i, j int) bool { return putEdges[i].ID.String() < putEdges[j].ID.String() })

	deleteNodes := make([]graph.NodeID, 0, len(o.deletedNodes))
	for id := range o.deletedNodes {
		deleteNodes = append(deleteNodes, id)
	}
	sort.Slice(deleteNodes, func(i, j int) bool { return deleteNodes[i].String() < deleteNodes[j].String() })

	deleteEdges := make([]graph.EdgeID, 0, len(o.deletedEdges))
	for id := range o.deletedEdges {
		deleteEdges = append(deleteEdges, id)
	}
	sort.Slice(deleteEdges, func(i, j int) bool { return deleteEdges[i].String() < deleteEdges[j].String() })
	return putNodes, putEdges, deleteNodes, deleteEdges
}

func (tx *fileTx) Query() *query.Builder { return query.NewBuilder(tx) }

func (tx *fileTx) ListTemplates(ctx context.Context) ([]graph.Template, error) {
	if err := tx.ensureOpen(ctx); err != nil {
		return nil, err
	}
	return tx.session.ListTemplates(ctx)
}

func (tx *fileTx) AddNode(ctx context.Context, in sessionapi.AddNodeInput) (graph.Node, error) {
	if err := tx.ensureWritable(ctx); err != nil {
		return graph.Node{}, err
	}
	nodes, err := tx.ListNodes(ctx)
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
	if findNodeIndex(nodes, nodeID) >= 0 {
		return graph.Node{}, fmt.Errorf("%w: node already exists", storetemplate.ErrInvalidInput)
	}
	n, err := tx.session.buildNode(ctx, nodes, nodeID, in.TemplateID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	now := time.Now().UTC()
	n.CreatedAt = now
	n.UpdatedAt = now
	delete(tx.overlay.deletedNodes, n.ID)
	tx.overlay.addedNodes[n.ID] = n
	return cloneNode(n), nil
}

func (tx *fileTx) AddBlobNode(ctx context.Context, in sessionapi.AddBlobNodeInput) (graph.Node, error) {
	if err := tx.ensureWritable(ctx); err != nil {
		return graph.Node{}, err
	}
	nodes, err := tx.ListNodes(ctx)
	if err != nil {
		return graph.Node{}, err
	}
	node, staged, err := tx.session.stageBlobNode(ctx, in, nodes)
	if err != nil {
		return graph.Node{}, err
	}
	if findNodeIndex(nodes, node.ID) >= 0 {
		blobs, blobErr := tx.session.blobStore()
		if blobErr == nil {
			_ = blobs.Discard(ctx, staged)
		}
		return graph.Node{}, fmt.Errorf("%w: node already exists", storetemplate.ErrInvalidInput)
	}
	tx.overlay.addedNodes[node.ID] = node
	tx.stagedBlobs = append(tx.stagedBlobs, txStagedBlob{staged: staged, blobID: staged.ID, nodeID: node.ID, existing: staged.Existing()})
	return cloneNode(node), nil
}

func (tx *fileTx) ListNodes(ctx context.Context) ([]graph.Node, error) {
	if err := tx.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := tx.session.ensureRead(); err != nil {
		return nil, err
	}
	base, err := tx.session.readNodes()
	if err != nil {
		return nil, err
	}
	return tx.mergedNodes(base), nil
}

func (tx *fileTx) UpdateNode(ctx context.Context, in sessionapi.UpdateNodeInput) (graph.Node, error) {
	if err := tx.ensureWritable(ctx); err != nil {
		return graph.Node{}, err
	}
	if in.ID == uuid.Nil {
		return graph.Node{}, fmt.Errorf("%w: node_id is required", tx.session.errors.NotFound)
	}
	nodes, err := tx.ListNodes(ctx)
	if err != nil {
		return graph.Node{}, err
	}
	idx := findNodeIndex(nodes, in.ID)
	if idx < 0 {
		return graph.Node{}, tx.session.errors.NotFound
	}
	if nodes[idx].BlobRef != nil && in.Content != "" {
		return graph.Node{}, fmt.Errorf("%w: blob nodes cannot have inline content; use props (e.g. caption) or annotation children", storetemplate.ErrInvalidInput)
	}
	n, err := tx.session.buildNode(ctx, nodes, in.ID, in.TemplateID, in.Content, in.Props)
	if err != nil {
		return graph.Node{}, err
	}
	n.BlobRef = nodes[idx].BlobRef
	n.DomainID = nodes[idx].DomainID
	n.CreatedAt = nodes[idx].CreatedAt
	if n.CreatedAt.IsZero() {
		n.CreatedAt = time.Now().UTC()
	}
	n.UpdatedAt = time.Now().UTC()
	candidateNodes := cloneNodes(nodes)
	candidateNodes[idx] = n
	edges, err := tx.ListEdges(ctx)
	if err != nil {
		return graph.Node{}, err
	}
	if err := tx.session.validateIncidentContains(ctx, n, candidateNodes, edges); err != nil {
		return graph.Node{}, err
	}
	if _, ok := tx.overlay.addedNodes[n.ID]; ok {
		tx.overlay.addedNodes[n.ID] = n
	} else {
		tx.overlay.updatedNodes[n.ID] = n
	}
	return cloneNode(n), nil
}

func (tx *fileTx) UpdateNodeAndCreateSibling(ctx context.Context, in sessionapi.UpdateNodeAndCreateSiblingInput) (sessionapi.UpdateNodeAndCreateSiblingResult, error) {
	if err := tx.ensureWritable(ctx); err != nil {
		return sessionapi.UpdateNodeAndCreateSiblingResult{}, err
	}
	snapshot := tx.overlay.clone()
	fail := func(err error) (sessionapi.UpdateNodeAndCreateSiblingResult, error) {
		tx.overlay = snapshot
		return sessionapi.UpdateNodeAndCreateSiblingResult{}, err
	}
	if in.NodeID == uuid.Nil {
		return fail(fmt.Errorf("%w: node_id is required", tx.session.errors.NotFound))
	}
	nodes, err := tx.ListNodes(ctx)
	if err != nil {
		return fail(err)
	}
	idx := findNodeIndex(nodes, in.NodeID)
	if idx < 0 {
		return fail(tx.session.errors.NotFound)
	}
	edges, err := tx.ListEdges(ctx)
	if err != nil {
		return fail(err)
	}
	parentIndexes := containsParentEdgeIndexes(edges, in.NodeID)
	if len(parentIndexes) == 0 {
		return fail(fmt.Errorf("%w: cannot insert a sibling for a root node", storetemplate.ErrInvalidInput))
	}
	if len(parentIndexes) > 1 {
		return fail(fmt.Errorf("%w: node has multiple contains parents", storetemplate.ErrInvalidInput))
	}
	parentID := edges[parentIndexes[0]].FromID
	childEdgeIndexes := orderedContainsEdgeIndexes(edges, parentID)
	currentOrder := -1
	previousOrder := 0.0
	for i, edgeIndex := range childEdgeIndexes {
		if edges[edgeIndex].ToID == in.NodeID {
			currentOrder = i
			if value, ok := edgeOrderNumber(edges[edgeIndex]); ok {
				previousOrder = value
			} else {
				previousOrder = float64(i * childOrderStep)
			}
			break
		}
	}
	if currentOrder < 0 {
		return fail(fmt.Errorf("%w: node is not contained by parent", storetemplate.ErrInvalidInput))
	}
	insertOrder := currentOrder + 1
	createdOrder := previousOrder + childOrderStep
	if insertOrder < len(childEdgeIndexes) {
		nextOrder, ok := edgeOrderNumber(edges[childEdgeIndexes[insertOrder]])
		if !ok {
			nextOrder = float64(insertOrder * childOrderStep)
		}
		if nextOrder > previousOrder {
			createdOrder = previousOrder + (nextOrder-previousOrder)/2
		}
	}
	updated, err := tx.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: in.NodeID, TemplateID: nodes[idx].TemplateID, Content: in.Content, Props: in.Props})
	if err != nil {
		return fail(err)
	}
	sibling, err := tx.AddNode(ctx, sessionapi.AddNodeInput{ID: in.SiblingID, TemplateID: in.SiblingTemplateID, Content: in.SiblingContent, Props: in.SiblingProps})
	if err != nil {
		return fail(err)
	}
	createdEdge, err := tx.AddEdge(ctx, sessionapi.AddEdgeInput{FromID: parentID, ToID: sibling.ID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": createdOrder}})
	if err != nil {
		return fail(err)
	}
	return sessionapi.UpdateNodeAndCreateSiblingResult{UpdatedNode: updated, CreatedNode: sibling, CreatedEdge: createdEdge, SiblingOrder: insertOrder}, nil
}

func (tx *fileTx) UpsertNode(ctx context.Context, in sessionapi.UpsertNodeInput) (graph.Node, error) {
	if in.ID == nil {
		return tx.AddNode(ctx, sessionapi.AddNodeInput{TemplateID: in.TemplateID, Content: in.Content, Props: in.Props})
	}
	if _, err := tx.GetNode(ctx, *in.ID); err == nil {
		return tx.UpdateNode(ctx, sessionapi.UpdateNodeInput{ID: *in.ID, TemplateID: in.TemplateID, Content: in.Content, Props: in.Props})
	}
	return tx.AddNode(ctx, sessionapi.AddNodeInput{ID: in.ID, TemplateID: in.TemplateID, Content: in.Content, Props: in.Props})
}

func (tx *fileTx) AddEdge(ctx context.Context, in sessionapi.AddEdgeInput) (graph.Edge, error) {
	if err := tx.ensureWritable(ctx); err != nil {
		return graph.Edge{}, err
	}
	nodes, err := tx.ListNodes(ctx)
	if err != nil {
		return graph.Edge{}, err
	}
	from, ok := findNode(nodes, in.FromID)
	if !ok {
		return graph.Edge{}, fmt.Errorf("%w: from node not found", tx.session.errors.NotFound)
	}
	to, ok := findNode(nodes, in.ToID)
	if !ok {
		return graph.Edge{}, fmt.Errorf("%w: to node not found", tx.session.errors.NotFound)
	}
	edges, err := tx.ListEdges(ctx)
	if err != nil {
		return graph.Edge{}, err
	}
	if err := tx.session.validateNewEdge(ctx, from, to, in.Kind, edges); err != nil {
		return graph.Edge{}, err
	}
	edgeID, err := newGraphUUID()
	if err != nil {
		return graph.Edge{}, err
	}
	if in.ID != nil {
		edgeID = *in.ID
	}
	if findEdgeIndex(edges, edgeID) >= 0 {
		return graph.Edge{}, fmt.Errorf("%w: edge already exists", storetemplate.ErrInvalidInput)
	}
	e := graph.Edge{ID: edgeID, FromID: in.FromID, ToID: in.ToID, Kind: in.Kind, Props: copyProps(in.Props)}
	delete(tx.overlay.deletedEdges, e.ID)
	tx.overlay.addedEdges[e.ID] = e
	return cloneEdge(e), nil
}

func (tx *fileTx) ListEdges(ctx context.Context) ([]graph.Edge, error) {
	if err := tx.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := tx.session.ensureRead(); err != nil {
		return nil, err
	}
	base, err := tx.session.readEdges()
	if err != nil {
		return nil, err
	}
	return tx.mergedEdges(base), nil
}

func (tx *fileTx) Children(ctx context.Context, parentID graph.NodeID) ([]graph.Edge, error) {
	edges, err := tx.ListEdges(ctx)
	if err != nil {
		return nil, err
	}
	out := []graph.Edge{}
	for _, edge := range edges {
		if edge.Kind == graph.EdgeKindContains && edge.FromID == parentID {
			out = append(out, edge)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, leftOK := edgeOrderNumber(out[i])
		right, rightOK := edgeOrderNumber(out[j])
		if leftOK && rightOK && left != right {
			return left < right
		}
		if leftOK != rightOK {
			return leftOK
		}
		return out[i].ID.String() < out[j].ID.String()
	})
	return cloneEdges(out), nil
}

func (tx *fileTx) Parent(ctx context.Context, childID graph.NodeID) (*graph.Edge, error) {
	edges, err := tx.ListEdges(ctx)
	if err != nil {
		return nil, err
	}
	for _, edge := range edges {
		if edge.Kind == graph.EdgeKindContains && edge.ToID == childID {
			cloned := cloneEdge(edge)
			return &cloned, nil
		}
	}
	return nil, nil
}

func (tx *fileTx) AddGraph(ctx context.Context, in sessionapi.AddGraphInput) error {
	_, err := tx.ApplyGraph(ctx, sessionapi.ApplyGraphInput{AddNodes: in.Nodes, AddEdges: in.Edges, Atomic: in.Atomic})
	return err
}

func (tx *fileTx) ApplyGraph(ctx context.Context, in sessionapi.ApplyGraphInput) (sessionapi.ApplyGraphResult, error) {
	if err := tx.ensureWritable(ctx); err != nil {
		return sessionapi.ApplyGraphResult{}, err
	}
	snapshot := tx.overlay.clone()
	result := sessionapi.ApplyGraphResult{}
	fail := func(err error) (sessionapi.ApplyGraphResult, error) {
		tx.overlay = snapshot
		return sessionapi.ApplyGraphResult{}, err
	}
	for _, del := range in.DeleteNodes {
		nodesBefore, err := tx.ListNodes(ctx)
		if err != nil {
			return fail(err)
		}
		edgesBefore, err := tx.ListEdges(ctx)
		if err != nil {
			return fail(err)
		}
		deletedIDs, _, remainingEdges, err := tx.session.applyDeleteNode(nodesBefore, edgesBefore, del)
		if err != nil {
			return fail(err)
		}
		if err := tx.DeleteNode(ctx, del); err != nil {
			return fail(err)
		}
		result.DeletedNodeIDs = append(result.DeletedNodeIDs, deletedIDs...)
		result.DeletedEdgeIDs = append(result.DeletedEdgeIDs, deletedEdges(edgesBefore, remainingEdges)...)
	}
	for _, del := range in.DeleteEdges {
		if err := tx.DeleteEdge(ctx, del); err != nil {
			return fail(err)
		}
		result.DeletedEdgeIDs = append(result.DeletedEdgeIDs, del.ID)
	}
	for _, add := range in.AddNodes {
		node, err := tx.AddNode(ctx, add)
		if err != nil {
			return fail(err)
		}
		result.AddedNodes = append(result.AddedNodes, node)
	}
	for _, add := range in.AddEdges {
		edge, err := tx.AddEdge(ctx, add)
		if err != nil {
			return fail(err)
		}
		result.AddedEdges = append(result.AddedEdges, edge)
	}
	return result, nil
}

func (tx *fileTx) MoveSubtree(ctx context.Context, in sessionapi.MoveSubtreeInput) (graph.Edge, error) {
	if err := tx.ensureWritable(ctx); err != nil {
		return graph.Edge{}, err
	}
	if in.NodeID == uuid.Nil {
		return graph.Edge{}, fmt.Errorf("%w: node_id is required", tx.session.errors.NotFound)
	}
	if in.NewParentID == uuid.Nil {
		return graph.Edge{}, fmt.Errorf("%w: new_parent_id is required", tx.session.errors.NotFound)
	}
	if in.Order != nil && *in.Order < 0 {
		return graph.Edge{}, fmt.Errorf("%w: order must be non-negative", storetemplate.ErrInvalidInput)
	}
	nodes, err := tx.ListNodes(ctx)
	if err != nil {
		return graph.Edge{}, err
	}
	node, ok := findNode(nodes, in.NodeID)
	if !ok {
		return graph.Edge{}, fmt.Errorf("%w: node not found", tx.session.errors.NotFound)
	}
	newParent, ok := findNode(nodes, in.NewParentID)
	if !ok {
		return graph.Edge{}, fmt.Errorf("%w: new parent not found", tx.session.errors.NotFound)
	}
	if in.NodeID == in.NewParentID {
		return graph.Edge{}, fmt.Errorf("%w: cannot move a node under itself", storetemplate.ErrInvalidInput)
	}
	if node.DomainID != uuid.Nil && newParent.DomainID != uuid.Nil && node.DomainID != newParent.DomainID {
		return graph.Edge{}, fmt.Errorf("%w: contains edges cannot cross domains", storetemplate.ErrInvalidInput)
	}
	edges, err := tx.ListEdges(ctx)
	if err != nil {
		return graph.Edge{}, err
	}
	originalEdges := cloneEdges(edges)
	oldParentIndexes := containsParentEdgeIndexes(edges, in.NodeID)
	if len(oldParentIndexes) > 1 {
		return graph.Edge{}, fmt.Errorf("%w: node has multiple contains parents", storetemplate.ErrInvalidInput)
	}
	if containsPath(edges, in.NodeID, in.NewParentID) {
		return graph.Edge{}, fmt.Errorf("%w: move would create a contains cycle", storetemplate.ErrInvalidInput)
	}
	childTemplate, err := tx.session.nodeTemplate(ctx, node, "child")
	if err != nil {
		return graph.Edge{}, err
	}
	if err := tx.session.validateChild(ctx, newParent, childTemplate); err != nil {
		return graph.Edge{}, err
	}
	oldParentID := graph.NodeID(uuid.Nil)
	if len(oldParentIndexes) == 1 {
		oldEdge := edges[oldParentIndexes[0]]
		oldParentID = oldEdge.FromID
		if oldParentID == in.NewParentID {
			if in.Order == nil {
				return cloneEdge(oldEdge), nil
			}
			updated, err := setChildPosition(edges, in.NewParentID, in.NodeID, in.Order)
			if err != nil {
				return graph.Edge{}, err
			}
			for _, edge := range changedEdges(originalEdges, edges) {
				tx.stageEdgePut(edge)
			}
			return updated, nil
		}
		edges[oldParentIndexes[0]].FromID = in.NewParentID
		edges[oldParentIndexes[0]].ToID = in.NodeID
		edges[oldParentIndexes[0]].Kind = graph.EdgeKindContains
		edges[oldParentIndexes[0]].Props = copyProps(edges[oldParentIndexes[0]].Props)
	} else {
		edgeID, err := newGraphUUID()
		if err != nil {
			return graph.Edge{}, err
		}
		edges = append(edges, graph.Edge{ID: graph.EdgeID(edgeID), FromID: in.NewParentID, ToID: in.NodeID, Kind: graph.EdgeKindContains, Props: map[string]any{}})
	}
	if oldParentID != uuid.Nil {
		normalizeChildrenOrder(edges, oldParentID)
	}
	updated, err := setChildPosition(edges, in.NewParentID, in.NodeID, in.Order)
	if err != nil {
		return graph.Edge{}, err
	}
	for _, edge := range changedEdges(originalEdges, edges) {
		tx.stageEdgePut(edge)
	}
	return updated, nil
}

func (tx *fileTx) ReorderChildren(ctx context.Context, in sessionapi.ReorderChildrenInput) ([]graph.Edge, error) {
	if err := tx.ensureWritable(ctx); err != nil {
		return nil, err
	}
	if in.ParentID == uuid.Nil {
		return nil, fmt.Errorf("%w: parent_id is required", tx.session.errors.NotFound)
	}
	nodes, err := tx.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := findNode(nodes, in.ParentID); !ok {
		return nil, fmt.Errorf("%w: parent not found", tx.session.errors.NotFound)
	}
	edges, err := tx.ListEdges(ctx)
	if err != nil {
		return nil, err
	}
	originalEdges := cloneEdges(edges)
	childEdgeByID, err := validateCompleteChildOrder(edges, in.ParentID, in.ChildIDs)
	if err != nil {
		return nil, err
	}
	updated := make([]graph.Edge, 0, len(in.ChildIDs))
	for order, childID := range in.ChildIDs {
		edgeIndex := childEdgeByID[childID]
		ensureEdgeProps(&edges[edgeIndex])
		edges[edgeIndex].Props["order"] = order * childOrderStep
		updated = append(updated, cloneEdge(edges[edgeIndex]))
	}
	for _, edge := range changedEdges(originalEdges, edges) {
		tx.stageEdgePut(edge)
	}
	return updated, nil
}

func (tx *fileTx) GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error) {
	if err := tx.ensureOpen(ctx); err != nil {
		return graph.Node{}, err
	}
	nodes, err := tx.ListNodes(ctx)
	if err != nil {
		return graph.Node{}, err
	}
	if node, ok := findNode(nodes, id); ok {
		return cloneNode(node), nil
	}
	return graph.Node{}, tx.session.errors.NotFound
}

func (tx *fileTx) DeleteNode(ctx context.Context, in sessionapi.DeleteNodeInput) error {
	if err := tx.ensureWritable(ctx); err != nil {
		return err
	}
	nodes, err := tx.ListNodes(ctx)
	if err != nil {
		return err
	}
	edges, err := tx.ListEdges(ctx)
	if err != nil {
		return err
	}
	deleteIDs, _, remainingEdges, err := tx.session.applyDeleteNode(nodes, edges, in)
	if err != nil {
		return err
	}
	for _, id := range deleteIDs {
		if _, added := tx.overlay.addedNodes[id]; added {
			delete(tx.overlay.addedNodes, id)
			delete(tx.overlay.updatedNodes, id)
		} else {
			delete(tx.overlay.updatedNodes, id)
			tx.overlay.deletedNodes[id] = struct{}{}
		}
	}
	for _, id := range deletedEdges(edges, remainingEdges) {
		if _, added := tx.overlay.addedEdges[id]; added {
			delete(tx.overlay.addedEdges, id)
			delete(tx.overlay.updatedEdges, id)
		} else {
			delete(tx.overlay.updatedEdges, id)
			tx.overlay.deletedEdges[id] = struct{}{}
		}
	}
	return nil
}

func (tx *fileTx) DeleteEdge(ctx context.Context, in sessionapi.DeleteEdgeInput) error {
	if err := tx.ensureWritable(ctx); err != nil {
		return err
	}
	if in.ID == uuid.Nil {
		return fmt.Errorf("%w: edge_id is required", tx.session.errors.NotFound)
	}
	edges, err := tx.ListEdges(ctx)
	if err != nil {
		return err
	}
	if findEdgeIndex(edges, in.ID) < 0 {
		return tx.session.errors.NotFound
	}
	if _, added := tx.overlay.addedEdges[in.ID]; added {
		delete(tx.overlay.addedEdges, in.ID)
		delete(tx.overlay.updatedEdges, in.ID)
		return nil
	}
	delete(tx.overlay.updatedEdges, in.ID)
	tx.overlay.deletedEdges[in.ID] = struct{}{}
	return nil
}

func (tx *fileTx) stageEdgePut(edge graph.Edge) {
	delete(tx.overlay.deletedEdges, edge.ID)
	if _, added := tx.overlay.addedEdges[edge.ID]; added || !tx.edgeExistsInBase(edge.ID) {
		tx.overlay.addedEdges[edge.ID] = cloneEdge(edge)
		delete(tx.overlay.updatedEdges, edge.ID)
		return
	}
	tx.overlay.updatedEdges[edge.ID] = cloneEdge(edge)
}

func (tx *fileTx) edgeExistsInBase(id graph.EdgeID) bool {
	edges, err := tx.session.readEdges()
	if err != nil {
		return false
	}
	return findEdgeIndex(edges, id) >= 0
}

func (tx *fileTx) Commit(ctx context.Context) error {
	if err := tx.ensureOpen(ctx); err != nil {
		return err
	}
	if tx.options.ReadOnly || tx.overlay.empty() {
		tx.closed = true
		return nil
	}
	baseNodes, err := tx.session.readNodes()
	if err != nil {
		return err
	}
	baseEdges, err := tx.session.readEdges()
	if err != nil {
		return err
	}
	finalNodes := tx.mergedNodes(baseNodes)
	finalEdges := tx.mergedEdges(baseEdges)
	if err := tx.validateFinalGraph(ctx, finalNodes, finalEdges); err != nil {
		return err
	}
	putNodes, putEdges, deleteNodes, deleteEdges := tx.overlay.delta()
	promoted, err := tx.promoteReferencedStagedBlobs(ctx, putNodes)
	if err != nil {
		return err
	}
	if err := tx.session.commitGraphAtRevision(ctx, putNodes, putEdges, deleteNodes, deleteEdges, &tx.baseRevision); err != nil {
		tx.cleanupPromotedStagedBlobs(ctx, promoted)
		return err
	}
	tx.discardUnreferencedStagedBlobs(ctx, putNodes)
	tx.closed = true
	return nil
}

func (tx *fileTx) Rollback(ctx context.Context) error {
	if err := tx.ensureOpen(ctx); err != nil {
		return err
	}
	tx.discardAllStagedBlobs(ctx)
	tx.overlay = newTxOverlay()
	tx.stagedBlobs = nil
	tx.closed = true
	return nil
}

func (tx *fileTx) promoteReferencedStagedBlobs(ctx context.Context, putNodes []graph.Node) ([]txStagedBlob, error) {
	blobs, err := tx.session.blobStore()
	if err != nil {
		return nil, err
	}
	referenced := blobRefsInNodes(putNodes)
	promoted := []txStagedBlob{}
	for _, staged := range tx.stagedBlobs {
		if _, ok := referenced[staged.blobID]; !ok {
			continue
		}
		if err := blobs.Promote(ctx, staged.staged); err != nil {
			tx.cleanupPromotedStagedBlobs(ctx, promoted)
			return nil, err
		}
		promoted = append(promoted, staged)
	}
	return promoted, nil
}

func (tx *fileTx) cleanupPromotedStagedBlobs(ctx context.Context, promoted []txStagedBlob) {
	blobs, err := tx.session.blobStore()
	if err != nil {
		return
	}
	for _, staged := range promoted {
		if staged.existing || tx.session.blobHasCommittedRef(ctx, staged.blobID) {
			continue
		}
		_ = blobs.Delete(ctx, staged.blobID)
	}
	tx.discardAllStagedBlobs(ctx)
}

func (tx *fileTx) discardUnreferencedStagedBlobs(ctx context.Context, putNodes []graph.Node) {
	referenced := blobRefsInNodes(putNodes)
	blobs, err := tx.session.blobStore()
	if err != nil {
		return
	}
	for _, staged := range tx.stagedBlobs {
		if _, ok := referenced[staged.blobID]; ok {
			continue
		}
		_ = blobs.Discard(ctx, staged.staged)
	}
}

func (tx *fileTx) discardAllStagedBlobs(ctx context.Context) {
	blobs, err := tx.session.blobStore()
	if err != nil {
		return
	}
	for _, staged := range tx.stagedBlobs {
		_ = blobs.Discard(ctx, staged.staged)
	}
}

func blobRefsInNodes(nodes []graph.Node) map[graph.BlobID]struct{} {
	out := map[graph.BlobID]struct{}{}
	for _, node := range nodes {
		if node.BlobRef != nil {
			out[*node.BlobRef] = struct{}{}
		}
	}
	return out
}

func (s *FileSession) blobHasCommittedRef(ctx context.Context, id graph.BlobID) bool {
	store, err := s.graphStore()
	if err != nil {
		return false
	}
	count, err := store.BlobRefCount(ctx, id)
	return err == nil && count > 0
}

func (tx *fileTx) ensureOpen(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if tx.closed {
		return sessionapi.ErrTransactionClosed
	}
	return tx.session.ensureOpen(ctx)
}

func (tx *fileTx) ensureWritable(ctx context.Context) error {
	if err := tx.ensureOpen(ctx); err != nil {
		return err
	}
	if tx.options.ReadOnly {
		return sessionapi.ErrReadOnlyTransaction
	}
	return tx.session.ensureWrite()
}

func (tx *fileTx) validateFinalGraph(ctx context.Context, nodes []graph.Node, edges []graph.Edge) error {
	nodeByID := make(map[graph.NodeID]graph.Node, len(nodes))
	for _, node := range nodes {
		if _, exists := nodeByID[node.ID]; exists {
			return fmt.Errorf("%w: duplicate node %s", storetemplate.ErrInvalidInput, node.ID)
		}
		nodeByID[node.ID] = node
	}
	edgeByID := make(map[graph.EdgeID]struct{}, len(edges))
	containsParent := map[graph.NodeID]graph.EdgeID{}
	for _, edge := range edges {
		if _, exists := edgeByID[edge.ID]; exists {
			return fmt.Errorf("%w: duplicate edge %s", storetemplate.ErrInvalidInput, edge.ID)
		}
		edgeByID[edge.ID] = struct{}{}
		from, fromOK := nodeByID[edge.FromID]
		to, toOK := nodeByID[edge.ToID]
		if !fromOK || !toOK {
			return fmt.Errorf("%w: edge endpoint not found", tx.session.errors.NotFound)
		}
		if edge.Kind != graph.EdgeKindContains {
			continue
		}
		if edge.FromID == edge.ToID {
			return fmt.Errorf("%w: contains edge cannot target itself", storetemplate.ErrInvalidInput)
		}
		if existing, ok := containsParent[edge.ToID]; ok && existing != edge.ID {
			return fmt.Errorf("%w: node already has a contains parent", storetemplate.ErrInvalidInput)
		}
		containsParent[edge.ToID] = edge.ID
		if containsPath(edges, edge.ToID, edge.FromID) {
			return fmt.Errorf("%w: contains edge would create a cycle", storetemplate.ErrInvalidInput)
		}
		childTemplate, err := tx.session.nodeTemplate(ctx, to, "child")
		if err != nil {
			return err
		}
		if err := tx.session.validateChild(ctx, from, childTemplate); err != nil {
			return err
		}
	}
	return nil
}

func (tx *fileTx) mergedNodes(base []graph.Node) []graph.Node {
	out := make([]graph.Node, 0, len(base)+len(tx.overlay.addedNodes))
	seen := map[graph.NodeID]struct{}{}
	for _, node := range base {
		if _, deleted := tx.overlay.deletedNodes[node.ID]; deleted {
			continue
		}
		if updated, ok := tx.overlay.updatedNodes[node.ID]; ok {
			out = append(out, cloneNode(updated))
		} else {
			out = append(out, cloneNode(node))
		}
		seen[node.ID] = struct{}{}
	}
	for _, node := range tx.overlay.addedNodes {
		if _, deleted := tx.overlay.deletedNodes[node.ID]; deleted {
			continue
		}
		if _, ok := seen[node.ID]; ok {
			continue
		}
		out = append(out, cloneNode(node))
	}
	return out
}

func (tx *fileTx) mergedEdges(base []graph.Edge) []graph.Edge {
	liveNodes := map[graph.NodeID]struct{}{}
	nodes, _ := tx.mergedNodesNoErr()
	for _, node := range nodes {
		liveNodes[node.ID] = struct{}{}
	}
	out := make([]graph.Edge, 0, len(base)+len(tx.overlay.addedEdges))
	seen := map[graph.EdgeID]struct{}{}
	for _, edge := range base {
		if _, deleted := tx.overlay.deletedEdges[edge.ID]; deleted {
			continue
		}
		if !edgeEndpointsLive(edge, liveNodes) {
			continue
		}
		if updated, ok := tx.overlay.updatedEdges[edge.ID]; ok {
			out = append(out, cloneEdge(updated))
		} else {
			out = append(out, cloneEdge(edge))
		}
		seen[edge.ID] = struct{}{}
	}
	for _, edge := range tx.overlay.addedEdges {
		if _, deleted := tx.overlay.deletedEdges[edge.ID]; deleted {
			continue
		}
		if _, ok := seen[edge.ID]; ok {
			continue
		}
		if !edgeEndpointsLive(edge, liveNodes) {
			continue
		}
		out = append(out, cloneEdge(edge))
	}
	return out
}

func (tx *fileTx) mergedNodesNoErr() ([]graph.Node, error) {
	base, err := tx.session.readNodes()
	if err != nil {
		return nil, err
	}
	return tx.mergedNodes(base), nil
}

func edgeEndpointsLive(edge graph.Edge, liveNodes map[graph.NodeID]struct{}) bool {
	_, fromOK := liveNodes[edge.FromID]
	_, toOK := liveNodes[edge.ToID]
	return fromOK && toOK
}

func findEdgeIndex(edges []graph.Edge, id graph.EdgeID) int {
	for i, edge := range edges {
		if edge.ID == id {
			return i
		}
	}
	return -1
}

func cloneNode(node graph.Node) graph.Node {
	node.Props = copyProps(node.Props)
	return node
}
