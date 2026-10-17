package physical

import (
	"fmt"
	"math"
	"sort"
	"strings"

	graph "github.com/myceldb/mycel/internal/graph/model"
	querybinding "github.com/myceldb/mycel/internal/query/binding"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryschema "github.com/myceldb/mycel/internal/query/schema"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
)

const (
	defaultSubtreeMaxNodes       = 10_000
	defaultMaxIndexEntries       = 50_000
	defaultMaxEdgesLoaded        = 50_000
	defaultMaxAdjacencyCalls     = 10_000
	defaultMaxCandidates         = 50_000
	defaultMaxOutputRows         = 10_000
	defaultMaxMemoryBytes        = 64 << 20
	defaultPathMaxStartNodes     = 1_000
	defaultMaxIntermediateRows   = 10_000
	defaultMaxNodesLoaded        = 50_000
	defaultMaxSemanticCandidates = 500
	defaultStructuredDepth       = 64
	defaultGQLDepth              = 5
)

const legacyBridgeReason = "physical plan execution has not migrated from the daemon adapter"

type planner struct {
	environment Environment
	catalog     queryschema.Catalog
}

// Build selects a physical plan from a successfully bound statement. Planning
// is side-effect-free: it uses only the supplied schema and capability facts.
func Build(bound querybinding.BoundStatement, environment Environment) (Plan, error) {
	p := planner{environment: environment, catalog: queryschema.NewCatalog(environment.Schema)}
	statement := bound.Statement
	if statement.Kind == querylogical.StatementMutation {
		if !environment.Capabilities.Mutations {
			return Plan{}, p.unavailable("mutation access is not configured")
		}
		plan := Plan{
			Name: "LegacyMutation", Family: FamilyLegacyMutation,
			Cursor: CursorStrategy{}, Limits: ResourceLimits{},
			LegacyBridgeReason: legacyBridgeReason, SchemaVersion: bound.SchemaVersion,
		}
		plan.Root = legacyRoot(Operator{Kind: OperatorProject, Name: "MutationResult"})
		return plan, nil
	}
	if statement.Kind != querylogical.StatementRead || statement.Read == nil {
		return Plan{}, p.invalid("physical planning requires one bound read or mutation statement")
	}

	query := *statement.Read
	rules := []func(querylogical.Query) (Plan, bool, error){
		p.planIndexedPath,
		p.planIndexedAdjacency,
		p.planIndexedEquality,
		p.planSemantic,
		p.planIndexedPredicate,
		p.planOrdered,
	}
	for _, rule := range rules {
		plan, matched, err := rule(query)
		if err != nil {
			return Plan{}, err
		}
		if matched {
			return p.finish(plan, bound, query), nil
		}
	}
	plan, err := p.planFullScan(query)
	if err != nil {
		return Plan{}, err
	}
	return p.finish(plan, bound, query), nil
}

func (p planner) planIndexedPath(query querylogical.Query) (Plan, bool, error) {
	if strings.TrimSpace(query.PathAlias) == "" || len(query.Steps) == 0 || len(query.OrderBy) != 0 {
		return Plan{}, false, nil
	}
	if len(query.Start.NodeIDs) == 0 && len(query.Start.Labels) != 1 {
		return Plan{}, false, nil
	}

	access := AccessPath{
		StartAlias: query.Start.Alias,
		Labels:     append([]string(nil), query.Start.Labels...),
		NodeIDs:    append([]string(nil), query.Start.NodeIDs...),
	}
	indexes := []IndexUse{}
	var pushed *querylogical.Predicate
	switch {
	case len(query.Start.NodeIDs) > 0:
		access.Kind = AccessNodeIDs
		if !p.environment.Capabilities.EntityReads {
			return Plan{}, true, p.unavailable("indexed path requires entity reads")
		}
	case query.Predicate == nil:
		access.Kind = AccessLabel
		indexes = append(indexes, builtInIndex("label:"+query.Start.Labels[0], query.Start.Labels[0]))
	default:
		tags, tagOK, err := tagBranches(query.Predicate, query.Start.Alias)
		if err != nil {
			return Plan{}, true, err
		}
		if tagOK {
			access.Kind = AccessTag
			access.TagBranches = tags
			pushed = query.Predicate
			for _, branch := range tags {
				for _, tag := range branch {
					indexes = append(indexes, builtInIndex("tag:"+tag, query.Start.Labels[0]))
				}
			}
			break
		}
		branches, indexed, predicateOK, err := indexedBranches(query.Predicate, query.Start.Alias, query.Source, false)
		if err != nil {
			return Plan{}, true, err
		}
		if !predicateOK {
			return Plan{}, false, nil
		}
		resolved, uses, err := p.resolveBranches(query.Start.Labels[0], branches)
		if err != nil {
			return Plan{}, true, err
		}
		access.Kind = AccessCompoundProperty
		access.Branches = resolved
		pushed = indexed
		indexes = append(indexes, uses...)
	}
	if access.Kind != AccessNodeIDs && !p.environment.Capabilities.Indexes {
		return Plan{}, true, p.unavailable("indexed path start requires index access")
	}
	if !p.environment.Capabilities.Traversals || !p.environment.Capabilities.EntityReads {
		return Plan{}, true, p.unavailable("indexed path requires traversal and entity access")
	}
	for i, step := range query.Steps {
		if len(step.Edge.Labels) != 1 || strings.TrimSpace(step.Edge.Labels[0]) == "" {
			return Plan{}, true, p.invalid("indexed path step %d requires exactly one edge label", i)
		}
		if step.Edge.Direction != querylogical.DirectionIncoming && step.Edge.Direction != querylogical.DirectionOutgoing {
			return Plan{}, true, p.invalid("indexed path step %d requires directed traversal", i)
		}
		maxDepth := p.resolvedMaxDepth(query.Source, step.Depth.Max)
		if maxDepth < step.Depth.Min || maxDepth <= 0 {
			return Plan{}, true, p.invalid("indexed path step %d has invalid depth bounds", i)
		}
		access.Steps = append(access.Steps, TraversalStep{
			EdgeAlias: step.Edge.Alias, Labels: append([]string(nil), step.Edge.Labels...), Direction: step.Edge.Direction,
			MinDepth: step.Depth.Min, MaxDepth: maxDepth, TargetAlias: step.Target.Alias,
			TargetLabels: append([]string(nil), step.Target.Labels...),
		})
		indexes = append(indexes, adjacencyIndex(step))
	}
	return Plan{
		Name: "IndexedMultiHopAdjacencyPathScan", Family: FamilyIndexedPath,
		Access: access, Indexes: dedupeIndexes(indexes), PushedPredicate: pushed, ResidualPredicate: query.Predicate,
		Cursor: legacyCursor("offset"), Limits: p.resourceLimits(query, FamilyIndexedPath),
	}, true, nil
}

func (p planner) planIndexedAdjacency(query querylogical.Query) (Plan, bool, error) {
	if len(query.OrderBy) != 0 || len(query.Steps) != 1 || len(query.Start.NodeIDs) == 0 || strings.TrimSpace(query.Steps[0].Edge.Alias) == "" {
		return Plan{}, false, nil
	}
	step := query.Steps[0]
	if step.Depth.Min > 1 || p.resolvedMaxDepth(query.Source, step.Depth.Max) != 1 {
		return Plan{}, true, p.invalid("indexed adjacency requires maximum depth 1 and minimum depth at most 1")
	}
	if len(step.Edge.Labels) != 1 || strings.TrimSpace(step.Edge.Labels[0]) == "" {
		return Plan{}, true, p.invalid("indexed adjacency requires exactly one edge label")
	}
	if step.Edge.Direction != querylogical.DirectionIncoming && step.Edge.Direction != querylogical.DirectionOutgoing {
		return Plan{}, true, p.invalid("indexed adjacency requires directed traversal")
	}
	if !p.environment.Capabilities.Traversals || !p.environment.Capabilities.EntityReads {
		return Plan{}, true, p.unavailable("indexed adjacency requires traversal and entity access")
	}
	access := AccessPath{
		Kind: AccessAdjacency, StartAlias: query.Start.Alias,
		Labels: append([]string(nil), query.Start.Labels...), NodeIDs: append([]string(nil), query.Start.NodeIDs...),
		Steps: []TraversalStep{{
			EdgeAlias: step.Edge.Alias, Labels: append([]string(nil), step.Edge.Labels...), Direction: step.Edge.Direction,
			MinDepth: step.Depth.Min, MaxDepth: 1, TargetAlias: step.Target.Alias, TargetLabels: append([]string(nil), step.Target.Labels...),
		}},
	}
	return Plan{
		Name: "EdgeAdjacencyIndexScan", Family: FamilyIndexedAdjacency,
		Access: access, Indexes: []IndexUse{adjacencyIndex(step)}, ResidualPredicate: query.Predicate,
		Cursor: legacyCursor("adjacency_key"), Limits: p.resourceLimits(query, FamilyIndexedAdjacency),
	}, true, nil
}

func (p planner) planIndexedEquality(query querylogical.Query) (Plan, bool, error) {
	if len(query.OrderBy) != 0 || len(query.Steps) != 0 || len(query.Start.Labels) != 1 {
		return Plan{}, false, nil
	}
	scan, ok := equalityScan(query.Predicate, query.Start.Alias)
	if !ok {
		return Plan{}, false, nil
	}
	if !p.environment.Capabilities.Indexes {
		return Plan{}, true, p.unavailable("indexed equality requires index access")
	}
	resolved, use, err := p.resolveScan(query.Start.Labels[0], scan)
	if err != nil {
		return Plan{}, true, err
	}
	field := resolved.Field
	return Plan{
		Name: "OrderedNodePropertyEqualityIndexScan", Family: FamilyIndexedEquality,
		Access:  AccessPath{Kind: AccessOrderedProperty, StartAlias: query.Start.Alias, Labels: append([]string(nil), query.Start.Labels...), Field: &field, Index: resolved.Index, Low: resolved.Low, High: resolved.High},
		Indexes: []IndexUse{use}, PushedPredicate: query.Predicate,
		Ordering: OrderingGuarantee{Stable: true, Source: resolved.Index}, Cursor: legacyCursor("index_key"),
		Limits: p.resourceLimits(query, FamilyIndexedEquality),
	}, true, nil
}

func (p planner) planSemantic(query querylogical.Query) (Plan, bool, error) {
	if len(query.OrderBy) != 0 || len(query.Steps) != 0 || len(query.Start.Labels) != 1 || !gqlIndexedPredicatesSupported(query) {
		return Plan{}, false, nil
	}
	semantic, residual, found, err := semanticPredicate(query.Predicate)
	if err != nil || !found {
		return Plan{}, found, err
	}
	if !p.environment.Capabilities.SemanticCandidates || !p.environment.Capabilities.EntityReads {
		return Plan{}, true, p.unavailable("semantic plan requires semantic candidate and entity access")
	}
	field := *semantic.Field
	name := "semantic"
	if strings.TrimSpace(semantic.RuleRef) != "" {
		name = "semantic:" + semantic.RuleRef
	}
	pushed := querylogical.LeafPredicate(semantic)
	return Plan{
		Name: "SemanticVectorSearch", Family: FamilySemantic,
		Access:          AccessPath{Kind: AccessSemantic, StartAlias: query.Start.Alias, Labels: append([]string(nil), query.Start.Labels...), Field: &field, Index: name},
		Indexes:         []IndexUse{{Name: name, Label: query.Start.Labels[0], Namespace: string(field.Namespace), Field: field.Name}},
		PushedPredicate: pushed, ResidualPredicate: residual,
		Cursor: legacyCursor("offset"), Limits: p.resourceLimits(query, FamilySemantic),
	}, true, nil
}

func (p planner) planIndexedPredicate(query querylogical.Query) (Plan, bool, error) {
	if len(query.OrderBy) != 0 || len(query.Steps) != 0 || len(query.Start.Labels) != 1 {
		return Plan{}, false, nil
	}
	branches, pushed, ok, err := indexedBranches(query.Predicate, query.Start.Alias, query.Source, true)
	if err != nil {
		return Plan{}, true, err
	}
	if !ok || len(branches) == 0 {
		return Plan{}, false, nil
	}
	if !p.environment.Capabilities.Indexes {
		return Plan{}, true, p.unavailable("indexed predicate requires index access")
	}
	resolved, uses, err := p.resolveBranches(query.Start.Labels[0], branches)
	if err != nil {
		return Plan{}, true, err
	}
	return Plan{
		Name: "OrderedNodePropertyPredicateIndexScan", Family: FamilyIndexedPredicate,
		Access:  AccessPath{Kind: AccessCompoundProperty, StartAlias: query.Start.Alias, Labels: append([]string(nil), query.Start.Labels...), Branches: resolved},
		Indexes: uses, PushedPredicate: pushed, ResidualPredicate: query.Predicate,
		Cursor: legacyCursor("offset"), Limits: p.resourceLimits(query, FamilyIndexedPredicate),
	}, true, nil
}

func (p planner) planOrdered(query querylogical.Query) (Plan, bool, error) {
	if len(query.OrderBy) == 0 {
		return Plan{}, false, nil
	}
	if len(query.OrderBy) != 1 || len(query.Start.Labels) != 1 {
		return Plan{}, true, p.unavailable("ORDER BY requires one ordered single-label node query")
	}
	if gqlOrderedNodeRequiresBroadFallback(query) {
		return Plan{}, false, nil
	}
	orderField, ok := valueField(query.OrderBy[0].Value)
	if !ok || orderField.Alias != query.Start.Alias || orderField.Namespace != querylogical.NamespaceProperties {
		return Plan{}, true, p.unavailable("ORDER BY requires an indexed property reference on the start alias")
	}
	if !p.environment.Capabilities.Indexes {
		return Plan{}, true, p.unavailable("ordered query requires index access")
	}
	index, ok := p.catalog.FindOrderedNodeIndex(query.Start.Labels[0], orderField.Name)
	if !ok {
		return Plan{}, true, p.missingOrderedIndex(query.Start.Labels[0], orderField.Name)
	}
	var low, high *Bound
	var err error
	if query.Source != querylogical.SourceGQL || len(query.Steps) != 0 {
		low, high, err = orderedBounds(query.Predicate, orderField)
		if err != nil {
			return Plan{}, true, p.unavailable("%s", err)
		}
	}
	pushed, residual := query.Predicate, (*querylogical.Predicate)(nil)
	if query.Source == querylogical.SourceGQL && len(query.Steps) == 0 {
		pushed, residual = nil, query.Predicate
	}
	use := schemaIndexUse(index, query.Start.Labels[0])
	access := AccessPath{
		Kind: AccessOrderedProperty, StartAlias: query.Start.Alias, Labels: append([]string(nil), query.Start.Labels...),
		Field: &orderField, Index: index.Name, Low: low, High: high,
	}
	plan := Plan{
		Name: "OrderedNodePropertyIndexScan", Family: FamilyIndexedOrder, Access: access,
		Indexes: []IndexUse{use}, PushedPredicate: pushed, ResidualPredicate: residual,
		Ordering: OrderingGuarantee{Terms: append([]querylogical.Order(nil), query.OrderBy...), Stable: true, Source: index.Name},
		Cursor:   legacyCursor("index_key"), Limits: p.resourceLimits(query, FamilyIndexedOrder),
	}
	if len(query.Steps) == 0 {
		return plan, true, nil
	}
	if len(query.Steps) != 1 {
		return Plan{}, true, p.unavailable("ORDER BY traversal requires an indexed bounded subtree query")
	}
	step := query.Steps[0]
	if strings.TrimSpace(step.Edge.Alias) != "" {
		return Plan{}, true, p.unavailable("indexed subtree traversal does not support edge aliases")
	}
	if len(step.Edge.Labels) != 1 || strings.TrimSpace(step.Edge.Labels[0]) == "" {
		return Plan{}, true, p.invalid("indexed subtree requires exactly one edge label")
	}
	if step.Edge.Direction != querylogical.DirectionIncoming && step.Edge.Direction != querylogical.DirectionOutgoing {
		return Plan{}, true, p.invalid("indexed subtree requires directed traversal")
	}
	if !p.environment.Capabilities.Traversals {
		return Plan{}, true, p.unavailable("indexed subtree requires traversal access")
	}
	maxDepth := p.resolvedMaxDepth(query.Source, step.Depth.Max)
	if maxDepth < step.Depth.Min || maxDepth <= 0 {
		return Plan{}, true, p.invalid("indexed subtree has invalid traversal depth")
	}
	access.Steps = []TraversalStep{{
		Labels: append([]string(nil), step.Edge.Labels...), Direction: step.Edge.Direction,
		MinDepth: step.Depth.Min, MaxDepth: maxDepth, TargetAlias: step.Target.Alias,
		TargetLabels: append([]string(nil), step.Target.Labels...),
	}}
	plan.Name = "OrderedNodePropertyIndexScan+EdgeAdjacencyIndexScan"
	plan.Family = FamilyIndexedSubtree
	plan.Access = access
	plan.Indexes = append(plan.Indexes, adjacencyIndex(step))
	plan.Cursor = legacyCursor("root_index_key")
	plan.Limits = p.resourceLimits(query, FamilyIndexedSubtree)
	return plan, true, nil
}

func (p planner) planFullScan(query querylogical.Query) (Plan, error) {
	if !p.environment.Domain.BroadSearchable {
		return Plan{}, p.unavailable("query shape requires broad fallback but domain is not broad-searchable")
	}
	if !p.environment.Capabilities.EntityReads {
		return Plan{}, p.unavailable("bounded full scan requires entity reads")
	}
	return Plan{
		Name: "BoundedFullGraphScan", Family: FamilyBoundedFullScan,
		Access:            AccessPath{Kind: AccessFullGraph, StartAlias: query.Start.Alias, Labels: append([]string(nil), query.Start.Labels...), NodeIDs: append([]string(nil), query.Start.NodeIDs...)},
		ResidualPredicate: query.Predicate, Cursor: legacyCursor("offset"),
		Limits: p.resourceLimits(query, FamilyBoundedFullScan), FullScan: true, FallbackMode: "broad_graph_fallback",
	}, nil
}

func (p planner) finish(plan Plan, bound querybinding.BoundStatement, query querylogical.Query) Plan {
	plan.SchemaVersion = bound.SchemaVersion
	plan.LegacyBridgeReason = legacyBridgeReason
	plan.Indexes = dedupeIndexes(plan.Indexes)
	plan.Root = legacyRoot(operatorPipeline(plan, query))
	return plan
}

func operatorPipeline(plan Plan, query querylogical.Query) Operator {
	accessKind := OperatorFullGraph
	switch plan.Family {
	case FamilyIndexedPath:
		accessKind = OperatorIndexedPath
	case FamilyIndexedAdjacency:
		accessKind = OperatorAdjacency
	case FamilyIndexedEquality, FamilyIndexedOrder:
		accessKind = OperatorOrderedIndex
	case FamilyIndexedPredicate:
		accessKind = OperatorCompoundIndex
	case FamilySemantic:
		accessKind = OperatorSemanticCandidates
	case FamilyIndexedSubtree:
		rootScan := Operator{Kind: OperatorOrderedIndex, Name: "OrderedNodePropertyIndexScan"}
		access := Operator{Kind: OperatorSubtree, Name: plan.Name, Inputs: []Operator{rootScan}}
		return shapingPipeline(access, plan, query)
	}
	return shapingPipeline(Operator{Kind: accessKind, Name: plan.Name}, plan, query)
}

func shapingPipeline(input Operator, plan Plan, query querylogical.Query) Operator {
	current := input
	if plan.ResidualPredicate != nil {
		current = Operator{Kind: OperatorFilter, Inputs: []Operator{current}}
	}
	if len(query.Aggregates) > 0 {
		current = Operator{Kind: OperatorAggregate, Inputs: []Operator{current}}
		if len(query.OrderBy) > 0 {
			current = Operator{Kind: OperatorSort, Inputs: []Operator{current}}
		}
	} else {
		if len(query.OrderBy) > 0 {
			kind := OperatorSort
			if physicalTopNSafe(query) {
				kind = OperatorTopN
			}
			current = Operator{Kind: kind, Inputs: []Operator{current}}
		}
		current = Operator{Kind: OperatorProject, Inputs: []Operator{current}}
	}
	if query.Distinct {
		current = Operator{Kind: OperatorDistinct, Inputs: []Operator{current}}
	}
	if query.Page.Offset > 0 {
		current = Operator{Kind: OperatorOffset, Inputs: []Operator{current}}
	}
	if query.Page.Limit > 0 || query.Page.Size > 0 {
		current = Operator{Kind: OperatorLimit, Inputs: []Operator{current}}
	}
	return current
}

func physicalTopNSafe(query querylogical.Query) bool {
	return query.Page.Limit > 0 && query.Page.Offset >= 0 && query.Page.Offset <= int64(math.MaxInt)-query.Page.Limit && !query.Distinct && len(query.Aggregates) == 0
}

func legacyRoot(input Operator) Operator {
	return Operator{Kind: OperatorLegacyBridge, Name: "LegacyBridge", Inputs: []Operator{input}}
}

func legacyCursor(kind string) CursorStrategy {
	return CursorStrategy{Kind: kind, LegacyForm: true}
}

func (p planner) resolveBranches(label string, branches []candidateBranch) ([]IndexBranch, []IndexUse, error) {
	resolved := make([]IndexBranch, 0, len(branches))
	uses := []IndexUse{}
	for _, branch := range branches {
		out := IndexBranch{Scans: make([]IndexScan, 0, len(branch.scans))}
		for _, scan := range branch.scans {
			resolvedScan, use, err := p.resolveScan(label, scan.scan)
			if err != nil {
				return nil, nil, err
			}
			out.Scans = append(out.Scans, resolvedScan)
			uses = append(uses, use)
		}
		resolved = append(resolved, out)
	}
	return resolved, dedupeIndexes(uses), nil
}

func (p planner) resolveScan(label string, scan IndexScan) (IndexScan, IndexUse, error) {
	if !p.catalog.Enabled() {
		return IndexScan{}, IndexUse{}, p.missingOrderedIndex(label, scan.Field.Name)
	}
	index, ok := p.catalog.FindOrderedNodeIndex(label, scan.Field.Name)
	if !ok {
		return IndexScan{}, IndexUse{}, p.missingOrderedIndex(label, scan.Field.Name)
	}
	scan.Index = index.Name
	return scan, schemaIndexUse(index, label), nil
}

func (p planner) resourceLimits(query querylogical.Query, family Family) ResourceLimits {
	limits := ResourceLimits{
		MaxCandidates:       defaultMaxCandidates,
		MaxIntermediateRows: defaultMaxIntermediateRows,
		MaxOutputRows:       defaultMaxOutputRows,
		MaxMemoryBytes:      defaultMaxMemoryBytes,
		MaxTraversalDepth:   maxQueryDepth(query, p),
	}
	switch family {
	case FamilyIndexedPath:
		limits.MaxIndexEntries = defaultMaxIndexEntries
		limits.MaxStartNodes = defaultPathMaxStartNodes
		limits.MaxNodesLoaded = defaultMaxNodesLoaded
		limits.MaxEdgesLoaded = defaultMaxEdgesLoaded
		limits.MaxAdjacencyCalls = defaultMaxAdjacencyCalls
	case FamilyIndexedAdjacency:
		limits.MaxIndexEntries = defaultMaxIndexEntries
		limits.MaxStartNodes = defaultPathMaxStartNodes
		limits.MaxNodesLoaded = defaultMaxNodesLoaded
		limits.MaxEdgesLoaded = defaultMaxEdgesLoaded
		limits.MaxAdjacencyCalls = defaultMaxAdjacencyCalls
	case FamilyIndexedEquality, FamilyIndexedPredicate, FamilyIndexedOrder:
		limits.MaxIndexEntries = defaultMaxIndexEntries
		limits.MaxNodesLoaded = defaultMaxNodesLoaded
	case FamilyIndexedSubtree:
		limits.MaxIndexEntries = defaultMaxIndexEntries
		limits.MaxStartNodes = defaultPathMaxStartNodes
		limits.MaxNodesLoaded, limits.MaxEdgesLoaded = graphLimits(query)
		limits.MaxAdjacencyCalls = defaultMaxAdjacencyCalls
	case FamilySemantic:
		limits.MaxSemanticCandidates = semanticCandidateLimit(query)
		limits.MaxNodesLoaded = limits.MaxSemanticCandidates
	case FamilyBoundedFullScan:
		limits.MaxNodesLoaded, limits.MaxEdgesLoaded = graphLimits(query)
		if len(query.Steps) > 0 {
			limits.MaxAdjacencyCalls = defaultMaxAdjacencyCalls
		}
	}
	return limits
}

func graphLimits(query querylogical.Query) (int64, int64) {
	maxNodes := int64(query.Safety.MaxNodes)
	if maxNodes <= 0 {
		maxNodes = defaultSubtreeMaxNodes
	}
	maxEdges := int64(query.Safety.MaxEdges)
	if maxEdges <= 0 {
		maxEdges = defaultMaxEdgesLoaded
	}
	return maxNodes, maxEdges
}

func semanticCandidateLimit(query querylogical.Query) int64 {
	var visit func(*querylogical.Predicate) int64
	visit = func(predicate *querylogical.Predicate) int64 {
		if predicate == nil {
			return 0
		}
		if predicate.Op == querylogical.PredicateLeafOp && predicate.Leaf != nil && predicate.Leaf.Kind == querylogical.LeafSemantic && predicate.Leaf.Limit > 0 {
			return int64(predicate.Leaf.Limit)
		}
		for i := range predicate.Terms {
			if limit := visit(&predicate.Terms[i]); limit > 0 {
				return limit
			}
		}
		return 0
	}
	if limit := visit(query.Predicate); limit > 0 {
		return limit
	}
	if query.Page.Size > 0 && int64(query.Page.Size) < defaultMaxSemanticCandidates {
		return int64(query.Page.Size)
	}
	if query.Page.Limit > 0 && query.Page.Limit < defaultMaxSemanticCandidates {
		return query.Page.Limit
	}
	return defaultMaxSemanticCandidates
}

func (p planner) resolvedMaxDepth(source querylogical.Source, maximum int) int {
	if maximum != -1 {
		return maximum
	}
	if p.environment.BindingLimits.OpenTraversalDepth > 0 {
		return p.environment.BindingLimits.OpenTraversalDepth
	}
	if source == querylogical.SourceGQL {
		return defaultGQLDepth
	}
	return defaultStructuredDepth
}

func maxQueryDepth(query querylogical.Query, p planner) int {
	maximum := 0
	for _, step := range query.Steps {
		if resolved := p.resolvedMaxDepth(query.Source, step.Depth.Max); resolved > maximum {
			maximum = resolved
		}
	}
	return maximum
}

func (p planner) invalid(format string, args ...any) error {
	return querylogical.NewError(querylogical.ErrorInvalidQuery, "plan query", fmt.Sprintf(format, args...), nil)
}

func (p planner) unavailable(format string, args ...any) error {
	return querylogical.NewError(querylogical.ErrorUnavailableAccessPath, "plan query", fmt.Sprintf(format, args...), nil)
}

func (p planner) missingOrderedIndex(label string, field string) error {
	required := label + ".properties." + field
	return querylogical.NewError(
		querylogical.ErrorUnavailableAccessPath,
		"plan query",
		fmt.Sprintf("no ordered index for %s", required),
		nil,
	).WithRequiredIndex(required)
}

type candidateScan struct {
	scan IndexScan
}

type candidateBranch struct {
	scans []candidateScan
}

func equalityScan(predicate *querylogical.Predicate, alias string) (IndexScan, bool) {
	if predicate == nil || predicate.Op != querylogical.PredicateLeafOp || predicate.Leaf == nil || predicate.Leaf.Kind != querylogical.LeafComparison || predicate.Leaf.Operator != "=" {
		return IndexScan{}, false
	}
	scan, ok := scanForLeaf(*predicate.Leaf, alias)
	return scan, ok
}

func indexedBranches(predicate *querylogical.Predicate, alias string, source querylogical.Source, strict bool) ([]candidateBranch, *querylogical.Predicate, bool, error) {
	if source == querylogical.SourceGQL && hasUnsupportedGQLIndexedComparison(predicate) {
		return nil, nil, false, nil
	}
	if predicate == nil {
		return nil, nil, false, nil
	}
	if predicate.Op == querylogical.PredicateOrOp {
		branches := []candidateBranch{}
		pushedTerms := []querylogical.Predicate{}
		for i := range predicate.Terms {
			childBranches, pushed, ok, err := indexedBranches(&predicate.Terms[i], alias, source, strict)
			if err != nil {
				return nil, nil, true, err
			}
			if !ok || len(childBranches) == 0 || pushed == nil {
				return nil, nil, false, nil
			}
			branches = append(branches, childBranches...)
			pushedTerms = append(pushedTerms, *pushed)
		}
		return branches, querylogical.CombinePredicate(querylogical.PredicateOrOp, pushedTerms), len(branches) > 0, nil
	}
	branch, pushed, err := indexedAndBranch(predicate, alias, source)
	if err != nil {
		if strict {
			return nil, nil, true, err
		}
		return nil, nil, false, nil
	}
	if len(branch.scans) == 0 || pushed == nil {
		return nil, nil, false, nil
	}
	return []candidateBranch{branch}, pushed, true, nil
}

func indexedAndBranch(predicate *querylogical.Predicate, alias string, source querylogical.Source) (candidateBranch, *querylogical.Predicate, error) {
	if predicate == nil {
		return candidateBranch{}, nil, nil
	}
	if predicate.Op == querylogical.PredicateAndOp {
		branch := candidateBranch{}
		terms := []querylogical.Predicate{}
		for i := range predicate.Terms {
			child, pushed, err := indexedAndBranch(&predicate.Terms[i], alias, source)
			if err != nil {
				return candidateBranch{}, nil, err
			}
			branch.scans = append(branch.scans, child.scans...)
			if pushed != nil {
				terms = append(terms, *pushed)
			}
		}
		return branch, querylogical.CombinePredicate(querylogical.PredicateAndOp, terms), nil
	}
	if predicate.Op != querylogical.PredicateLeafOp || predicate.Leaf == nil {
		return candidateBranch{}, nil, nil
	}
	if source == querylogical.SourceGQL && predicate.Leaf.Kind == querylogical.LeafComparison && predicate.Leaf.Operator != "=" {
		return candidateBranch{}, nil, nil
	}
	scan, ok := scanForLeaf(*predicate.Leaf, alias)
	if !ok {
		if err := malformedIndexedLeaf(*predicate.Leaf, alias, source); err != nil {
			return candidateBranch{}, nil, err
		}
		return candidateBranch{}, nil, nil
	}
	leaf := *predicate
	return candidateBranch{scans: []candidateScan{{scan: scan}}}, &leaf, nil
}

func scanForLeaf(leaf querylogical.Leaf, alias string) (IndexScan, bool) {
	field := querylogical.FieldRef{}
	scan := IndexScan{}
	switch leaf.Kind {
	case querylogical.LeafComparison:
		if leaf.Left == nil || leaf.Right == nil {
			return IndexScan{}, false
		}
		var ok bool
		field, ok = valueField(*leaf.Left)
		if !ok || !staticValue(*leaf.Right) {
			return IndexScan{}, false
		}
		value := *leaf.Right
		switch leaf.Operator {
		case "=":
			scan.Low, scan.High = &Bound{Value: value, Inclusive: true}, &Bound{Value: value, Inclusive: true}
		case "<":
			scan.High = &Bound{Value: value, Inclusive: false}
		case "<=":
			scan.High = &Bound{Value: value, Inclusive: true}
		case ">":
			scan.Low = &Bound{Value: value, Inclusive: false}
		case ">=":
			scan.Low = &Bound{Value: value, Inclusive: true}
		default:
			return IndexScan{}, false
		}
	case querylogical.LeafBetween:
		if leaf.Left == nil || leaf.Low == nil || leaf.High == nil || !staticValue(*leaf.Low) || !staticValue(*leaf.High) {
			return IndexScan{}, false
		}
		var ok bool
		field, ok = valueField(*leaf.Left)
		if !ok {
			return IndexScan{}, false
		}
		scan.Low = &Bound{Value: *leaf.Low, Inclusive: true}
		scan.High = &Bound{Value: *leaf.High, Inclusive: true}
	case querylogical.LeafPropertyExists:
		if leaf.Field == nil {
			return IndexScan{}, false
		}
		field = *leaf.Field
	case querylogical.LeafString:
		if leaf.Operand == nil {
			return IndexScan{}, false
		}
		var ok bool
		field, ok = valueField(*leaf.Operand)
		if !ok {
			return IndexScan{}, false
		}
	case querylogical.LeafText:
		if leaf.Field == nil {
			return IndexScan{}, false
		}
		field = *leaf.Field
	default:
		return IndexScan{}, false
	}
	if field.Alias != alias || field.Namespace != querylogical.NamespaceProperties || strings.TrimSpace(field.Name) == "" {
		return IndexScan{}, false
	}
	scan.Field = field
	return scan, true
}

func gqlOrderedNodeRequiresBroadFallback(query querylogical.Query) bool {
	if query.Source != querylogical.SourceGQL || len(query.Steps) != 0 {
		return false
	}
	var requiresBroad func(*querylogical.Predicate) bool
	requiresBroad = func(predicate *querylogical.Predicate) bool {
		if predicate == nil {
			return false
		}
		if predicate.Op == querylogical.PredicateLeafOp && predicate.Leaf != nil {
			switch predicate.Leaf.Kind {
			case querylogical.LeafComparison, querylogical.LeafBetween, querylogical.LeafText, querylogical.LeafSemantic:
				return true
			default:
				return false
			}
		}
		for i := range predicate.Terms {
			if requiresBroad(&predicate.Terms[i]) {
				return true
			}
		}
		return false
	}
	return requiresBroad(query.Predicate)
}

func gqlIndexedPredicatesSupported(query querylogical.Query) bool {
	return query.Source != querylogical.SourceGQL || !hasUnsupportedGQLIndexedComparison(query.Predicate)
}

func hasUnsupportedGQLIndexedComparison(predicate *querylogical.Predicate) bool {
	if predicate == nil {
		return false
	}
	if predicate.Op == querylogical.PredicateLeafOp && predicate.Leaf != nil {
		return predicate.Leaf.Kind == querylogical.LeafBetween ||
			(predicate.Leaf.Kind == querylogical.LeafComparison && predicate.Leaf.Operator != "=")
	}
	for i := range predicate.Terms {
		if hasUnsupportedGQLIndexedComparison(&predicate.Terms[i]) {
			return true
		}
	}
	return false
}

func malformedIndexedLeaf(leaf querylogical.Leaf, alias string, _ querylogical.Source) error {
	invalidField := func(field querylogical.FieldRef, ok bool) bool {
		return !ok || field.Alias != alias || strings.TrimSpace(field.Name) == ""
	}
	switch leaf.Kind {
	case querylogical.LeafComparison:
		if leaf.Operator != "=" && leaf.Operator != "<" && leaf.Operator != ">=" && leaf.Operator != "<=" {
			return nil
		}
		field, ok := querylogical.FieldRef{}, false
		if leaf.Left != nil {
			field, ok = valueField(*leaf.Left)
		}
		if invalidField(field, ok) || leaf.Right == nil || !staticValue(*leaf.Right) {
			return querylogical.NewError(querylogical.ErrorInvalidQuery, "plan query", "indexed comparison predicate must compare the start alias property with a static value", nil)
		}
	case querylogical.LeafBetween:
		field, ok := querylogical.FieldRef{}, false
		if leaf.Left != nil {
			field, ok = valueField(*leaf.Left)
		}
		if invalidField(field, ok) || leaf.Low == nil || leaf.High == nil || !staticValue(*leaf.Low) || !staticValue(*leaf.High) {
			return querylogical.NewError(querylogical.ErrorInvalidQuery, "plan query", "indexed between predicate must compare the start alias property with static bounds", nil)
		}
	case querylogical.LeafPropertyExists:
		if leaf.Field == nil || invalidField(*leaf.Field, true) {
			return querylogical.NewError(querylogical.ErrorInvalidQuery, "plan query", "indexed property-exists predicate must target the start alias", nil)
		}
	case querylogical.LeafString:
		field, ok := querylogical.FieldRef{}, false
		if leaf.Operand != nil {
			field, ok = valueField(*leaf.Operand)
		}
		if invalidField(field, ok) {
			return querylogical.NewError(querylogical.ErrorInvalidQuery, "plan query", "indexed string predicate must target the start alias property", nil)
		}
	case querylogical.LeafText:
		if leaf.Field != nil && leaf.Field.Alias != alias {
			return querylogical.NewError(querylogical.ErrorInvalidQuery, "plan query", "indexed text predicate must target the start alias", nil)
		}
	}
	return nil
}

func semanticPredicate(predicate *querylogical.Predicate) (querylogical.Leaf, *querylogical.Predicate, bool, error) {
	if predicate == nil {
		return querylogical.Leaf{}, nil, false, nil
	}
	leaves := []querylogical.Leaf{}
	var walk func(*querylogical.Predicate, bool) error
	walk = func(current *querylogical.Predicate, insideOR bool) error {
		if current == nil {
			return nil
		}
		if current.Op == querylogical.PredicateLeafOp && current.Leaf != nil {
			if current.Leaf.Kind == querylogical.LeafSemantic {
				if insideOR {
					return fmt.Errorf("semantic predicates inside OR are not indexed")
				}
				leaves = append(leaves, *current.Leaf)
			}
			return nil
		}
		for i := range current.Terms {
			if err := walk(&current.Terms[i], insideOR || current.Op == querylogical.PredicateOrOp); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(predicate, false); err != nil {
		return querylogical.Leaf{}, nil, true, querylogical.NewError(querylogical.ErrorUnavailableAccessPath, "plan query", err.Error(), nil)
	}
	if len(leaves) == 0 {
		return querylogical.Leaf{}, nil, false, nil
	}
	if len(leaves) != 1 {
		return querylogical.Leaf{}, nil, true, querylogical.NewError(querylogical.ErrorUnavailableAccessPath, "plan query", "semantic predicate query supports one semantic predicate", nil)
	}
	return leaves[0], removeSemantic(predicate), true, nil
}

func removeSemantic(predicate *querylogical.Predicate) *querylogical.Predicate {
	if predicate == nil {
		return nil
	}
	if predicate.Op == querylogical.PredicateLeafOp {
		if predicate.Leaf != nil && predicate.Leaf.Kind == querylogical.LeafSemantic {
			return nil
		}
		copy := *predicate
		return &copy
	}
	terms := []querylogical.Predicate{}
	for i := range predicate.Terms {
		if child := removeSemantic(&predicate.Terms[i]); child != nil {
			terms = append(terms, *child)
		}
	}
	return querylogical.CombinePredicate(predicate.Op, terms)
}

func orderedBounds(predicate *querylogical.Predicate, field querylogical.FieldRef) (*Bound, *Bound, error) {
	if predicate == nil {
		return nil, nil, nil
	}
	if predicate.Op == querylogical.PredicateLeafOp && predicate.Leaf != nil {
		scan, ok := scanForLeaf(*predicate.Leaf, field.Alias)
		operator := predicate.Leaf.Operator
		if !ok || scan.Field != field || (predicate.Leaf.Kind == querylogical.LeafComparison && operator != "<" && operator != ">=" && operator != "<=") {
			return nil, nil, fmt.Errorf("indexed ORDER BY supports only BETWEEN or less-than bounds on the ordered property")
		}
		return scan.Low, scan.High, nil
	}
	if predicate.Op != querylogical.PredicateAndOp {
		return nil, nil, fmt.Errorf("indexed ORDER BY bounds must target the ordered property")
	}
	var low, high *Bound
	for _, term := range predicate.Terms {
		if term.Op != querylogical.PredicateLeafOp || term.Leaf == nil || term.Leaf.Kind != querylogical.LeafComparison {
			return nil, nil, fmt.Errorf("indexed ORDER BY supports only BETWEEN or less-than bounds on the ordered property")
		}
		scan, ok := scanForLeaf(*term.Leaf, field.Alias)
		if !ok || scan.Field != field || (term.Leaf.Operator != ">=" && term.Leaf.Operator != "<=") {
			return nil, nil, fmt.Errorf("indexed ORDER BY bounds must target the ordered property")
		}
		if scan.Low != nil {
			if low != nil {
				return nil, nil, fmt.Errorf("indexed ORDER BY has multiple lower bounds")
			}
			low = scan.Low
		}
		if scan.High != nil {
			if high != nil {
				return nil, nil, fmt.Errorf("indexed ORDER BY has multiple upper bounds")
			}
			high = scan.High
		}
	}
	if low == nil && high == nil {
		return nil, nil, fmt.Errorf("indexed ORDER BY requires usable bounds")
	}
	return low, high, nil
}

func tagBranches(predicate *querylogical.Predicate, alias string) ([][]string, bool, error) {
	if predicate == nil {
		return nil, false, nil
	}
	if predicate.Op == querylogical.PredicateOrOp {
		branches := [][]string{}
		for i := range predicate.Terms {
			child, ok, err := tagBranches(&predicate.Terms[i], alias)
			if err != nil || !ok {
				return nil, ok, err
			}
			branches = append(branches, child...)
		}
		return branches, len(branches) > 0, nil
	}
	if predicate.Op == querylogical.PredicateAndOp {
		branch := []string{}
		for i := range predicate.Terms {
			child, ok, err := tagBranches(&predicate.Terms[i], alias)
			if err != nil || !ok || len(child) != 1 {
				return nil, ok, err
			}
			branch = append(branch, child[0]...)
		}
		return [][]string{branch}, len(branch) > 0, nil
	}
	if predicate.Op != querylogical.PredicateLeafOp || predicate.Leaf == nil || predicate.Leaf.Kind != querylogical.LeafHasTag || predicate.Leaf.Field == nil || predicate.Leaf.Field.Alias != alias {
		return nil, false, nil
	}
	normalized, err := graph.NormalizeTag(predicate.Leaf.Tag)
	if err != nil {
		return nil, true, querylogical.NewError(querylogical.ErrorInvalidQuery, "plan query", err.Error(), err)
	}
	return [][]string{{normalized}}, true, nil
}

func valueField(value querylogical.Value) (querylogical.FieldRef, bool) {
	if value.Kind != querylogical.ValueField || value.Field == nil {
		return querylogical.FieldRef{}, false
	}
	return *value.Field, true
}

func staticValue(value querylogical.Value) bool {
	switch value.Kind {
	case querylogical.ValueNull, querylogical.ValueBool, querylogical.ValueInteger, querylogical.ValueUnsigned,
		querylogical.ValueNumber, querylogical.ValueString, querylogical.ValueList, querylogical.ValueObject,
		querylogical.ValueDate, querylogical.ValueCurrentDate, querylogical.ValueParameter:
		return true
	default:
		return false
	}
}

func adjacencyIndex(step querylogical.Step) IndexUse {
	direction := "out"
	if step.Edge.Direction == querylogical.DirectionIncoming {
		direction = "in"
	}
	label := ""
	if len(step.Edge.Labels) > 0 {
		label = step.Edge.Labels[0]
	}
	return IndexUse{Name: direction + ":" + label, Target: schemamodel.IndexTargetEdge, Label: label}
}

func builtInIndex(name string, label string) IndexUse {
	return IndexUse{Name: name, Target: schemamodel.IndexTargetNode, Label: label}
}

func schemaIndexUse(index schemamodel.IndexDefinition, label string) IndexUse {
	return IndexUse{
		Name: index.Name, Kind: index.Kind, Target: index.TargetKind,
		Label: label, Namespace: index.Field.Namespace, Field: index.Field.Name,
	}
}

func dedupeIndexes(indexes []IndexUse) []IndexUse {
	byName := make(map[string]IndexUse, len(indexes))
	for _, index := range indexes {
		if strings.TrimSpace(index.Name) != "" {
			byName[index.Name] = index
		}
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]IndexUse, 0, len(names))
	for _, name := range names {
		out = append(out, byName[name])
	}
	return out
}
