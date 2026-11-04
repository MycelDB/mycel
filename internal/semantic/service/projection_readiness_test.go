package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daeconfig "github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProjectionReadinessDiagnosesEveryLeaseDimension(t *testing.T) {
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	expected := ProjectionReadinessTuple{LeaderNode: 2, LeaderTerm: 9, SemanticGeneration: 3, RebuildEpoch: "epoch-3", FreshThrough: 7}
	base := semanticProjectionLease{SemanticRuleID: ruleID, EmbeddingBindingKey: "search", LeaderNode: 2, LeaderTerm: 9, SemanticGeneration: 3, RebuildEpoch: "epoch-3", FreshThrough: 7}
	tests := []struct {
		name   string
		mutate func(*semanticProjectionLease)
		reason string
	}{
		{name: "leader", mutate: func(lease *semanticProjectionLease) { lease.LeaderNode = 1 }, reason: ProjectionReadinessLeaderMismatch},
		{name: "term", mutate: func(lease *semanticProjectionLease) { lease.LeaderTerm = 8 }, reason: ProjectionReadinessTermMismatch},
		{name: "generation", mutate: func(lease *semanticProjectionLease) { lease.SemanticGeneration = 2 }, reason: ProjectionReadinessGenerationMismatch},
		{name: "epoch", mutate: func(lease *semanticProjectionLease) { lease.RebuildEpoch = "epoch-2" }, reason: ProjectionReadinessEpochMismatch},
		{name: "freshness", mutate: func(lease *semanticProjectionLease) { lease.FreshThrough = 6 }, reason: ProjectionReadinessFreshnessLag},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lease := base
			test.mutate(&lease)
			err := newProjectionReadinessError([]semanticProjectionLease{lease}, ruleID, "search", 2, expected, "completed", "")
			if !reflect.DeepEqual(err.Reasons, []string{test.reason}) {
				t.Fatalf("reasons=%v, want %s", err.Reasons, test.reason)
			}
			if err.Observed == nil || *err.Observed != projectionLeaseTuple(lease) || err.Expected != expected {
				t.Fatalf("diagnostic tuple mismatch: %+v", err)
			}
			metadata := err.DiagnosticMetadata()
			if metadata["readiness_reasons"] != test.reason || metadata["lease_present"] != "true" || metadata["semantic_rule_id"] != ruleID.String() || metadata["leader_term"] != "9" {
				t.Fatalf("metadata=%v", metadata)
			}
			if !strings.Contains(err.Error(), test.reason) || !strings.Contains(err.Error(), "not ready for leader 2 term 9") {
				t.Fatalf("non-actionable error: %v", err)
			}
		})
	}
	if reasons := projectionLeaseMismatches(expected, ProjectionReadinessTuple{LeaderNode: 2, LeaderTerm: 9, SemanticGeneration: 3, RebuildEpoch: "epoch-3", FreshThrough: 8}); len(reasons) != 0 {
		t.Fatalf("advanced frontier incorrectly rejected: %v", reasons)
	}
	missing := newProjectionReadinessError([]semanticProjectionLease{{SemanticRuleID: domainsemantic.SemanticRuleID(uuid.New()), EmbeddingBindingKey: "search"}, {SemanticRuleID: ruleID, EmbeddingBindingKey: "another-binding"}}, ruleID, "search", 2, expected, "completed", ProjectionReadinessMissingBaseline)
	if !reflect.DeepEqual(missing.Reasons, []string{ProjectionReadinessMissingLease, ProjectionReadinessMissingBaseline}) || missing.Observed != nil {
		t.Fatalf("missing lease diagnostic=%+v", missing)
	}
	if metadata := missing.DiagnosticMetadata(); metadata["lease_present"] != "false" || metadata["lease_leader_node"] != "" {
		t.Fatalf("missing lease invented observed evidence: %v", metadata)
	}
}

func TestProjectionReadinessReportsAllMismatchesAndClosestLease(t *testing.T) {
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	expected := ProjectionReadinessTuple{LeaderNode: 2, LeaderTerm: 9, SemanticGeneration: 3, RebuildEpoch: "epoch-3", FreshThrough: 7}
	stale := semanticProjectionLease{SemanticRuleID: ruleID, EmbeddingBindingKey: "search", LeaderNode: 1, LeaderTerm: 8, SemanticGeneration: 2, RebuildEpoch: "epoch-2", FreshThrough: 6}
	err := newProjectionReadinessError([]semanticProjectionLease{stale}, ruleID, "search", 2, expected, "completed", "")
	want := []string{ProjectionReadinessLeaderMismatch, ProjectionReadinessTermMismatch, ProjectionReadinessGenerationMismatch, ProjectionReadinessEpochMismatch, ProjectionReadinessFreshnessLag}
	if !reflect.DeepEqual(err.Reasons, want) {
		t.Fatalf("reasons=%v, want %v", err.Reasons, want)
	}
	closest := stale
	closest.LeaderNode, closest.SemanticGeneration, closest.RebuildEpoch, closest.FreshThrough = 2, 3, "epoch-3", 7
	err = newProjectionReadinessError([]semanticProjectionLease{stale, closest}, ruleID, "search", 2, expected, "completed", "")
	if !reflect.DeepEqual(err.Reasons, []string{ProjectionReadinessTermMismatch}) || err.Observed == nil || *err.Observed != projectionLeaseTuple(closest) {
		t.Fatalf("wrong historical lease selected: %+v", err)
	}
}

func TestProjectionReadinessStatusContainsVersionedSafeDetails(t *testing.T) {
	for _, test := range []struct {
		state string
		code  codes.Code
	}{
		{state: "completed", code: codes.Unavailable},
		{state: "running", code: codes.Unavailable},
		{state: "required", code: codes.FailedPrecondition},
		{state: "failed", code: codes.FailedPrecondition},
	} {
		t.Run(test.state, func(t *testing.T) {
			err := &ProjectionReadinessError{SemanticRuleID: domainsemantic.SemanticRuleID(uuid.New()), EmbeddingBindingKey: "search", LocalNode: 2, Expected: ProjectionReadinessTuple{LeaderNode: 2, LeaderTerm: 9}, Reasons: []string{ProjectionReadinessMissingLease}, RebuildState: test.state}
			st := status.Convert(err)
			if st.Code() != test.code || len(st.Details()) != 1 {
				t.Fatalf("status=%v, want %s with ErrorInfo", st, test.code)
			}
			detail, ok := st.Details()[0].(*errdetails.ErrorInfo)
			if !ok || detail.Reason != "SEMANTIC_PROJECTION_NOT_READY" || detail.Domain != "myceldb.io" || detail.Metadata["schema_version"] != "1" || !reflect.DeepEqual(detail.Metadata, err.DiagnosticMetadata()) {
				t.Fatalf("details=%+v", st.Details())
			}
		})
	}
}

func TestProjectionReadinessMissingBaselineRemainsFencedAndDoesNotWriteLease(t *testing.T) {
	ctx := context.Background()
	module := NewModule()
	if result := module.Init(ctx, testRuntime(t, daeconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	rule := domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "missing-baseline", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true}}}
	if _, err := mgr.UpsertSemanticRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	module.raftLocalNode = 2
	module.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) { return 2, 9, true, nil }
	_, err = module.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: []domainsemantic.SemanticRuleID{ruleID}, EmbeddingBindingKey: "search"})
	var readiness *ProjectionReadinessError
	if !errors.As(err, &readiness) || !reflect.DeepEqual(readiness.Reasons, []string{ProjectionReadinessMissingLease, ProjectionReadinessMissingBaseline}) {
		t.Fatalf("error=%v, want missing lease and missing baseline diagnostics", err)
	}
	if _, statErr := os.Stat(module.projectionLeasesPath(spaceID)); !os.IsNotExist(statErr) {
		t.Fatalf("uncertified baseline wrote a lease: %v", statErr)
	}
}

func TestProjectionReadinessRejectsNonLocalLeadership(t *testing.T) {
	for _, leader := range []consensus.NodeID{0, 1} {
		t.Run(fmt.Sprint(leader), func(t *testing.T) {
			module := NewModule()
			module.raftLocalNode = 2
			module.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) { return leader, 9, true, nil }
			err := module.validateLocalProjectionForSearch(context.Background(), SearchInput{SpaceID: domainspace.SpaceID(uuid.New())})
			var readiness *ProjectionReadinessError
			if !errors.As(err, &readiness) || !reflect.DeepEqual(readiness.Reasons, []string{ProjectionReadinessNoLocalLeadership}) || status.Code(err) != codes.Unavailable {
				t.Fatalf("leader=%d readiness error=%v", leader, err)
			}
		})
	}
}

func TestProjectionReadinessReportsBlockedRebuild(t *testing.T) {
	for _, test := range []struct {
		state  string
		reason string
		code   codes.Code
	}{
		{state: "required", reason: ProjectionReadinessRebuildRequired, code: codes.FailedPrecondition},
		{state: "running", reason: ProjectionReadinessRebuildRunning, code: codes.Unavailable},
		{state: "failed", reason: ProjectionReadinessRebuildFailed, code: codes.FailedPrecondition},
	} {
		t.Run(test.state, func(t *testing.T) {
			ctx := context.Background()
			module := NewModule()
			if result := module.Init(ctx, testRuntime(t, daeconfig.SemanticMaintenanceConfig{Enabled: false})); !result.OK {
				t.Fatal(result.Error)
			}
			spaceID, domainID := domainspace.SpaceID(uuid.New()), graph.DomainID(uuid.New())
			ruleID := domainsemantic.SemanticRuleID(uuid.New())
			mgr, err := module.SpaceManager(ctx, spaceID)
			if err != nil {
				t.Fatal(err)
			}
			rule := domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "blocked-rebuild", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Enabled: true}}}
			if _, err := mgr.UpsertSemanticRule(ctx, rule); err != nil {
				t.Fatal(err)
			}
			if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 2, RebuildState: test.state}}}); err != nil {
				t.Fatal(err)
			}
			module.raftLocalNode = 2
			module.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) { return 2, 9, true, nil }
			_, err = module.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: []domainsemantic.SemanticRuleID{ruleID}, EmbeddingBindingKey: "search"})
			var readiness *ProjectionReadinessError
			if !errors.As(err, &readiness) || !reflect.DeepEqual(readiness.Reasons, []string{ProjectionReadinessMissingLease, test.reason}) || status.Code(err) != test.code {
				t.Fatalf("rebuild state=%s error=%v", test.state, err)
			}
		})
	}
}

func TestProjectionRecoveryReplaysRetainedRecordsAndFencesRepeatedTerms(t *testing.T) {
	ctx := context.Background()
	runtime := testRuntime(t, daeconfig.SemanticMaintenanceConfig{Enabled: false})
	config := Config{GraphReader: &recordingGraphReader{}, GraphRevisionReader: fixedGraphRevisionReader{revision: 5}}
	module := NewModule(config)
	if result := module.Init(ctx, runtime); !result.OK {
		t.Fatal(result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	now := time.Now().UTC()
	if _, err := module.GlobalManager().UpsertVectorStore(ctx, domainsemantic.VectorStoreBackend{ID: storeID, Key: "retained-recovery", Name: "Retained recovery", Type: domainsemantic.VectorStoreMycelFile, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	mgr, err := module.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	rule := domainsemantic.SemanticGenerationRule{ID: ruleID, SpaceID: spaceID, DomainID: domainID, Key: "retained-rule", Enabled: true, Selector: domainsemantic.SemanticTargetSelector{Mode: domainsemantic.SemanticTargetSelectorNodeType, Labels: []string{"Document"}}, Source: domainsemantic.SemanticSourceAssemblyPolicy{Mode: domainsemantic.SemanticSourceSelf}, Embeddings: []domainsemantic.SemanticEmbeddingBinding{{Key: "search", Purpose: "semantic_search", IntelligenceProfile: "profile", VectorStoreID: storeID, Enabled: true}}}
	if _, err := mgr.UpsertSemanticRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticRuleState(ctx, domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{"search": {EmbeddingBindingKey: "search", SemanticGeneration: 3, RebuildEpoch: "epoch-3", RebuildState: "completed", BaselineFreshThrough: 5}}}); err != nil {
		t.Fatal(err)
	}
	record, err := module.localVectorBackend().Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: graph.NodeID(uuid.New()), SourceHash: "sha256:retained", SourceMode: "self", SourceGraphRevision: 5, SemanticGeneration: 3, RebuildEpoch: "epoch-3", SemanticConfigurationHash: "retained-configuration", ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	key := vectorstore.SearchIndexKey{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", VectorStoreID: storeID}
	if _, err := module.localVectorBackend().RebuildSearchIndex(ctx, key, vectorstore.RebuildLimit{MaxRecords: 100, MaxBytes: 1 << 20, SemanticGeneration: 3, RebuildEpoch: "epoch-3"}); err != nil {
		t.Fatal(err)
	}
	module.raftLocalNode = 1
	module.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) { return 1, 7, true, nil }
	bound, err := module.bindSemanticOperationLeadership(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := module.saveProjectionLease(bound, spaceID, ruleID, "search", 3, "epoch-3", 5); err != nil {
		t.Fatal(err)
	}
	// A new subsystem instance reopens the retained rule, reliability state,
	// vector records, physical index, and prior-term lease from the same disk.
	if err := module.Close(); err != nil {
		t.Fatal(err)
	}
	restartRuntime := testRuntime(t, daeconfig.SemanticMaintenanceConfig{Enabled: false})
	restartRuntime.Config.DataDir = runtime.Config.DataDir
	restarted := NewModule(config)
	if result := restarted.Init(ctx, restartRuntime); !result.OK {
		t.Fatal(result.Error)
	}
	restarted.raftLocalNode = 1
	var term atomic.Uint64
	term.Store(8)
	restarted.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) { return 1, term.Load(), true, nil }
	search := SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleIDs: []domainsemantic.SemanticRuleID{ruleID}, EmbeddingBindingKey: "search"}
	for current := uint64(8); current <= 10; current++ {
		staleContext, err := restarted.bindSemanticOperationLeadership(ctx, spaceID)
		if err != nil {
			t.Fatal(err)
		}
		term.Store(current)
		if current > 8 {
			err := restarted.validateLocalProjectionForSearch(staleContext, search)
			if err == nil || !strings.Contains(err.Error(), "leadership changed") {
				t.Fatalf("stale operation in term %d was not fenced: %v", current, err)
			}
			leases, loadErr := restarted.loadProjectionLeasesLocked(spaceID)
			if loadErr != nil || len(leases.Entries) != 1 || leases.Entries[0].LeaderTerm != current-1 {
				t.Fatalf("stale operation certified a new term: leases=%+v err=%v", leases, loadErr)
			}
		}
		bound, err = restarted.bindSemanticOperationLeadership(ctx, spaceID)
		if err != nil {
			t.Fatal(err)
		}
		if err := restarted.validateLocalProjectionForSearch(bound, search); err != nil {
			t.Fatalf("retained baseline did not recertify for term %d: %v", current, err)
		}
		leases, err := restarted.loadProjectionLeasesLocked(spaceID)
		if err != nil || len(leases.Entries) != 1 || leases.Entries[0].LeaderTerm != current || leases.Entries[0].SemanticGeneration != 3 || leases.Entries[0].FreshThrough != 5 {
			t.Fatalf("term %d lease=%+v err=%v", current, leases, err)
		}
	}
	// Promotion must use independently current local record evidence, not the
	// preceding node's lease. A missing baseline is covered by the test above.
	restarted.raftLocalNode = 2
	restarted.semanticLeadershipProbe = func(domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) { return 2, 11, true, nil }
	bound, err = restarted.bindSemanticOperationLeadership(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.validateLocalProjectionForSearch(bound, search); err != nil {
		t.Fatalf("promotion with a retained current local baseline failed: %v", err)
	}
	leases, err := restarted.loadProjectionLeasesLocked(spaceID)
	if err != nil || len(leases.Entries) != 1 || leases.Entries[0].LeaderNode != 2 || leases.Entries[0].LeaderTerm != 11 {
		t.Fatalf("promoted lease=%+v err=%v", leases, err)
	}
	records, err := restarted.localVectorBackend().ListRecords(ctx, spaceID, domainsemantic.SemanticIndexID(ruleID))
	if err != nil || len(records) != 1 || records[0].ID != record.ID || records[0].SourceGraphRevision != 5 || !reflect.DeepEqual(records[0].Vector, record.Vector) {
		t.Fatalf("retained record changed during recovery: records=%+v err=%v", records, err)
	}
}
