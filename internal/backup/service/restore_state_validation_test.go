package service

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	automationservice "github.com/myceldb/mycel/internal/automation/service"
	automationstorage "github.com/myceldb/mycel/internal/automation/storage"
	backupcore "github.com/myceldb/mycel/internal/backup"
	graph "github.com/myceldb/mycel/internal/graph/model"
	inference "github.com/myceldb/mycel/internal/inference/model"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	lexicalanalyzer "github.com/myceldb/mycel/internal/search/lexical/analyzer"
	lexicalindex "github.com/myceldb/mycel/internal/search/lexical/index"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	semanticvectorstore "github.com/myceldb/mycel/internal/semantic/vectorstore"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestBackupRestoreValidatesSchemaAutomationAndDerivedState(t *testing.T) {
	ctx := context.Background()
	sourceDataDir := t.TempDir()
	source := runtimetest.New(sourceDataDir, nil)
	domainID := graph.DomainID(uuid.New())
	spaceID := domainspace.SpaceID(uuid.New())
	nodeID := graph.NodeID(uuid.New())

	sourceSchema := schemaservice.NewModule("")
	sourceAutomation := automationservice.NewModule(filepath.Join(sourceDataDir, "automation"))
	sourceLexical := lexicalservice.NewModule()
	sourceSemantic := semanticservice.NewModule()
	if err := source.InitServices(ctx, []runtimetest.Service{sourceSchema, sourceAutomation, sourceLexical, sourceSemantic}); err != nil {
		t.Fatalf("init source services: %v", err)
	}

	seedBackupRestoreFixture(t, ctx, sourceDataDir, sourceSchema, sourceAutomation, sourceLexical, sourceSemantic, spaceID, domainID, nodeID)

	backupDir := t.TempDir()
	mgr := backupcore.NewManager(backupcore.ManagerConfig{
		DataDir: sourceDataDir,
		Policy: backupcore.Policy{
			BackupDir:              backupDir,
			ArchiveFormat:          backupcore.ArchiveFormatTar,
			Interval:               time.Hour,
			RetentionCount:         2,
			QuiesceDrainTimeout:    time.Second,
			BackupTimeout:          time.Minute,
			RetryAfter:             time.Second,
			StatusHistoryLimit:     2,
			ScheduleKind:           backupcore.ScheduleKindInterval,
			AllowReadsDuringBackup: true,
		},
		Quiesce: source.QuiesceCoordinator(),
	})
	result, err := mgr.Trigger(ctx, backupcore.TriggerInput{Source: "restore-state-test", Reason: "validate durable writer restore state"})
	if err != nil {
		t.Fatalf("Trigger() error=%v", err)
	}

	restoreDataDir := t.TempDir()
	extractTarArchive(t, result.ArchivePath, restoreDataDir)

	restored := runtimetest.New(restoreDataDir, nil)
	restoredSchema := schemaservice.NewModule("")
	restoredAutomation := automationservice.NewModule(filepath.Join(restoreDataDir, "automation"))
	restoredLexical := lexicalservice.NewModule()
	restoredSemantic := semanticservice.NewModule()
	if err := restored.InitServices(ctx, []runtimetest.Service{restoredSchema, restoredAutomation, restoredLexical, restoredSemantic}); err != nil {
		t.Fatalf("init restored services: %v", err)
	}

	validateRestoredSchemaAndAutomation(t, ctx, restoreDataDir, restoredSchema, restoredAutomation, spaceID, domainID)
	validateRestoredInferenceBudgetState(t, ctx, restoreDataDir, spaceID)
	validateRestoredLexicalDerivedState(t, ctx, restoredLexical, spaceID, domainID, nodeID)
	validateRestoredSemanticDerivedState(t, ctx, restoredSemantic, spaceID, domainID, nodeID)
}

func seedBackupRestoreFixture(t *testing.T, ctx context.Context, dataDir string, schemaMod *schemaservice.Module, automationMod *automationservice.Module, lexicalMod *lexicalservice.Module, semanticMod *semanticservice.Module, spaceID domainspace.SpaceID, domainID graph.DomainID, nodeID graph.NodeID) {
	t.Helper()
	if err := schemaMod.PutDomainSchema(ctx, schemamodel.DomainSchema{
		DomainID: domainID,
		Mode:     schemamodel.SchemaModeStrict,
		NodeTypes: []schemamodel.NodeType{{
			Name:   "Page",
			Labels: []string{"Page"},
			Properties: []schemamodel.FieldSpec{{
				Name: "title",
				Type: schemamodel.FieldTypeString,
			}},
		}},
	}); err != nil {
		t.Fatalf("seed schema: %v", err)
	}

	procedure := automation.Procedure{ID: "backup-restore-procedure", Version: 1, DomainID: domainID, Status: automation.StatusEnabled, Input: automation.Input{Target: "changed", Fields: []string{"properties.title"}}, Workflow: &automation.Workflow{Steps: []automation.WorkflowStep{{ID: "echo", Kind: automation.WorkflowStepTool, Tool: "debug.echo"}}}}
	if _, err := automationMod.CreateProcedureAs(ctx, domainID, mustJSON(t, procedure), "operator"); err != nil {
		t.Fatalf("seed automation procedure: %v", err)
	}
	binding := automation.Binding{ID: "backup-restore-binding", Version: 1, DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: procedure.Version, Status: automation.StatusEnabled, Scope: automation.BindingScope{SpaceID: spaceID.String(), DomainID: domainID}, Trigger: automation.BindingTrigger{Type: automation.TriggerTypeSchedule, Schedule: &automation.ScheduleTrigger{Interval: "1h"}}, Runtime: automation.RuntimeContext{ActorPrincipalID: "automation", OwnerPrincipalID: "operator", OnBehalfOfPrincipalID: "operator"}}
	if _, err := automationMod.CreateBindingAs(ctx, domainID, mustJSON(t, binding), "operator"); err != nil {
		t.Fatalf("seed automation binding: %v", err)
	}
	attempt := automation.ProcedureInferenceAttempt{ID: "backup-attempt", RecordVersion: 1, SpaceID: spaceID.String(), DomainID: domainID, ProcedureID: procedure.ID, ProcedureVersion: 1, BindingID: binding.ID, BindingVersion: 1, InvocationID: "backup-invocation", RunID: "backup-run", RunAttemptNumber: 1, ProviderAttemptNumber: 1, State: automation.InferenceAttemptStateRequested, RequestedAt: time.Unix(1_700_000_000, 0).UTC()}
	change := automation.ProcedureInferenceAttemptChange{EventID: "backup-change", SchemaVersion: automation.InferenceAttemptChangeSchemaV1, SpaceID: spaceID.String(), Sequence: 7, ChangeKind: attempt.State, AttemptID: attempt.ID, RecordVersion: attempt.RecordVersion, Attempt: attempt, OccurredAt: attempt.RequestedAt, CommittedAt: attempt.RequestedAt}
	ledger := automation.ProcedureInferenceLedger{SpaceID: spaceID.String(), HighWaterSequence: 7, EarliestRetainedSequence: 7, EarliestRetainedStateSequence: 7, Attempts: []automation.ProcedureInferenceAttempt{attempt}, Changes: []automation.ProcedureInferenceAttemptChange{change}}
	if err := automationstorage.NewFileStore(filepath.Join(dataDir, "automation")).PutProcedureInferenceLedger(ctx, ledger); err != nil {
		t.Fatalf("seed procedure inference ledger: %v", err)
	}

	seedInferenceBudgetLedger(t, ctx, dataDir, spaceID)

	if err := lexicalMod.Rebuild(ctx, spaceID.String(), domainID.String(), []lexicalindex.IndexedDocument{{Document: lexicalanalyzer.Document{NodeID: nodeID.String(), DomainID: domainID.String(), Fields: []lexicalanalyzer.Field{{Path: "properties.title", Text: "restore validation unique phrase"}}}, GraphRevision: 7}}, 7); err != nil {
		t.Fatalf("seed lexical index: %v", err)
	}

	global := semanticMod.GlobalManager()
	stores, err := global.ListVectorStores(ctx)
	if err != nil {
		t.Fatalf("list semantic vector stores: %v", err)
	}
	if len(stores) == 0 {
		t.Fatal("semantic module did not create a default vector store")
	}
	modelEndpointID := uuid.New()
	modelID := uuid.New()
	spaceMgr, err := semanticMod.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("open semantic space manager: %v", err)
	}
	idx, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domainID, Key: "backup-restore-index", Name: "Backup Restore Index", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: modelEndpointID, ModelID: modelID, VectorStoreID: stores[0].ID, Enabled: true})
	if err != nil {
		t.Fatalf("seed semantic index: %v", err)
	}
	backend := semanticvectorstore.MycelFileBackend{GraphsDir: filepath.Join(dataDir, "graphs")}
	if _, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: idx.ID, NodeID: nodeID, SourceHash: "sha256:restore-validation", SourceMode: string(domainsemantic.SourceExtractionSelf), ModelEndpointID: modelEndpointID, ModelID: modelID, VectorStoreID: idx.VectorStoreID, VectorSpaceKey: "restore-test/3", Dimensions: 3, Vector: []float64{1, 0, 0}}); err != nil {
		t.Fatalf("seed semantic vector record: %v", err)
	}
}

func validateRestoredSchemaAndAutomation(t *testing.T, ctx context.Context, dataDir string, schemaMod *schemaservice.Module, automationMod *automationservice.Module, spaceID domainspace.SpaceID, domainID graph.DomainID) {
	t.Helper()
	restoredSchema, err := schemaMod.GetDomainSchema(ctx, domainID)
	if err != nil {
		t.Fatalf("restored schema missing: %v", err)
	}
	if restoredSchema.Mode != schemamodel.SchemaModeStrict || len(restoredSchema.NodeTypes) != 1 || restoredSchema.NodeTypes[0].Name != "Page" {
		t.Fatalf("unexpected restored schema: %+v", restoredSchema)
	}
	procedures, err := automationMod.ListProcedures(ctx, domainID, "")
	if err != nil {
		t.Fatalf("list restored procedures: %v", err)
	}
	if len(procedures) != 1 || procedures[0].ID != "backup-restore-procedure" {
		t.Fatalf("unexpected restored procedures: %+v", procedures)
	}
	bindings, err := automationMod.ListBindings(ctx, domainID, "")
	if err != nil {
		t.Fatalf("list restored bindings: %v", err)
	}
	if len(bindings) != 1 || bindings[0].ID != "backup-restore-binding" || bindings[0].Trigger.Schedule == nil {
		t.Fatalf("unexpected restored bindings: %+v", bindings)
	}
	ledger, err := automationstorage.NewFileStore(filepath.Join(dataDir, "automation")).GetProcedureInferenceLedger(ctx, spaceID.String())
	if err != nil {
		t.Fatalf("restored procedure inference ledger missing: %v", err)
	}
	if ledger.HighWaterSequence != 7 || ledger.EarliestRetainedSequence != 7 || ledger.EarliestRetainedStateSequence != 7 || len(ledger.Attempts) != 1 || len(ledger.Changes) != 1 || ledger.Attempts[0].ID != "backup-attempt" {
		t.Fatalf("unexpected restored procedure inference ledger: %+v", ledger)
	}
}

func seedInferenceBudgetLedger(t *testing.T, ctx context.Context, dataDir string, spaceID domainspace.SpaceID) {
	t.Helper()
	ledger := inferencestorage.NewUsageLedger()
	if err := ledger.Init(ctx, filepath.Join(dataDir, "meta", "inference_runtime", "accounting")); err != nil {
		t.Fatalf("init inference budget ledger: %v", err)
	}
	now := time.Unix(1_700_000_100, 0).UTC()
	attempt := inference.BudgetAttempt{ID: "backup-budget-attempt", RequestID: "backup-budget-request", State: inference.BudgetAttemptSucceeded, BatchItems: 1, ReservedInputTokens: 8, ChargedInputTokens: 5, ReservedProviderMillis: 1_000, ChargedProviderMillis: 250, ReservedCostMicros: 8, ChargedCostMicros: 5, UsageKnown: true, OutcomeKnown: true, ProviderDurationKnown: true, ReservedAt: now, ReservedUntil: now.Add(time.Minute), CompletedAt: now.Add(250 * time.Millisecond), Outcome: "succeeded"}
	scope := inference.BudgetScope{ID: "backup-budget-scope", ScopeSequence: 9, SpaceID: spaceID.String(), SemanticRuleID: "backup-semantic-rule", EmbeddingBindingKey: "backup-embedding-binding", CorrelationID: "backup-budget-correlation", State: inference.BudgetScopeCompleted, Route: inference.BudgetRoute{EndpointID: inference.EndpointID(uuid.New()), ModelID: inference.ModelID(uuid.New()), CapabilityID: inference.CapabilityID(uuid.New()), Operation: inference.OperationEmbeddings}, Limits: inference.BudgetLimits{MaxRequests: 2, MaxAttempts: 3, MaxRetryAttempts: 1, MaxRetriesPerRequest: 1, MaxConcurrentProviderCalls: 1, BatchingMode: inference.BatchingModeSingle, MaxBatchItems: 1, MaxInputTokensPerRequest: 8, MaxInputTokens: 16, MaxProviderMillisPerRequest: 1_000, MaxProviderMillis: 2_000, Deadline: now.Add(time.Hour), Currency: "USD", PriceDefinitionVersion: "backup-price-v1", InputMicrosPerMillionTokens: 1_000_000, MaxEstimatedCostMicros: 16}, Usage: inference.BudgetUsage{Requests: 1, Attempts: 1, BatchItems: 1, InputTokens: 5, ProviderMillis: 250, EstimatedCostMicros: 5}, Attempts: []inference.BudgetAttempt{attempt}, AttemptEvents: []inference.BudgetAttemptEvent{{Sequence: 11, AttemptVersion: 1, Attempt: inference.BudgetAttempt{ID: attempt.ID, RequestID: attempt.RequestID, State: inference.BudgetAttemptReserved, BatchItems: 1, ReservedInputTokens: 8, ReservedProviderMillis: 1_000, ReservedCostMicros: 8, ReservedAt: now, ReservedUntil: now.Add(time.Minute)}, RecordedAt: now}, {Sequence: 12, AttemptVersion: 2, Attempt: attempt, RecordedAt: attempt.CompletedAt}}, CreatedAt: now, UpdatedAt: attempt.CompletedAt, ExpiresAt: now.Add(time.Hour), RetentionUntil: now.Add(2 * time.Hour)}
	if _, err := ledger.UpsertBudgetScope(ctx, scope); err != nil {
		t.Fatalf("seed inference budget scope: %v", err)
	}
}

func validateRestoredInferenceBudgetState(t *testing.T, ctx context.Context, dataDir string, spaceID domainspace.SpaceID) {
	t.Helper()
	ledger := inferencestorage.NewUsageLedger()
	if err := ledger.Init(ctx, filepath.Join(dataDir, "meta", "inference_runtime", "accounting")); err != nil {
		t.Fatalf("init restored inference budget ledger: %v", err)
	}
	scope, err := ledger.GetBudgetScope(ctx, "backup-budget-scope")
	if err != nil {
		t.Fatalf("restored inference budget scope missing: %v", err)
	}
	highWater, err := ledger.BudgetScopeHighWater(ctx)
	if err != nil {
		t.Fatalf("restored inference budget high water: %v", err)
	}
	if scope.ScopeSequence != 9 || highWater != 9 || scope.SpaceID != spaceID.String() || scope.State != inference.BudgetScopeCompleted || scope.Usage.Attempts != 1 || len(scope.Attempts) != 1 || len(scope.AttemptEvents) != 2 || scope.AttemptEvents[0].Sequence != 11 || scope.AttemptEvents[1].Sequence != 12 || scope.AttemptEvents[1].AttemptVersion != 2 {
		t.Fatalf("unexpected restored inference budget scope/high-water: scope=%+v highWater=%d", scope, highWater)
	}
}

func validateRestoredLexicalDerivedState(t *testing.T, ctx context.Context, lexicalMod *lexicalservice.Module, spaceID domainspace.SpaceID, domainID graph.DomainID, nodeID graph.NodeID) {
	t.Helper()
	status := lexicalMod.Status(ctx, spaceID.String(), domainID.String(), 7)
	if status.State != lexicalservice.StateFresh || status.LiveDocumentCount != 1 || status.IndexedGraphRevision != 7 {
		t.Fatalf("unexpected restored lexical status: %+v", status)
	}
	res, err := lexicalMod.Search(ctx, spaceID.String(), domainID.String(), "unique", lexicalindex.SearchOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("restored lexical search failed: %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].NodeID != nodeID.String() {
		t.Fatalf("unexpected restored lexical results: %+v", res.Results)
	}
}

func validateRestoredSemanticDerivedState(t *testing.T, ctx context.Context, semanticMod *semanticservice.Module, spaceID domainspace.SpaceID, domainID graph.DomainID, nodeID graph.NodeID) {
	t.Helper()
	spaceMgr, err := semanticMod.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("open restored semantic space manager: %v", err)
	}
	indexes, err := spaceMgr.ListSemanticIndexes(ctx)
	if err != nil {
		t.Fatalf("list restored semantic indexes: %v", err)
	}
	if len(indexes) != 1 || indexes[0].Key != "backup-restore-index" || indexes[0].DomainID != domainID {
		t.Fatalf("unexpected restored semantic indexes: %+v", indexes)
	}
	records, err := semanticMod.ListVectorRecords(ctx, spaceID, indexes[0].ID)
	if err != nil {
		t.Fatalf("list restored semantic vector records: %v", err)
	}
	if len(records) != 1 || records[0].NodeID != nodeID || records[0].SourceHash != "sha256:restore-validation" {
		t.Fatalf("unexpected restored semantic vector records: %+v", records)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func extractTarArchive(t *testing.T, archivePath string, targetDir string) {
	t.Helper()
	file, err := os.Open(archivePath)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	defer file.Close()
	tr := tar.NewReader(file)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Fatalf("read archive: %v", err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		dest, err := safeTarEntryPathForTest(targetDir, header.Name)
		if err != nil {
			t.Fatalf("archive path escapes target dir: %s", header.Name)
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			t.Fatalf("create restore dir: %v", err)
		}
		out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatalf("create restored file: %v", err)
		}
		_, copyErr := io.Copy(out, tr)
		closeErr := out.Close()
		if copyErr != nil {
			t.Fatalf("restore file: %v", copyErr)
		}
		if closeErr != nil {
			t.Fatalf("close restored file: %v", closeErr)
		}
	}
}

func safeTarEntryPathForTest(root string, entryName string) (string, error) {
	cleanName := filepath.Clean(entryName)
	if cleanName == "." || !filepath.IsLocal(cleanName) {
		return "", fmt.Errorf("unsafe archive path: %s", entryName)
	}
	dest := filepath.Join(root, cleanName)
	if !pathWithinDirForTest(root, dest) {
		return "", fmt.Errorf("archive path escapes target dir: %s", entryName)
	}
	return dest, nil
}

func pathWithinDirForTest(root string, path string) bool {
	root = filepath.Clean(root)
	path = filepath.Clean(path)
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != "." && rel != "" && !strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel)
}
