package internal

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/google/uuid"
	domainspace "github.com/myceldb/mycel/domain/space"
	"github.com/myceldb/mycel/internal/semantic/backfill"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	"github.com/myceldb/mycel/internal/semantic/maintenance"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	storesemantic "github.com/myceldb/mycel/store/semantic"
)

// RunSemanticMaintenanceInput runs a bounded semantic maintenance pass for a space.
type RunSemanticMaintenanceInput struct {
	AccessToken  AccessToken
	SpaceID      domainspace.SpaceID
	AnalyzeLimit int
	ProcessLimit int
}

// RunSemanticMaintenanceResult summarizes a semantic maintenance pass.
type RunSemanticMaintenanceResult struct {
	ProcessedEvents int
	EnqueuedItems   int
	ProcessedItems  int
	CompletedItems  int
	FailedItems     int
}

// RunSemanticMaintenance performs one graph-dirty analysis pass followed by one
// pending-work processing pass for a space. It is intended for embedded apps
// that want same-process semantic indexing without shelling out to the CLI.
func (e *defaultEngine) RunSemanticMaintenance(ctx context.Context, in RunSemanticMaintenanceInput) (RunSemanticMaintenanceResult, error) {
	if err := e.Ready(ctx); err != nil {
		return RunSemanticMaintenanceResult{}, err
	}
	if !e.advancedSemanticEnabled {
		return RunSemanticMaintenanceResult{}, fmt.Errorf("advanced semantic maintenance is disabled")
	}
	if in.SpaceID == uuid.Nil {
		return RunSemanticMaintenanceResult{}, fmt.Errorf("%w: space_id is required", ErrInvalidConfig)
	}
	sess, err := e.OpenSession(ctx, OpenSessionInput{AccessToken: in.AccessToken, SpaceID: in.SpaceID})
	if err != nil {
		return RunSemanticMaintenanceResult{}, err
	}
	defer sess.Close()
	spaceMgr := storesemantic.NewSpaceManager()
	if err := spaceMgr.Init(ctx, filepath.Join(graphsDir(e.dataDir), in.SpaceID.String(), "semantic"), in.SpaceID); err != nil {
		return RunSemanticMaintenanceResult{}, err
	}
	analyze, err := (maintenance.Analyzer{SpaceManager: spaceMgr}).AnalyzeOnce(ctx, maintenance.AnalyzeInput{Limit: in.AnalyzeLimit})
	if err != nil {
		return RunSemanticMaintenanceResult{}, err
	}
	processLimit := in.ProcessLimit
	if processLimit <= 0 {
		processLimit = 10
	}
	runner := backfill.Runner{
		Session:       sess,
		GlobalManager: e.semanticManager,
		SpaceManager:  spaceMgr,
		Connector: connectors.Service{
			GlobalManager: e.semanticManager,
			Accounting:    e.accountingManager,
			SecretKeyB64:  e.userStoreEncryptionKeyB64,
		},
		VectorBackend: vectorstore.MycelFileBackend{GraphsDir: graphsDir(e.dataDir)},
	}
	processed, err := (maintenance.Worker{SpaceManager: spaceMgr, Backfill: runner}).ProcessOnce(ctx, processLimit)
	if err != nil {
		return RunSemanticMaintenanceResult{}, err
	}
	return RunSemanticMaintenanceResult{ProcessedEvents: analyze.ProcessedEvents, EnqueuedItems: analyze.EnqueuedItems, ProcessedItems: processed.Processed, CompletedItems: processed.Completed, FailedItems: processed.Failed}, nil
}
