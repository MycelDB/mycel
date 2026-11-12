package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	"github.com/myceldb/mycel/internal/projection"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

func (m *Module) triggerBackup(ctx context.Context, input backupcore.TriggerInput) (backupcore.TriggerResult, error) {
	return m.manager.Trigger(ctx, input)
}

func (m *Module) projectionBackupCatalog(ctx context.Context) (projection.BackupCatalog, error) {
	if m.projectionRegistries == nil {
		return projection.BackupCatalog{}, nil
	}
	catalog := m.projectionRegistries.BackupCatalog()
	if m.projectionArtifacts != nil {
		artifacts, err := m.projectionArtifacts(ctx)
		if err != nil {
			return projection.BackupCatalog{}, fmt.Errorf("collect projection artifact manifests: %w", err)
		}
		for _, artifact := range artifacts {
			spaceID, err := uuid.Parse(artifact.Scope.SpaceID)
			if err != nil {
				return projection.BackupCatalog{}, fmt.Errorf("projection artifact scope is invalid")
			}
			partition, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(spaceID), catalog.PartitionCount)
			if err != nil {
				return projection.BackupCatalog{}, err
			}
			if err := catalog.AddArtifact(partition.Uint32(), artifact); err != nil {
				return projection.BackupCatalog{}, err
			}
		}
	}
	if err := catalog.Validate(); err != nil {
		return projection.BackupCatalog{}, err
	}
	return catalog, nil
}

func (m *Module) prepareBackupSnapshot(ctx context.Context) error {
	if m.wal == nil || m.progress == nil || m.checkpoint == nil {
		return nil
	}
	cp, err := wal.CreateCheckpoint(ctx, m.progress, m.checkpoint, 0)
	if err != nil {
		return err
	}
	if cp.LSN > 0 {
		if err := m.wal.RetainFrom(ctx, cp.LSN+1); err != nil {
			return err
		}
	}
	return nil
}
