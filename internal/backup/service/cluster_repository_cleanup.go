package service

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (m *Module) cleanupIncompleteRepositoryBackupSet(ctx context.Context, input StartClusterBackupInput, backupSetID string) (int, error) {
	repositoryID := strings.TrimSpace(input.RepositoryID)
	if repositoryID == "" {
		return 0, nil
	}
	repo, err := m.GetBackupRepository(repositoryID)
	if err != nil {
		return 0, err
	}
	store, err := m.repositoryObjectStore(ctx, repo)
	if err != nil {
		return 0, err
	}
	prefix := strings.TrimSpace(input.RepositoryPrefix)
	if prefix == "" {
		prefix = repositoryBackupSetPrefix(repo, backupSetID)
	}
	prefix = cleanRepositoryObjectKey(prefix)
	if prefix == "" {
		return 0, fmt.Errorf("repository backup-set prefix is required")
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return store.DeletePrefix(cleanupCtx, prefix)
}

func (m *Module) cleanupRepositoryPrefixBestEffort(ctx context.Context, store backupRepositoryObjectStore, prefix string) {
	prefix = cleanRepositoryObjectKey(prefix)
	if store == nil || prefix == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	deleted, err := store.DeletePrefix(cleanupCtx, prefix)
	if err != nil {
		if m.logger != nil {
			m.logger.Warn("failed to clean incomplete repository prefix", "prefix", prefix, "error", err)
		}
		return
	}
	if deleted > 0 && m.logger != nil {
		m.logger.Info("cleaned incomplete repository prefix", "prefix", prefix, "deleted_objects", deleted)
	}
}
