package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	blobmodel "github.com/myceldb/mycel/internal/blob/model"
	usageservice "github.com/myceldb/mycel/internal/usage/service"
	"github.com/myceldb/mycel/internal/wal"
)

func (m *Module) applyBlobUsagePutDelta(ctx context.Context, old blobmodel.BlobMeta, hadOld bool, meta blobmodel.BlobMeta, mutationID string) error {
	if m.usageAccounting == nil {
		return nil
	}
	delta, err := m.blobUsagePutDelta(ctx, old, hadOld, meta, mutationID)
	if err != nil {
		return err
	}
	if len(delta.Metrics) == 0 {
		return nil
	}
	return m.usageAccounting.ApplyDelta(ctx, delta)
}

func (m *Module) reserveBlobUsagePutAdmission(ctx context.Context, meta blobmodel.BlobMeta, mutationID string) (func(), error) {
	if m.usageAccounting == nil {
		return func() {}, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	metas, err := m.loadSpaceMetaLocked(meta.SpaceID)
	if err != nil {
		return nil, err
	}
	old, hadOld := metas[blobMetaStorageKey(meta)]
	return m.reserveBlobUsagePutAdmissionLocked(ctx, old, hadOld, meta, mutationID)
}

func (m *Module) reserveBlobUsagePutAdmissionLocked(ctx context.Context, old blobmodel.BlobMeta, hadOld bool, meta blobmodel.BlobMeta, mutationID string) (func(), error) {
	if m.usageAccounting == nil {
		return func() {}, nil
	}
	delta, err := m.blobUsagePutDelta(ctx, old, hadOld, meta, mutationID)
	if err != nil {
		return nil, err
	}
	if len(delta.Metrics) == 0 {
		return func() {}, nil
	}
	return m.usageAccounting.ReserveDeltaForAdmission(ctx, delta)
}

func (m *Module) blobUsagePutDelta(ctx context.Context, old blobmodel.BlobMeta, hadOld bool, meta blobmodel.BlobMeta, mutationID string) (usageservice.Delta, error) {
	metrics, err := blobUsagePutDelta(old, hadOld, meta)
	if err != nil {
		return usageservice.Delta{}, err
	}
	return usageservice.Delta{MutationID: firstNonEmpty(strings.TrimSpace(mutationID), blobUsageLocalMutationID("put", meta)), Source: blobUsageSource(mutationID), Revision: blobUsageRevision(mutationID), Scope: blobUsageScope(meta), DomainClass: m.blobUsageDomainClass(ctx, meta.SpaceID, meta.DomainID), Metrics: metrics}, nil
}

func (m *Module) applyBlobUsageDeleteDelta(ctx context.Context, meta blobmodel.BlobMeta, mutationID string) error {
	if m.usageAccounting == nil {
		return nil
	}
	metrics, err := blobUsageDeleteDelta(meta)
	if err != nil {
		return err
	}
	if len(metrics) == 0 {
		return nil
	}
	return m.usageAccounting.ApplyDelta(ctx, usageservice.Delta{MutationID: firstNonEmpty(strings.TrimSpace(mutationID), blobUsageLocalMutationID("delete", meta)), Source: blobUsageSource(mutationID), Revision: blobUsageRevision(mutationID), Scope: blobUsageScope(meta), DomainClass: m.blobUsageDomainClass(ctx, meta.SpaceID, meta.DomainID), Metrics: metrics})
}

func blobUsagePutDelta(old blobmodel.BlobMeta, hadOld bool, meta blobmodel.BlobMeta) ([]usageservice.MetricDelta, error) {
	current, err := blobUsageMetricValues(meta)
	if err != nil {
		return nil, err
	}
	previous := map[string]int64{}
	if hadOld {
		previous, err = blobUsageMetricValues(old)
		if err != nil {
			return nil, err
		}
	}
	return blobUsageMetricDifference(previous, current), nil
}

func blobUsageDeleteDelta(meta blobmodel.BlobMeta) ([]usageservice.MetricDelta, error) {
	current, err := blobUsageMetricValues(meta)
	if err != nil {
		return nil, err
	}
	return blobUsageMetricDifference(current, map[string]int64{}), nil
}

func (m *Module) ReconcileUsage(ctx context.Context, req usageservice.ReconciliationRequest) (usageservice.ReconciliationResult, error) {
	if req.MaxRecords == 0 {
		req.MaxRecords = 100000
	}
	result := usageservice.ReconciliationResult{Source: "blob", Evidence: map[string]string{"max_records": strconv.FormatUint(req.MaxRecords, 10)}}
	scope := req.Scope
	if err := scope.Validate(); err != nil {
		return result, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	result.Deltas = append(result.Deltas, blobUsageZeroDeltas(ctx, m, scope)...)
	m.mu.Lock()
	defer m.mu.Unlock()
	if strings.TrimSpace(m.metaDir) == "" {
		return result, nil
	}
	spaceIDs, err := m.blobUsageReconciliationSpaceIDsLocked(scope)
	if err != nil {
		return result, err
	}
	for _, spaceID := range spaceIDs {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		spaceMetas, err := m.loadSpaceMetaLocked(spaceID)
		if err != nil {
			return result, err
		}
		if len(spaceMetas) > 0 {
			result.ScopesScanned++
		}
		keys := make([]string, 0, len(spaceMetas))
		for key := range spaceMetas {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			meta := spaceMetas[key]
			if !blobUsageMetaInReconciliationScope(meta, scope) {
				continue
			}
			values, err := blobUsageMetricValues(meta)
			if err != nil {
				return result, err
			}
			result.RecordsScanned++
			if result.RecordsScanned > req.MaxRecords {
				result.Truncated = true
				return result, nil
			}
			result.Deltas = append(result.Deltas, usageservice.Delta{Scope: blobUsageScope(meta), DomainClass: m.blobUsageDomainClass(ctx, meta.SpaceID, meta.DomainID), Metrics: blobUsageMetricValueDeltas(values)})
		}
	}
	return result, nil
}

func (m *Module) blobUsageReconciliationSpaceIDsLocked(scope usageservice.Scope) ([]string, error) {
	switch scope.Type {
	case usageservice.ScopeTypeDomain, usageservice.ScopeTypeSpace:
		return []string{strings.TrimSpace(scope.SpaceID)}, nil
	default:
		entries, err := os.ReadDir(m.metaDir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, err
		}
		spaceIDs := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			spaceIDs = append(spaceIDs, strings.TrimSuffix(entry.Name(), ".json"))
		}
		sort.Strings(spaceIDs)
		return spaceIDs, nil
	}
}

func blobUsageMetaInReconciliationScope(meta blobmodel.BlobMeta, scope usageservice.Scope) bool {
	if strings.TrimSpace(meta.SpaceID) != strings.TrimSpace(scope.SpaceID) {
		return false
	}
	switch scope.Type {
	case usageservice.ScopeTypeSpace:
		return true
	case usageservice.ScopeTypeDomain:
		return strings.TrimSpace(meta.DomainID) == strings.TrimSpace(scope.DomainID)
	default:
		return false
	}
}

func blobUsageZeroDeltas(ctx context.Context, m *Module, scope usageservice.Scope) []usageservice.Delta {
	switch scope.Type {
	case usageservice.ScopeTypeDomain:
		return []usageservice.Delta{{Scope: usageservice.DomainScope(scope.SpaceID, scope.DomainID), DomainClass: m.blobUsageDomainClass(ctx, scope.SpaceID, scope.DomainID), Metrics: blobUsageMetricValueDeltas(map[string]int64{})}}
	case usageservice.ScopeTypeSpace:
		return []usageservice.Delta{
			{Scope: usageservice.SpaceScope(scope.SpaceID), DomainClass: usageservice.DomainClassRetainedNonSystem, Metrics: blobUsageMetricValueDeltas(map[string]int64{})},
			{Scope: usageservice.SpaceScope(scope.SpaceID), DomainClass: usageservice.DomainClassSystem, Metrics: blobUsageMetricValueDeltas(map[string]int64{})},
		}
	default:
		return nil
	}
}

func blobUsageMetricValueDeltas(values map[string]int64) []usageservice.MetricDelta {
	metrics := blobUsageMetricNames()
	deltas := make([]usageservice.MetricDelta, 0, len(metrics))
	for _, name := range metrics {
		deltas = append(deltas, usageservice.MetricDelta{Name: name, Version: 1, Delta: values[name]})
	}
	return deltas
}

func blobUsageMetricDifference(previous map[string]int64, current map[string]int64) []usageservice.MetricDelta {
	metrics := blobUsageMetricNames()
	deltas := make([]usageservice.MetricDelta, 0, len(metrics))
	for _, name := range metrics {
		delta := current[name] - previous[name]
		if delta == 0 {
			continue
		}
		deltas = append(deltas, usageservice.MetricDelta{Name: name, Version: 1, Delta: delta})
	}
	return deltas
}

func blobUsageMetricNames() []string {
	return []string{
		usageservice.MetricBlobActiveLogicalBytes,
		usageservice.MetricBlobActiveCount,
		usageservice.MetricBlobLocalActiveLogicalBytes,
		usageservice.MetricBlobLocalActiveCount,
		usageservice.MetricBlobObjectActiveLogicalBytes,
		usageservice.MetricBlobObjectActiveCount,
	}
}

func blobUsageMetricValues(meta blobmodel.BlobMeta) (map[string]int64, error) {
	size := meta.SizeBytes
	desc := descriptorFromMeta(meta)
	if size == 0 && desc.SizeBytes != 0 {
		size = desc.SizeBytes
	}
	if size < 0 {
		return nil, fmt.Errorf("blob %s has negative size %d", meta.BlobID, size)
	}
	values := map[string]int64{
		usageservice.MetricBlobActiveLogicalBytes: size,
		usageservice.MetricBlobActiveCount:        1,
	}
	if isObjectStoreBackend(payloadBackend(desc)) {
		values[usageservice.MetricBlobObjectActiveLogicalBytes] = size
		values[usageservice.MetricBlobObjectActiveCount] = 1
		values[usageservice.MetricBlobLocalActiveLogicalBytes] = 0
		values[usageservice.MetricBlobLocalActiveCount] = 0
	} else {
		values[usageservice.MetricBlobLocalActiveLogicalBytes] = size
		values[usageservice.MetricBlobLocalActiveCount] = 1
		values[usageservice.MetricBlobObjectActiveLogicalBytes] = 0
		values[usageservice.MetricBlobObjectActiveCount] = 0
	}
	return values, nil
}

func blobUsageScope(meta blobmodel.BlobMeta) usageservice.Scope {
	if strings.TrimSpace(meta.DomainID) == "" {
		return usageservice.SpaceScope(meta.SpaceID)
	}
	return usageservice.DomainScope(meta.SpaceID, meta.DomainID)
}

func (m *Module) blobUsageDomainClass(_ context.Context, _ string, domainID string) usageservice.DomainClass {
	// Until the space/domain service is wired as a classifier, only clearly
	// internal-looking domains are marked system. Normal domains remain in the
	// retained non-system aggregate used by quota/product snapshots.
	trimmed := strings.TrimSpace(strings.ToLower(domainID))
	if strings.HasPrefix(trimmed, "_") || strings.HasPrefix(trimmed, "system.") || trimmed == "system" {
		return usageservice.DomainClassSystem
	}
	return usageservice.DomainClassRetainedNonSystem
}

func blobUsageWALMutationID(op string, rec wal.Record) string {
	if !rec.LSN.IsZero() {
		return blobUsageWALLSNMutationID(op, rec.LSN)
	}
	return fmt.Sprintf("blob-usage:%s:wal:no-lsn:%s", op, uuid.NewString())
}

func blobUsageWALLSNMutationID(op string, lsn wal.LSN) string {
	return fmt.Sprintf("blob-usage:%s:wal:%s", op, lsn.String())
}

func blobUsageRaftMutationID(op string, commandID string) string {
	return fmt.Sprintf("blob-usage:%s:raft:%s", op, strings.TrimSpace(commandID))
}

func blobUsageLocalMutationID(op string, meta blobmodel.BlobMeta) string {
	return fmt.Sprintf("blob-usage:%s:local:%s:%s:%s:%s", op, strings.TrimSpace(meta.SpaceID), strings.TrimSpace(meta.DomainID), strings.TrimSpace(meta.BlobID), uuid.NewString())
}

func blobUsageSource(mutationID string) string {
	parts := strings.Split(mutationID, ":")
	if len(parts) >= 3 {
		return "blob_" + parts[2]
	}
	return "blob"
}

func blobUsageRevision(mutationID string) string {
	parts := strings.Split(mutationID, ":")
	if len(parts) >= 4 {
		return strings.Join(parts[3:], ":")
	}
	return mutationID
}
