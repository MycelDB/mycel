package service

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const defaultDiagnosticMaxEntries uint64 = 100000

type LocalDiagnosticCollector struct {
	dataDir    string
	maxEntries uint64
}

type localDiagnosticPath struct {
	Metric string
	Path   string
}

func NewLocalDiagnosticCollector(dataDir string) *LocalDiagnosticCollector {
	return &LocalDiagnosticCollector{dataDir: strings.TrimSpace(dataDir), maxEntries: defaultDiagnosticMaxEntries}
}

func (c *LocalDiagnosticCollector) CollectUsageDiagnostics(ctx context.Context, req DiagnosticRequest) (DiagnosticResult, error) {
	result := DiagnosticResult{Source: "local_filesystem", Evidence: map[string]string{"collector": "local_filesystem"}}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	scope := req.Scope.normalized()
	if err := scope.Validate(); err != nil {
		return result, err
	}
	root := strings.TrimSpace(c.dataDir)
	if root == "" {
		return result, fmt.Errorf("local diagnostic data dir is not configured")
	}
	maxEntries := req.MaxEntries
	if maxEntries == 0 {
		maxEntries = c.maxEntries
	}
	if maxEntries == 0 {
		maxEntries = defaultDiagnosticMaxEntries
	}
	paths := localDiagnosticPaths(root, scope)
	observedAt := time.Now().UTC()
	scopeBytes := uint64(0)
	for _, item := range paths {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		bytes, entries, truncated, err := directorySize(ctx, item.Path, maxEntries-result.EntriesScanned)
		result.EntriesScanned += entries
		if err != nil {
			result.Statuses = append(result.Statuses, Status{Reason: StatusReasonDiagnosticCollectorUnavailable, Severity: StatusSeverityWarning, Message: "local diagnostic collector failed", Scope: scope, Metric: item.Metric, Evidence: map[string]string{"source": result.Source, "path": sanitizeDiagnosticPath(root, item.Path), "error": err.Error()}})
			continue
		}
		if truncated {
			result.Truncated = true
			result.Statuses = append(result.Statuses, Status{Reason: StatusReasonDiagnosticCollectorUnavailable, Severity: StatusSeverityWarning, Message: "local diagnostic collector reached scan limit", Scope: scope, Metric: item.Metric, Evidence: map[string]string{"source": result.Source, "path": sanitizeDiagnosticPath(root, item.Path), "max_entries": fmt.Sprintf("%d", maxEntries)}})
		}
		result.Metrics = append(result.Metrics, diagnosticMetric(item.Metric, bytes, observedAt, result.Source))
		scopeBytes += bytes
		if result.Truncated {
			break
		}
	}
	result.Metrics = append(result.Metrics, diagnosticMetric(MetricDiagnosticLocalScopePhysicalBytes, scopeBytes, observedAt, result.Source))
	sort.Slice(result.Metrics, func(i, j int) bool { return result.Metrics[i].Name < result.Metrics[j].Name })
	result.Evidence["entries_scanned"] = fmt.Sprintf("%d", result.EntriesScanned)
	result.Evidence["max_entries"] = fmt.Sprintf("%d", maxEntries)
	if result.Truncated {
		result.Evidence["truncated"] = "true"
	}
	return result, nil
}

func localDiagnosticPaths(root string, scope Scope) []localDiagnosticPath {
	spaceID := strings.TrimSpace(scope.SpaceID)
	domainID := strings.TrimSpace(scope.DomainID)
	switch scope.Type {
	case ScopeTypeDomain:
		return []localDiagnosticPath{
			{Metric: MetricDiagnosticLocalGraphStoreBytes, Path: filepath.Join(root, "graphs", spaceID, "domains", domainID)},
			{Metric: MetricDiagnosticLocalLexicalStoreBytes, Path: filepath.Join(root, "search", "lexical", spaceID, domainID)},
		}
	case ScopeTypeSpace:
		return []localDiagnosticPath{
			{Metric: MetricDiagnosticLocalGraphStoreBytes, Path: filepath.Join(root, "graphs", spaceID, "domains")},
			{Metric: MetricDiagnosticLocalBlobStoreBytes, Path: filepath.Join(root, "blobs", spaceID)},
			{Metric: MetricDiagnosticLocalBlobMetadataBytes, Path: filepath.Join(root, "blob_meta", spaceID+".json")},
			{Metric: MetricDiagnosticLocalLexicalStoreBytes, Path: filepath.Join(root, "search", "lexical", spaceID)},
			{Metric: MetricDiagnosticLocalSemanticStoreBytes, Path: filepath.Join(root, "graphs", spaceID, "semantic")},
			{Metric: MetricDiagnosticLocalProjectionStoreBytes, Path: filepath.Join(root, "graphs", spaceID, "projections")},
		}
	default:
		return nil
	}
}

func directorySize(ctx context.Context, path string, maxEntries uint64) (uint64, uint64, bool, error) {
	if strings.TrimSpace(path) == "" {
		return 0, 0, false, nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return 0, 1, false, nil
	}
	if !info.IsDir() {
		if info.Mode().IsRegular() {
			if maxEntries == 0 {
				return 0, 0, true, nil
			}
			return uint64(nonNegativeFileSize(info.Size())), 1, false, nil
		}
		return 0, 1, false, nil
	}
	var total uint64
	var entries uint64
	truncated := false
	walkErr := filepath.WalkDir(path, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if current == path {
			return nil
		}
		if entries >= maxEntries {
			truncated = true
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		entries++
		if info.Mode()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.Mode().IsRegular() {
			total += uint64(nonNegativeFileSize(info.Size()))
		}
		return nil
	})
	return total, entries, truncated, walkErr
}

func nonNegativeFileSize(size int64) int64 {
	if size < 0 {
		return 0
	}
	return size
}

func diagnosticMetric(name string, value uint64, observedAt time.Time, source string) Metric {
	return Metric{Name: name, Version: 1, Value: value, Unit: UnitBytes, Authority: AuthorityObservedApproximate, QuotaSafe: false, HardLimitEligible: false, ObservedAt: observedAt, Revision: Revision{Token: "diagnostic:" + strings.TrimSpace(source) + ":" + observedAt.Format(time.RFC3339Nano), Sources: []SourceRevision{{Source: strings.TrimSpace(source), Revision: observedAt.Format(time.RFC3339Nano)}}}}
}

func sanitizeDiagnosticPath(root string, path string) string {
	root = filepath.Clean(root)
	path = filepath.Clean(path)
	if rel, err := filepath.Rel(root, path); err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".." {
		return filepath.ToSlash(rel)
	}
	return filepath.Base(path)
}
