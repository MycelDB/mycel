package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
)

var (
	ErrBackupRunning  = errors.New("backup already running")
	ErrBackupNotFound = errors.New("backup not found")
)

// Manager owns backup policy/status and archive creation.
type Manager struct {
	mu          sync.Mutex
	dataDir     string
	policy      Policy
	logger      *slog.Logger
	quiesce     *quiesce.Coordinator
	version     string
	now         func() time.Time
	running     bool
	lastRunAt   time.Time
	lastSuccess time.Time
	last        RunStatus
	history     []RunStatus
}

type ManagerConfig struct {
	DataDir string
	Policy  Policy
	Logger  *slog.Logger
	Quiesce *quiesce.Coordinator
	Version string
	Now     func() time.Time
}

type TriggerInput struct {
	Source string
	Reason string
}

type LocalArchiveInput struct {
	BackupID      string
	ArchiveName   string
	BackupDir     string
	ArchiveFormat string
	Source        string
	Reason        string
	CreatedAt     time.Time
}

type TriggerResult struct {
	BackupID     string
	ArchivePath  string
	ManifestPath string
	Manifest     Manifest
}

type RunState string

const (
	RunStateIdle      RunState = "idle"
	RunStateRunning   RunState = "running"
	RunStateSucceeded RunState = "succeeded"
	RunStateFailed    RunState = "failed"
)

type RunStatus struct {
	BackupID      string
	State         RunState
	StartedAt     time.Time
	CompletedAt   time.Time
	LastSuccessAt time.Time
	NextRunAt     time.Time
	ArchivePath   string
	ManifestPath  string
	Error         string
}

func NewManager(cfg ManagerConfig) *Manager {
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	policy := EffectivePolicy(cfg.DataDir, cfg.Policy)
	if persisted, err := loadPersistedPolicy(cfg.DataDir); err == nil {
		policy = EffectivePolicy(cfg.DataDir, persisted)
	}
	return &Manager{dataDir: cfg.DataDir, policy: policy, logger: cfg.Logger, quiesce: cfg.Quiesce, version: cfg.Version, now: now, last: RunStatus{State: RunStateIdle}}
}

func (m *Manager) Policy() Policy {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.policy
}

func (m *Manager) UpdatePolicy(ctx context.Context, policy Policy) (Policy, error) {
	if err := ctx.Err(); err != nil {
		return Policy{}, err
	}
	if err := validateRawPolicy(policy); err != nil {
		return Policy{}, err
	}
	policy = EffectivePolicy(m.dataDir, policy)
	if err := validatePolicy(m.dataDir, policy); err != nil {
		return Policy{}, err
	}
	if err := persistPolicy(m.dataDir, policy); err != nil {
		return Policy{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.policy = policy
	return policy, nil
}

func (m *Manager) LastRunAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastRunAt
}

func (m *Manager) LastSuccessAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSuccess
}

func (m *Manager) Status() RunStatus {
	return m.RunStatus()
}

func (m *Manager) RunStatus() RunStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last
}

func (m *Manager) History() []RunStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RunStatus, len(m.history))
	copy(out, m.history)
	return out
}

func (m *Manager) ListBackups(ctx context.Context) ([]Manifest, error) {
	policy := m.Policy()
	_, backupDir, err := validateBackupDir(m.dataDir, policy.BackupDir)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []Manifest{}, nil
		}
		return nil, err
	}
	manifests := []Manifest{}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".manifest.json") {
			continue
		}
		manifest, err := readManifestFile(filepath.Join(backupDir, entry.Name()))
		if err != nil {
			continue
		}
		if strings.TrimSpace(manifest.BackupID) == "" || strings.TrimSpace(manifest.ArchiveName) == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(backupDir, manifest.ArchiveName)); err != nil {
			continue
		}
		manifests = append(manifests, manifest)
	}
	sort.SliceStable(manifests, func(i, j int) bool { return manifests[i].CreatedAt.After(manifests[j].CreatedAt) })
	return manifests, nil
}

func (m *Manager) DeleteBackup(ctx context.Context, backupID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	backupID = strings.TrimSpace(backupID)
	if backupID == "" || strings.ContainsAny(backupID, `/\\`) {
		return fmt.Errorf("backup_id is required")
	}
	policy := m.Policy()
	_, backupDir, err := validateBackupDir(m.dataDir, policy.BackupDir)
	if err != nil {
		return err
	}
	manifestPath := filepath.Join(backupDir, backupID+".manifest.json")
	manifest, err := readManifestFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrBackupNotFound
		}
		return err
	}
	if manifest.BackupID != backupID {
		return ErrBackupNotFound
	}
	if !validBackupArchiveName(backupID, manifest.ArchiveName) {
		return fmt.Errorf("invalid backup manifest archive_name")
	}
	archivePath := filepath.Join(backupDir, manifest.ArchiveName)
	if !pathWithinDir(backupDir, archivePath) {
		return fmt.Errorf("invalid backup manifest archive path")
	}
	if err := os.Remove(archivePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(manifestPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (m *Manager) Trigger(ctx context.Context, input TriggerInput) (TriggerResult, error) {
	if err := ctx.Err(); err != nil {
		return TriggerResult{}, err
	}
	backupID := "backup-" + m.now().UTC().Format("20060102T150405Z") + "-" + uuid.NewString()
	started := m.now().UTC()
	if !m.beginRun(backupID, started) {
		return TriggerResult{}, ErrBackupRunning
	}
	result, err := m.run(ctx, backupID, started, input)
	if err == nil {
		if retentionErr := m.ApplyRetention(ctx); retentionErr != nil {
			err = retentionErr
		}
	}
	m.finishRun(result, err)
	return result, err
}

// RunScheduledBackup triggers the same archive path used by manual backup. The
// scheduler/retention policy is expanded in later phases.
func (m *Manager) RunScheduledBackup(ctx context.Context) error {
	if !m.Policy().Enabled {
		return nil
	}
	_, err := m.Trigger(ctx, TriggerInput{Source: "scheduler", Reason: "scheduled backup"})
	return err
}

func (m *Manager) beginRun(backupID string, started time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return false
	}
	m.running = true
	m.lastRunAt = started
	m.last = RunStatus{BackupID: backupID, State: RunStateRunning, StartedAt: started, LastSuccessAt: m.lastSuccess}
	return true
}

func (m *Manager) finishRun(result TriggerResult, err error) {
	completed := m.now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = false
	status := m.last
	status.CompletedAt = completed
	if err != nil {
		status.State = RunStateFailed
		status.Error = err.Error()
	} else {
		status.State = RunStateSucceeded
		status.ArchivePath = result.ArchivePath
		status.ManifestPath = result.ManifestPath
		m.lastSuccess = completed
		status.LastSuccessAt = completed
	}
	m.last = status
	m.history = append(m.history, status)
	limit := m.policy.StatusHistoryLimit
	if limit <= 0 {
		limit = DefaultStatusHistoryLimit
	}
	if len(m.history) > limit {
		m.history = append([]RunStatus(nil), m.history[len(m.history)-limit:]...)
	}
}

func (m *Manager) CreateLocalArchive(ctx context.Context, input LocalArchiveInput) (TriggerResult, error) {
	if err := ctx.Err(); err != nil {
		return TriggerResult{}, err
	}
	backupID := strings.TrimSpace(input.BackupID)
	if backupID == "" || strings.ContainsAny(backupID, `/\\`) {
		return TriggerResult{}, fmt.Errorf("backup_id is required")
	}
	started := input.CreatedAt.UTC()
	if started.IsZero() {
		started = m.now().UTC()
	}
	if !m.beginRun(backupID, started) {
		return TriggerResult{}, ErrBackupRunning
	}
	policy := m.Policy()
	if strings.TrimSpace(input.BackupDir) != "" {
		policy.BackupDir = strings.TrimSpace(input.BackupDir)
	}
	if strings.TrimSpace(input.ArchiveFormat) != "" {
		policy.ArchiveFormat = ArchiveFormat(strings.TrimSpace(input.ArchiveFormat))
	}
	result, err := m.createArchiveWithPolicy(ctx, backupID, started, TriggerInput{Source: input.Source, Reason: input.Reason}, strings.TrimSpace(input.ArchiveName), false, policy)
	m.finishRun(result, err)
	return result, err
}

func (m *Manager) run(ctx context.Context, backupID string, started time.Time, input TriggerInput) (TriggerResult, error) {
	return m.createArchive(ctx, backupID, started, input, "", true)
}

func (m *Manager) createArchive(ctx context.Context, backupID string, started time.Time, input TriggerInput, archiveNameOverride string, acquireQuiesce bool) (TriggerResult, error) {
	return m.createArchiveWithPolicy(ctx, backupID, started, input, archiveNameOverride, acquireQuiesce, m.Policy())
}

func (m *Manager) createArchiveWithPolicy(ctx context.Context, backupID string, started time.Time, input TriggerInput, archiveNameOverride string, acquireQuiesce bool, policy Policy) (TriggerResult, error) {
	policy = EffectivePolicy(m.dataDir, policy)
	archiveExt, err := ArchiveExtension(policy.ArchiveFormat)
	if err != nil {
		return TriggerResult{}, err
	}
	dataDir, backupDir, err := validateBackupDir(m.dataDir, policy.BackupDir)
	if err != nil {
		return TriggerResult{}, err
	}
	runCtx := ctx
	cancel := func() {}
	if policy.BackupTimeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, policy.BackupTimeout)
	}
	defer cancel()

	var lease *quiesce.CompositeLease
	if acquireQuiesce && m.quiesce != nil {
		quiesceCtx := runCtx
		quiesceCancel := func() {}
		if policy.QuiesceDrainTimeout > 0 {
			quiesceCtx, quiesceCancel = context.WithTimeout(runCtx, policy.QuiesceDrainTimeout)
		}
		lease, err = m.quiesce.QuiesceAll(quiesceCtx, quiesce.Request{Reason: firstNonEmpty(input.Reason, "backup"), Mode: quiesce.ModeBackup, Source: firstNonEmpty(input.Source, "backup-manager")})
		quiesceCancel()
		if err != nil {
			return TriggerResult{}, err
		}
		defer func() {
			if releaseErr := lease.Release(context.Background()); releaseErr != nil && m.logger != nil {
				m.logger.Warn("failed to release backup quiesce lease", "error", releaseErr)
			}
		}()
	}

	stagingRoot := filepath.Join(backupDir, ".staging")
	stagingDir := filepath.Join(stagingRoot, backupID)
	defer os.RemoveAll(stagingDir)
	if err := stageSnapshot(runCtx, dataDir, stagingDir, policy.IncludeLogs); err != nil {
		return TriggerResult{}, fmt.Errorf("stage snapshot: %w", err)
	}

	archiveName := backupID + archiveExt
	if archiveNameOverride != "" {
		if filepath.Base(archiveNameOverride) != archiveNameOverride {
			return TriggerResult{}, fmt.Errorf("archive_name must be a base name")
		}
		if !strings.HasSuffix(archiveNameOverride, archiveExt) {
			return TriggerResult{}, fmt.Errorf("archive_name must end with %s", archiveExt)
		}
		archiveName = archiveNameOverride
	}
	archivePath := filepath.Join(backupDir, archiveName)
	archiveTmp := archivePath + ".tmp"
	defer os.Remove(archiveTmp)
	if err := WriteArchive(runCtx, policy.ArchiveFormat, stagingDir, archiveTmp); err != nil {
		return TriggerResult{}, fmt.Errorf("create archive: %w", err)
	}
	checksum, size, err := fileSHA256(archiveTmp)
	if err != nil {
		return TriggerResult{}, fmt.Errorf("checksum archive: %w", err)
	}
	manifest := Manifest{Version: ManifestVersion, BackupID: backupID, ArchiveName: archiveName, CreatedAt: started, CompletedAt: m.now().UTC(), SizeBytes: size, ChecksumSHA256: checksum, DaemonVersion: m.version, Policy: policySummary(policy)}
	manifestPath := filepath.Join(backupDir, backupID+".manifest.json")
	manifestTmp := manifestPath + ".tmp"
	defer os.Remove(manifestTmp)
	if err := writeManifestFile(manifestTmp, manifest); err != nil {
		return TriggerResult{}, fmt.Errorf("write manifest: %w", err)
	}
	if err := os.Rename(archiveTmp, archivePath); err != nil {
		return TriggerResult{}, fmt.Errorf("complete archive: %w", err)
	}
	if err := os.Rename(manifestTmp, manifestPath); err != nil {
		_ = os.Remove(archivePath)
		return TriggerResult{}, fmt.Errorf("complete manifest: %w", err)
	}
	if m.logger != nil {
		m.logger.Info("backup completed", "backup_id", backupID, "archive", archivePath, "size_bytes", size)
	}
	return TriggerResult{BackupID: backupID, ArchivePath: archivePath, ManifestPath: manifestPath, Manifest: manifest}, nil
}

func validateRawPolicy(policy Policy) error {
	if policy.Interval < 0 {
		return fmt.Errorf("interval must not be negative")
	}
	if policy.RetentionCount < 0 {
		return fmt.Errorf("retention_count must not be negative")
	}
	if policy.QuiesceDrainTimeout < 0 {
		return fmt.Errorf("quiesce_drain_timeout must not be negative")
	}
	if policy.BackupTimeout < 0 {
		return fmt.Errorf("backup_timeout must not be negative")
	}
	if policy.RetryAfter < 0 {
		return fmt.Errorf("retry_after must not be negative")
	}
	if policy.StatusHistoryLimit < 0 {
		return fmt.Errorf("status_history_limit must not be negative")
	}
	return nil
}

func validatePolicy(dataDir string, policy Policy) error {
	if policy.Interval <= 0 {
		return fmt.Errorf("interval must be positive")
	}
	if policy.RetentionCount <= 0 {
		return fmt.Errorf("retention_count must be positive")
	}
	if policy.QuiesceDrainTimeout <= 0 {
		return fmt.Errorf("quiesce_drain_timeout must be positive")
	}
	if policy.BackupTimeout <= 0 {
		return fmt.Errorf("backup_timeout must be positive")
	}
	if policy.RetryAfter <= 0 {
		return fmt.Errorf("retry_after must be positive")
	}
	if policy.StatusHistoryLimit <= 0 {
		return fmt.Errorf("status_history_limit must be positive")
	}
	if !IsSupportedArchiveFormat(policy.ArchiveFormat) {
		return fmt.Errorf("unsupported backup archive_format %q", policy.ArchiveFormat)
	}
	if err := ValidateSchedule(policy); err != nil {
		return err
	}
	_, _, err := validateBackupDir(dataDir, policy.BackupDir)
	return err
}

func policyPath(dataDir string) string {
	return filepath.Join(dataDir, "meta", "backup", "policy.json")
}

func loadPersistedPolicy(dataDir string) (Policy, error) {
	raw, err := os.ReadFile(policyPath(dataDir))
	if err != nil {
		return Policy{}, err
	}
	var policy Policy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return Policy{}, err
	}
	return policy, nil
}

func persistPolicy(dataDir string, policy Policy) error {
	path := policyPath(dataDir)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func validBackupArchiveName(backupID string, archiveName string) bool {
	if filepath.Base(archiveName) != archiveName {
		return false
	}
	for _, format := range []ArchiveFormat{ArchiveFormatZip, ArchiveFormatTar, ArchiveFormatTarGz, ArchiveFormatTarZst} {
		ext, err := ArchiveExtension(format)
		if err == nil && archiveName == backupID+ext {
			return true
		}
	}
	return false
}

func pathWithinDir(dir string, path string) bool {
	dir = filepath.Clean(dir)
	path = filepath.Clean(path)
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "."
}

func readManifestFile(path string) (Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func writeManifestAtomic(path string, manifest Manifest) error {
	tmp := path + ".tmp"
	if err := writeManifestFile(tmp, manifest); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func writeManifestFile(path string, manifest Manifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o600)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
