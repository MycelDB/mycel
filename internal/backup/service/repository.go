package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/fsperm"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	RepositoryProviderS3Compatible = "s3-compatible"

	SecretSchemeAWSIRSA        = "aws-irsa"
	SecretSchemeK8sSecret      = "k8s-secret"
	SecretSchemeExternalSecret = "external-secret"
	SecretSchemeVault          = "vault"
	SecretSchemeKMSKey         = "kms-key"
	SecretSchemeTLSCA          = "tls-ca"
	SecretSchemeEnv            = "env"
	SecretSchemeFileRef        = "file-ref"

	EncryptionModeSSEKMS          = "sse-kms"
	EncryptionModeSSES3           = "sse-s3"
	EncryptionModeProviderManaged = "provider-managed"
	EncryptionModeClientEnvelope  = "client-envelope"
	EncryptionModeNone            = "none"

	ValidationStateNotValidated = "not_validated"
	ValidationStateValidating   = "validating"
	ValidationStateValid        = "valid"
	ValidationStateWarning      = "warning"
	ValidationStateInvalid      = "invalid"
	ValidationStateUnsupported  = "unsupported"
)

var (
	ErrBackupRepositoryNotFound      = errors.New("backup repository not found")
	ErrBackupRepositoryAlreadyExists = errors.New("backup repository already exists")
)

type SecretReference struct {
	Scheme      string            `json:"scheme,omitempty"`
	Name        string            `json:"name,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	Path        string            `json:"path,omitempty"`
	Keys        map[string]string `json:"keys,omitempty"`
	Description string            `json:"description,omitempty"`
}

type EncryptionPolicy struct {
	Mode                           string          `json:"mode,omitempty"`
	KeyRef                         SecretReference `json:"key_ref,omitempty"`
	KMSKeyID                       string          `json:"kms_key_id,omitempty"`
	RequireBucketDefaultEncryption bool            `json:"require_bucket_default_encryption,omitempty"`
}

type S3CompatibleRepositoryConfig struct {
	Bucket         string `json:"bucket,omitempty"`
	Prefix         string `json:"prefix,omitempty"`
	Region         string `json:"region,omitempty"`
	EndpointURI    string `json:"endpoint_uri,omitempty"`
	ForcePathStyle bool   `json:"force_path_style,omitempty"`
	DisableTLS     bool   `json:"disable_tls,omitempty"`
}

type BackupRepository struct {
	RepositoryID  string                            `json:"repository_id"`
	DisplayName   string                            `json:"display_name,omitempty"`
	Provider      string                            `json:"provider"`
	S3            S3CompatibleRepositoryConfig      `json:"s3,omitempty"`
	CredentialRef SecretReference                   `json:"credential_ref,omitempty"`
	Encryption    EncryptionPolicy                  `json:"encryption,omitempty"`
	TLSCARef      SecretReference                   `json:"tls_ca_ref,omitempty"`
	DevOnly       bool                              `json:"dev_only,omitempty"`
	CreateTime    time.Time                         `json:"create_time,omitempty"`
	UpdateTime    time.Time                         `json:"update_time,omitempty"`
	Validation    BackupRepositoryValidationSummary `json:"validation,omitempty"`
}

type BackupRepositoryValidationSummary struct {
	State          string                  `json:"state"`
	CheckedAt      time.Time               `json:"checked_at,omitempty"`
	Probes         []ValidationProbeResult `json:"probes,omitempty"`
	Blockers       []ValidationBlocker     `json:"blockers,omitempty"`
	SanitizedError string                  `json:"sanitized_error,omitempty"`
}

type ValidationProbeResult struct {
	Name      string    `json:"name"`
	State     string    `json:"state"`
	Detail    string    `json:"detail,omitempty"`
	CheckedAt time.Time `json:"checked_at,omitempty"`
}

type ValidationBlocker struct {
	Reason    string `json:"reason"`
	Detail    string `json:"detail,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
	FieldPath string `json:"field_path,omitempty"`
}

type backupRepositoryUpsertRecord struct {
	Repository   BackupRepository `json:"repository"`
	AllowReplace bool             `json:"allow_replace,omitempty"`
}

type backupRepositoryDeleteRecord struct {
	RepositoryID string `json:"repository_id"`
}

type backupRepositoryValidationRecord struct {
	RepositoryID string                            `json:"repository_id"`
	Validation   BackupRepositoryValidationSummary `json:"validation"`
	UpdatedAt    time.Time                         `json:"updated_at"`
}

type backupRepositoryCatalogFile struct {
	Version      int                `json:"version"`
	Repositories []BackupRepository `json:"repositories"`
}

type backupRepositoryValidator interface {
	ValidateBackupRepository(context.Context, BackupRepository) BackupRepositoryValidationSummary
}

type s3RepositoryValidator struct{}

func (m *Module) ListBackupRepositories() []BackupRepository {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureBackupRepositoriesLocked()
	out := make([]BackupRepository, 0, len(m.repositories))
	for _, repo := range m.repositories {
		out = append(out, cloneBackupRepository(repo))
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].RepositoryID < out[j].RepositoryID })
	return out
}

func (m *Module) GetBackupRepository(repositoryID string) (BackupRepository, error) {
	repositoryID = strings.TrimSpace(repositoryID)
	if repositoryID == "" {
		return BackupRepository{}, fmt.Errorf("repository_id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureBackupRepositoriesLocked()
	repo, ok := m.repositories[repositoryID]
	if !ok {
		return BackupRepository{}, ErrBackupRepositoryNotFound
	}
	return cloneBackupRepository(repo), nil
}

func (m *Module) CreateBackupRepository(ctx context.Context, repository BackupRepository) (BackupRepository, error) {
	repository = normalizeBackupRepository(repository)
	if repository.RepositoryID == "" {
		repository.RepositoryID = "repo-" + uuid.NewString()
	}
	if err := validateBackupRepository(repository); err != nil {
		return BackupRepository{}, err
	}
	m.mu.Lock()
	m.ensureBackupRepositoriesLocked()
	_, exists := m.repositories[repository.RepositoryID]
	m.mu.Unlock()
	if exists {
		return BackupRepository{}, ErrBackupRepositoryAlreadyExists
	}
	if err := m.commitBackupRepositoryRecord(ctx, recordTypeBackupRepositoryUpsert, backupRepositoryUpsertRecord{Repository: repository}); err != nil {
		return BackupRepository{}, err
	}
	return m.GetBackupRepository(repository.RepositoryID)
}

func (m *Module) UpdateBackupRepository(ctx context.Context, repository BackupRepository) (BackupRepository, error) {
	repository = normalizeBackupRepository(repository)
	if err := validateBackupRepository(repository); err != nil {
		return BackupRepository{}, err
	}
	m.mu.Lock()
	m.ensureBackupRepositoriesLocked()
	_, exists := m.repositories[repository.RepositoryID]
	m.mu.Unlock()
	if !exists {
		return BackupRepository{}, ErrBackupRepositoryNotFound
	}
	if err := m.commitBackupRepositoryRecord(ctx, recordTypeBackupRepositoryUpsert, backupRepositoryUpsertRecord{Repository: repository, AllowReplace: true}); err != nil {
		return BackupRepository{}, err
	}
	return m.GetBackupRepository(repository.RepositoryID)
}

func (m *Module) DeleteBackupRepository(ctx context.Context, repositoryID string) error {
	repositoryID = strings.TrimSpace(repositoryID)
	if repositoryID == "" {
		return fmt.Errorf("repository_id is required")
	}
	m.mu.Lock()
	m.ensureBackupRepositoriesLocked()
	_, exists := m.repositories[repositoryID]
	m.mu.Unlock()
	if !exists {
		return ErrBackupRepositoryNotFound
	}
	return m.commitBackupRepositoryRecord(ctx, recordTypeBackupRepositoryDelete, backupRepositoryDeleteRecord{RepositoryID: repositoryID})
}

func (m *Module) ValidateBackupRepository(ctx context.Context, repositoryID string) (BackupRepositoryValidationSummary, error) {
	repo, err := m.GetBackupRepository(repositoryID)
	if err != nil {
		return BackupRepositoryValidationSummary{}, err
	}
	validator := backupRepositoryValidator(s3RepositoryValidator{})
	if m.repositoryValidator != nil {
		validator = m.repositoryValidator
	}
	validation := sanitizeRepositoryValidationSummary(validator.ValidateBackupRepository(ctx, repo))
	if validation.State == "" {
		validation.State = ValidationStateInvalid
	}
	if validation.CheckedAt.IsZero() {
		validation.CheckedAt = time.Now().UTC()
	}
	if err := m.commitBackupRepositoryRecord(ctx, recordTypeBackupRepositoryValidate, backupRepositoryValidationRecord{RepositoryID: repo.RepositoryID, Validation: validation, UpdatedAt: time.Now().UTC()}); err != nil {
		return BackupRepositoryValidationSummary{}, err
	}
	return validation, nil
}

func (m *Module) commitBackupRepositoryRecord(ctx context.Context, typ wal.RecordType, payload any) error {
	if m.raftMode() {
		return m.commitRaft(ctx, typ, payload)
	}
	if m.wal != nil {
		return m.commitWAL(ctx, typ, payload)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	appliers := m.backupRepositoryWALAppliers()
	applier, ok := appliers[typ]
	if !ok {
		return fmt.Errorf("unsupported backup repository record type %s", typ)
	}
	return applier.ApplyWAL(ctx, wal.Record{Type: typ, Payload: raw})
}

func (m *Module) backupRepositoryWALAppliers() map[wal.RecordType]wal.Applier {
	return map[wal.RecordType]wal.Applier{
		recordTypeBackupRepositoryUpsert:   wal.ApplierFunc(m.applyBackupRepositoryUpsert),
		recordTypeBackupRepositoryDelete:   wal.ApplierFunc(m.applyBackupRepositoryDelete),
		recordTypeBackupRepositoryValidate: wal.ApplierFunc(m.applyBackupRepositoryValidation),
	}
}

func (m *Module) applyBackupRepositoryUpsert(ctx context.Context, rec wal.Record) error {
	var payload backupRepositoryUpsertRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	repository := normalizeBackupRepository(payload.Repository)
	repository.Validation = sanitizeRepositoryValidationSummary(repository.Validation)
	if repository.Validation.State == "" {
		repository.Validation.State = ValidationStateNotValidated
	}
	if err := validateBackupRepository(repository); err != nil {
		return err
	}
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureBackupRepositoriesLocked()
	existing, exists := m.repositories[repository.RepositoryID]
	if exists && !payload.AllowReplace {
		return ErrBackupRepositoryAlreadyExists
	}
	if !exists {
		repository.CreateTime = now
	} else if !existing.CreateTime.IsZero() {
		repository.CreateTime = existing.CreateTime
	}
	if repository.CreateTime.IsZero() {
		repository.CreateTime = now
	}
	repository.UpdateTime = now
	m.repositories[repository.RepositoryID] = cloneBackupRepository(repository)
	return m.persistBackupRepositoryCatalogLocked()
}

func (m *Module) applyBackupRepositoryDelete(ctx context.Context, rec wal.Record) error {
	var payload backupRepositoryDeleteRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	repositoryID := strings.TrimSpace(payload.RepositoryID)
	if repositoryID == "" {
		return fmt.Errorf("repository_id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureBackupRepositoriesLocked()
	if _, ok := m.repositories[repositoryID]; !ok {
		return ErrBackupRepositoryNotFound
	}
	delete(m.repositories, repositoryID)
	return m.persistBackupRepositoryCatalogLocked()
}

func (m *Module) applyBackupRepositoryValidation(ctx context.Context, rec wal.Record) error {
	var payload backupRepositoryValidationRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_ = ctx
	repositoryID := strings.TrimSpace(payload.RepositoryID)
	if repositoryID == "" {
		return fmt.Errorf("repository_id is required")
	}
	validation := sanitizeRepositoryValidationSummary(payload.Validation)
	if validation.State == "" {
		validation.State = ValidationStateInvalid
	}
	if validation.CheckedAt.IsZero() {
		validation.CheckedAt = time.Now().UTC()
	}
	updatedAt := payload.UpdatedAt.UTC()
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureBackupRepositoriesLocked()
	repository, ok := m.repositories[repositoryID]
	if !ok {
		return ErrBackupRepositoryNotFound
	}
	repository.Validation = validation
	repository.UpdateTime = updatedAt
	m.repositories[repositoryID] = repository
	return m.persistBackupRepositoryCatalogLocked()
}

func (m *Module) ensureBackupRepositoriesLocked() {
	if m.repositories == nil {
		m.repositories = map[string]BackupRepository{}
	}
}

func (m *Module) loadBackupRepositoryCatalog() error {
	path := backupRepositoryCatalogPath(m.dataDir)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var catalog backupRepositoryCatalogFile
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return err
	}
	if catalog.Version != 1 {
		return fmt.Errorf("unsupported backup repository catalog version %d", catalog.Version)
	}
	repositories := map[string]BackupRepository{}
	for _, repo := range catalog.Repositories {
		repo = normalizeBackupRepository(repo)
		repo.Validation = sanitizeRepositoryValidationSummary(repo.Validation)
		if repo.Validation.State == "" {
			repo.Validation.State = ValidationStateNotValidated
		}
		if err := validateBackupRepository(repo); err != nil {
			return fmt.Errorf("repository %s: %w", repo.RepositoryID, err)
		}
		repositories[repo.RepositoryID] = cloneBackupRepository(repo)
	}
	m.mu.Lock()
	m.repositories = repositories
	m.mu.Unlock()
	return nil
}

func (m *Module) persistBackupRepositoryCatalogLocked() error {
	repositories := make([]BackupRepository, 0, len(m.repositories))
	for _, repo := range m.repositories {
		repositories = append(repositories, cloneBackupRepository(repo))
	}
	sort.SliceStable(repositories, func(i, j int) bool { return repositories[i].RepositoryID < repositories[j].RepositoryID })
	catalog := backupRepositoryCatalogFile{Version: 1, Repositories: repositories}
	raw, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return err
	}
	path := backupRepositoryCatalogPath(m.dataDir)
	if err := os.MkdirAll(filepath.Dir(path), fsperm.PrivateDir); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), fsperm.PrivateFile); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func backupRepositoryCatalogPath(dataDir string) string {
	return filepath.Join(dataDir, "meta", "backup", "repositories.json")
}

func normalizeBackupRepository(repo BackupRepository) BackupRepository {
	repo.RepositoryID = strings.TrimSpace(repo.RepositoryID)
	repo.DisplayName = strings.TrimSpace(repo.DisplayName)
	repo.Provider = normalizeRepositoryProvider(repo.Provider)
	repo.S3.Bucket = strings.TrimSpace(repo.S3.Bucket)
	repo.S3.Prefix = cleanRepositoryPrefix(repo.S3.Prefix)
	repo.S3.Region = strings.TrimSpace(repo.S3.Region)
	repo.S3.EndpointURI = strings.TrimSpace(repo.S3.EndpointURI)
	repo.CredentialRef = normalizeSecretReference(repo.CredentialRef)
	repo.Encryption.Mode = normalizeEncryptionMode(repo.Encryption.Mode)
	repo.Encryption.KeyRef = normalizeSecretReference(repo.Encryption.KeyRef)
	repo.Encryption.KMSKeyID = strings.TrimSpace(repo.Encryption.KMSKeyID)
	repo.TLSCARef = normalizeSecretReference(repo.TLSCARef)
	return repo
}

func normalizeRepositoryProvider(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "", RepositoryProviderS3Compatible, "s3", "s3_compatible":
		return RepositoryProviderS3Compatible
	default:
		return strings.ToLower(strings.TrimSpace(provider))
	}
}

func normalizeSecretReference(ref SecretReference) SecretReference {
	ref.Scheme = strings.ToLower(strings.TrimSpace(ref.Scheme))
	ref.Name = strings.TrimSpace(ref.Name)
	ref.Namespace = strings.TrimSpace(ref.Namespace)
	ref.Path = strings.TrimSpace(ref.Path)
	ref.Description = strings.TrimSpace(ref.Description)
	if len(ref.Keys) > 0 {
		keys := make(map[string]string, len(ref.Keys))
		for k, v := range ref.Keys {
			keys[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
		ref.Keys = keys
	}
	return ref
}

func normalizeEncryptionMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", EncryptionModeProviderManaged:
		return EncryptionModeProviderManaged
	case EncryptionModeSSEKMS, "sse_kms":
		return EncryptionModeSSEKMS
	case EncryptionModeSSES3, "sse_s3":
		return EncryptionModeSSES3
	case EncryptionModeClientEnvelope, "client_envelope":
		return EncryptionModeClientEnvelope
	case EncryptionModeNone:
		return EncryptionModeNone
	default:
		return strings.ToLower(strings.TrimSpace(mode))
	}
}

var repositoryIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateBackupRepository(repo BackupRepository) error {
	if !repositoryIDPattern.MatchString(repo.RepositoryID) {
		return fmt.Errorf("repository_id is required and must contain only letters, numbers, dot, underscore, or dash")
	}
	if repo.Provider != RepositoryProviderS3Compatible {
		return fmt.Errorf("unsupported backup repository provider %q", repo.Provider)
	}
	if repo.S3.Bucket == "" {
		return fmt.Errorf("s3.bucket is required")
	}
	if strings.ContainsAny(repo.S3.Bucket, `/\\`) {
		return fmt.Errorf("s3.bucket must be a bucket name, not a path")
	}
	if repo.S3.EndpointURI != "" {
		u, err := url.Parse(repo.S3.EndpointURI)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("s3.endpoint_uri must be an absolute URL")
		}
		if u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("s3.endpoint_uri must not contain query strings or fragments")
		}
		if u.Scheme != "https" && u.Scheme != "http" {
			return fmt.Errorf("s3.endpoint_uri must use http or https")
		}
		if u.Scheme == "http" && !repo.S3.DisableTLS {
			return fmt.Errorf("http endpoint_uri requires s3.disable_tls=true")
		}
		if repo.S3.DisableTLS && u.Scheme != "http" {
			return fmt.Errorf("s3.disable_tls requires an http endpoint_uri")
		}
		if repo.S3.DisableTLS && !repo.DevOnly {
			return fmt.Errorf("s3.disable_tls requires dev_only=true")
		}
	}
	if usesDevelopmentCredentialReference(repo.CredentialRef) && !repo.DevOnly {
		return fmt.Errorf("%s credential references require dev_only=true", repo.CredentialRef.Scheme)
	}
	if usesDevelopmentCredentialReference(repo.Encryption.KeyRef) && !repo.DevOnly {
		return fmt.Errorf("%s encryption key references require dev_only=true", repo.Encryption.KeyRef.Scheme)
	}
	if repo.Encryption.Mode == EncryptionModeNone && !repo.DevOnly {
		return fmt.Errorf("encryption.mode none requires dev_only=true")
	}
	if err := validateCredentialReference(repo.CredentialRef, false); err != nil {
		return fmt.Errorf("credential_ref: %w", err)
	}
	if repo.TLSCARef.Scheme != "" {
		if err := validateCredentialReference(repo.TLSCARef, true); err != nil {
			return fmt.Errorf("tls_ca_ref: %w", err)
		}
	}
	if err := validateEncryptionPolicy(repo.Encryption); err != nil {
		return fmt.Errorf("encryption: %w", err)
	}
	return nil
}

func usesDevelopmentCredentialReference(ref SecretReference) bool {
	switch ref.Scheme {
	case SecretSchemeEnv, SecretSchemeFileRef:
		return true
	default:
		return false
	}
}

func validateCredentialReference(ref SecretReference, allowTLSCA bool) error {
	if ref.Scheme == "" {
		return nil
	}
	switch ref.Scheme {
	case SecretSchemeAWSIRSA:
		return nil
	case SecretSchemeEnv:
		if len(ref.Keys) == 0 {
			return fmt.Errorf("env reference requires keys naming environment variables")
		}
		for key, value := range ref.Keys {
			if key == "" || value == "" {
				return fmt.Errorf("env reference keys must not be empty")
			}
			if !envNamePattern.MatchString(value) || looksLikeRawCredential(value) {
				return fmt.Errorf("env reference value for %s must be an environment variable name", key)
			}
		}
		return nil
	case SecretSchemeFileRef:
		if ref.Path == "" {
			return fmt.Errorf("file-ref requires path")
		}
		return nil
	case SecretSchemeK8sSecret, SecretSchemeExternalSecret:
		if ref.Name == "" {
			return fmt.Errorf("%s requires name", ref.Scheme)
		}
		return nil
	case SecretSchemeVault:
		if ref.Path == "" {
			return fmt.Errorf("vault requires path")
		}
		return nil
	case SecretSchemeKMSKey:
		if ref.Name == "" && ref.Path == "" {
			return fmt.Errorf("kms-key requires name or path")
		}
		return nil
	case SecretSchemeTLSCA:
		if !allowTLSCA {
			return fmt.Errorf("tls-ca is only valid for tls_ca_ref")
		}
		if ref.Name == "" && ref.Path == "" {
			return fmt.Errorf("tls-ca requires name or path")
		}
		return nil
	default:
		return fmt.Errorf("unsupported scheme %q", ref.Scheme)
	}
}

func validateEncryptionPolicy(policy EncryptionPolicy) error {
	switch policy.Mode {
	case "", EncryptionModeProviderManaged, EncryptionModeSSEKMS, EncryptionModeSSES3, EncryptionModeNone:
	case EncryptionModeClientEnvelope:
		return fmt.Errorf("client-envelope encryption is not supported yet")
	default:
		return fmt.Errorf("unsupported mode %q", policy.Mode)
	}
	if policy.KeyRef.Scheme != "" {
		if err := validateCredentialReference(policy.KeyRef, false); err != nil {
			return fmt.Errorf("key_ref: %w", err)
		}
	}
	return nil
}

func cleanRepositoryPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	prefix = strings.Trim(prefix, "/")
	if prefix == "." {
		return ""
	}
	return prefix
}

func cloneBackupRepository(in BackupRepository) BackupRepository {
	in.CredentialRef = cloneSecretReference(in.CredentialRef)
	in.Encryption.KeyRef = cloneSecretReference(in.Encryption.KeyRef)
	in.TLSCARef = cloneSecretReference(in.TLSCARef)
	in.Validation.Probes = append([]ValidationProbeResult(nil), in.Validation.Probes...)
	in.Validation.Blockers = append([]ValidationBlocker(nil), in.Validation.Blockers...)
	return in
}

func cloneSecretReference(in SecretReference) SecretReference {
	if len(in.Keys) > 0 {
		keys := make(map[string]string, len(in.Keys))
		for k, v := range in.Keys {
			keys[k] = v
		}
		in.Keys = keys
	}
	return in
}

func sanitizeRepositoryValidationSummary(in BackupRepositoryValidationSummary) BackupRepositoryValidationSummary {
	in.SanitizedError = sanitizeRepositoryError(in.SanitizedError)
	for i := range in.Probes {
		in.Probes[i].Detail = sanitizeRepositoryError(in.Probes[i].Detail)
	}
	for i := range in.Blockers {
		in.Blockers[i].Detail = sanitizeRepositoryError(in.Blockers[i].Detail)
	}
	return in
}

func (s3RepositoryValidator) ValidateBackupRepository(ctx context.Context, repo BackupRepository) BackupRepositoryValidationSummary {
	now := time.Now().UTC()
	summary := BackupRepositoryValidationSummary{State: ValidationStateValid, CheckedAt: now}
	addProbe := func(name, state, detail string) {
		summary.Probes = append(summary.Probes, ValidationProbeResult{Name: name, State: state, Detail: sanitizeRepositoryError(detail), CheckedAt: time.Now().UTC()})
		if state == ValidationStateInvalid && summary.State != ValidationStateUnsupported {
			summary.State = ValidationStateInvalid
		}
		if state == ValidationStateUnsupported {
			summary.State = ValidationStateUnsupported
		}
	}
	addBlocker := func(reason, detail string, retryable bool, fieldPath string) {
		summary.Blockers = append(summary.Blockers, ValidationBlocker{Reason: reason, Detail: sanitizeRepositoryError(detail), Retryable: retryable, FieldPath: fieldPath})
	}
	if err := validateBackupRepository(repo); err != nil {
		addProbe("configuration", ValidationStateInvalid, err.Error())
		addBlocker("invalid_repository_configuration", err.Error(), false, "repository")
		summary.SanitizedError = sanitizeRepositoryError(err.Error())
		return summary
	}
	addProbe("configuration", ValidationStateValid, "repository configuration is structurally valid")
	client, err := newS3RepositoryClient(ctx, repo)
	if err != nil {
		state := ValidationStateInvalid
		if errors.Is(err, errUnsupportedCredentialReference) {
			state = ValidationStateUnsupported
		}
		addProbe("credentials", state, err.Error())
		addBlocker("credential_resolution_failed", err.Error(), true, "credential_ref")
		summary.SanitizedError = sanitizeRepositoryError(err.Error())
		return summary
	}
	addProbe("credentials", ValidationStateValid, "credential reference resolved without exposing credential material")

	if repo.Encryption.RequireBucketDefaultEncryption {
		if err := validateBucketDefaultEncryption(ctx, client, repo); err != nil {
			addProbe("bucket_encryption", ValidationStateInvalid, err.Error())
			addBlocker("bucket_default_encryption_invalid", err.Error(), false, "encryption.require_bucket_default_encryption")
			summary.SanitizedError = sanitizeRepositoryError(err.Error())
			return summary
		}
		addProbe("bucket_encryption", ValidationStateValid, "bucket default encryption is configured")
	}

	key := sentinelObjectKey(repo)
	body := []byte("mycel backup repository validation " + now.Format(time.RFC3339Nano) + "\n")
	put := &s3.PutObjectInput{Bucket: aws.String(repo.S3.Bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body)))}
	applyRepositoryEncryption(repo, put)
	if _, err := client.PutObject(ctx, put); err != nil {
		addProbe("sentinel_write", ValidationStateInvalid, err.Error())
		addBlocker("sentinel_write_failed", err.Error(), true, "s3")
		summary.SanitizedError = sanitizeRepositoryError(err.Error())
		return summary
	}
	addProbe("sentinel_write", ValidationStateValid, "sentinel object written")
	defer func() {
		_, _ = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(repo.S3.Bucket), Key: aws.String(key)})
	}()

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(repo.S3.Bucket), Key: aws.String(key)})
	if err != nil {
		addProbe("sentinel_head", ValidationStateInvalid, err.Error())
		addBlocker("sentinel_head_failed", err.Error(), true, "s3")
		summary.SanitizedError = sanitizeRepositoryError(err.Error())
		return summary
	}
	if head.ContentLength != nil && *head.ContentLength != int64(len(body)) {
		detail := fmt.Sprintf("sentinel size mismatch: got %d want %d", *head.ContentLength, len(body))
		addProbe("sentinel_head", ValidationStateInvalid, detail)
		addBlocker("sentinel_size_mismatch", detail, true, "s3")
		summary.SanitizedError = detail
		return summary
	}
	if err := validateRepositoryEncryptionEvidence(repo, head); err != nil {
		addProbe("encryption", ValidationStateInvalid, err.Error())
		addBlocker("encryption_validation_failed", err.Error(), false, "encryption")
		summary.SanitizedError = sanitizeRepositoryError(err.Error())
		return summary
	}
	addProbe("sentinel_head", ValidationStateValid, "sentinel object metadata verified")
	addProbe("encryption", ValidationStateValid, "server-side encryption evidence accepted")

	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(repo.S3.Bucket), Key: aws.String(key)})
	if err != nil {
		addProbe("sentinel_read", ValidationStateInvalid, err.Error())
		addBlocker("sentinel_read_failed", err.Error(), true, "s3")
		summary.SanitizedError = sanitizeRepositoryError(err.Error())
		return summary
	}
	read, err := io.ReadAll(out.Body)
	_ = out.Body.Close()
	if err != nil {
		addProbe("sentinel_read", ValidationStateInvalid, err.Error())
		addBlocker("sentinel_read_failed", err.Error(), true, "s3")
		summary.SanitizedError = sanitizeRepositoryError(err.Error())
		return summary
	}
	if !bytes.Equal(read, body) {
		detail := "sentinel object content mismatch"
		addProbe("sentinel_read", ValidationStateInvalid, detail)
		addBlocker("sentinel_content_mismatch", detail, true, "s3")
		summary.SanitizedError = detail
		return summary
	}
	addProbe("sentinel_read", ValidationStateValid, "sentinel object read back successfully")

	prefix := key
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		prefix = prefix[:i+1]
	}
	if _, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(repo.S3.Bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1)}); err != nil {
		addProbe("sentinel_list", ValidationStateInvalid, err.Error())
		addBlocker("sentinel_list_failed", err.Error(), true, "s3")
		summary.SanitizedError = sanitizeRepositoryError(err.Error())
		return summary
	}
	addProbe("sentinel_list", ValidationStateValid, "repository prefix list succeeded")
	return summary
}

var errUnsupportedCredentialReference = errors.New("unsupported credential reference")

func newS3RepositoryClient(ctx context.Context, repo BackupRepository) (*s3.Client, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{}
	if repo.S3.Region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(repo.S3.Region))
	}
	provider, err := credentialProviderForReference(repo.CredentialRef)
	if err != nil {
		return nil, err
	}
	if provider != nil {
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(provider))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, err
	}
	var tlsClient *http.Client
	if repo.TLSCARef.Scheme != "" {
		tlsClient, err = httpClientForTLSCAReference(repo.TLSCARef)
		if err != nil {
			return nil, err
		}
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if repo.S3.EndpointURI != "" {
			o.BaseEndpoint = aws.String(repo.S3.EndpointURI)
		}
		o.UsePathStyle = repo.S3.ForcePathStyle
		if tlsClient != nil {
			o.HTTPClient = tlsClient
		}
	}), nil
}

func credentialProviderForReference(ref SecretReference) (aws.CredentialsProvider, error) {
	switch ref.Scheme {
	case "", SecretSchemeAWSIRSA:
		// Use the AWS default chain. In EKS this resolves IRSA web identity credentials
		// without storing raw credentials in Mycel metadata.
		return nil, nil
	case SecretSchemeEnv:
		return staticCredentialsProviderFromEnv(ref)
	case SecretSchemeFileRef, SecretSchemeK8sSecret, SecretSchemeExternalSecret:
		return staticCredentialsProviderFromFileReference(ref)
	default:
		return nil, fmt.Errorf("%w: %s validation resolver is not available in this daemon", errUnsupportedCredentialReference, ref.Scheme)
	}
}

func staticCredentialsProviderFromEnv(ref SecretReference) (aws.CredentialsProvider, error) {
	accessName := firstNonEmptyString(ref.Keys["access_key_id"], ref.Keys["AWS_ACCESS_KEY_ID"])
	secretName := firstNonEmptyString(ref.Keys["secret_access_key"], ref.Keys["AWS_SECRET_ACCESS_KEY"])
	sessionName := firstNonEmptyString(ref.Keys["session_token"], ref.Keys["AWS_SESSION_TOKEN"])
	if accessName == "" || secretName == "" {
		return nil, fmt.Errorf("env credential reference requires access_key_id and secret_access_key names")
	}
	accessKey := os.Getenv(accessName)
	secretKey := os.Getenv(secretName)
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("env credential reference variables are not set")
	}
	sessionToken := ""
	if sessionName != "" {
		sessionToken = os.Getenv(sessionName)
	}
	return credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken), nil
}

func staticCredentialsProviderFromFileReference(ref SecretReference) (aws.CredentialsProvider, error) {
	accessKey, err := readSecretReferenceValue(ref, "access_key_id", false, "AWS_ACCESS_KEY_ID")
	if err != nil {
		return nil, err
	}
	secretKey, err := readSecretReferenceValue(ref, "secret_access_key", false, "AWS_SECRET_ACCESS_KEY")
	if err != nil {
		return nil, err
	}
	sessionToken, err := readSecretReferenceValue(ref, "session_token", false, "AWS_SESSION_TOKEN")
	if err != nil && !errors.Is(err, errOptionalSecretReferenceValueMissing) {
		return nil, err
	}
	return credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken), nil
}

var errOptionalSecretReferenceValueMissing = errors.New("optional secret reference value missing")

func readSecretReferenceValue(ref SecretReference, logicalName string, allowDirectPath bool, aliases ...string) (string, error) {
	key := firstNonEmptyString(ref.Keys[logicalName])
	for _, alias := range aliases {
		key = firstNonEmptyString(key, ref.Keys[alias])
	}
	if key == "" {
		if logicalName == "session_token" {
			return "", errOptionalSecretReferenceValueMissing
		}
		if !allowDirectPath {
			return "", fmt.Errorf("%s reference requires a file key", logicalName)
		}
	}
	value, err := readSecretReferenceFile(ref.Path, key, allowDirectPath)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(value)), nil
}

func readSecretReferenceFile(basePath, key string, allowDirectPath bool) ([]byte, error) {
	basePath = strings.TrimSpace(basePath)
	key = strings.TrimSpace(key)
	if basePath == "" {
		return nil, fmt.Errorf("secret reference path is required for projected file resolution")
	}
	if key == "" {
		if !allowDirectPath {
			return nil, fmt.Errorf("secret reference file key is required")
		}
		return os.ReadFile(basePath)
	}
	if filepath.IsAbs(key) {
		return nil, fmt.Errorf("secret reference file key must be relative")
	}
	cleanKey := filepath.Clean(key)
	if cleanKey == "." || strings.HasPrefix(cleanKey, ".."+string(filepath.Separator)) || cleanKey == ".." {
		return nil, fmt.Errorf("secret reference file key must stay within the referenced path")
	}
	stat, err := os.Stat(basePath)
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("secret reference path must be a directory when keys are used")
	}
	return os.ReadFile(filepath.Join(basePath, cleanKey))
}

func httpClientForTLSCAReference(ref SecretReference) (*http.Client, error) {
	pemText, err := readSecretReferenceValue(ref, "ca_bundle", true, "tls_ca", "ca.crt")
	if err != nil {
		return nil, fmt.Errorf("tls_ca_ref: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM([]byte(pemText)) {
		return nil, fmt.Errorf("tls_ca_ref does not contain PEM certificates")
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}}, nil
}

func sentinelObjectKey(repo BackupRepository) string {
	seed := repo.RepositoryID + ":" + time.Now().UTC().Format(time.RFC3339Nano)
	digest := sha256.Sum256([]byte(seed))
	name := "sentinel-" + hex.EncodeToString(digest[:8]) + ".txt"
	parts := []string{}
	if repo.S3.Prefix != "" {
		parts = append(parts, repo.S3.Prefix)
	}
	parts = append(parts, ".mycel-validation", name)
	return strings.Join(parts, "/")
}

func applyRepositoryEncryption(repo BackupRepository, put *s3.PutObjectInput) {
	switch repo.Encryption.Mode {
	case EncryptionModeSSEKMS:
		put.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		if repo.Encryption.KMSKeyID != "" {
			put.SSEKMSKeyId = aws.String(repo.Encryption.KMSKeyID)
		}
	case EncryptionModeSSES3:
		put.ServerSideEncryption = types.ServerSideEncryptionAes256
	}
}

func validateBucketDefaultEncryption(ctx context.Context, client *s3.Client, repo BackupRepository) error {
	out, err := client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(repo.S3.Bucket)})
	if err != nil {
		return err
	}
	if out.ServerSideEncryptionConfiguration == nil || len(out.ServerSideEncryptionConfiguration.Rules) == 0 {
		return fmt.Errorf("bucket default encryption is not configured")
	}
	if repo.Encryption.Mode == EncryptionModeSSEKMS || repo.Encryption.Mode == EncryptionModeSSES3 {
		want := types.ServerSideEncryptionAes256
		if repo.Encryption.Mode == EncryptionModeSSEKMS {
			want = types.ServerSideEncryptionAwsKms
		}
		for _, rule := range out.ServerSideEncryptionConfiguration.Rules {
			if rule.ApplyServerSideEncryptionByDefault != nil && rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm == want {
				return nil
			}
		}
		return fmt.Errorf("bucket default encryption does not include %s", want)
	}
	return nil
}

func validateRepositoryEncryptionEvidence(repo BackupRepository, head *s3.HeadObjectOutput) error {
	switch repo.Encryption.Mode {
	case EncryptionModeSSEKMS:
		if head.ServerSideEncryption != types.ServerSideEncryptionAwsKms {
			return fmt.Errorf("object is not encrypted with SSE-KMS")
		}
	case EncryptionModeSSES3:
		if head.ServerSideEncryption != types.ServerSideEncryptionAes256 {
			return fmt.Errorf("object is not encrypted with SSE-S3")
		}
	case EncryptionModeNone:
		return nil
	}
	return nil
}

var sensitiveErrorPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(access_key_id|aws_access_key_id|secret_access_key|aws_secret_access_key|session_token|aws_session_token|x-amz-security-token|x-amz-credential)=([^\s&]+)`),
	regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
	regexp.MustCompile(`ASIA[0-9A-Z]{16}`),
	regexp.MustCompile(`(?i)(https?://[^\s?]+)\?[^\s]+`),
}

func looksLikeRawCredential(value string) bool {
	for _, pattern := range sensitiveErrorPatterns[:3] {
		if pattern.MatchString(value) {
			return true
		}
	}
	return false
}

func sanitizeRepositoryError(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = sensitiveErrorPatterns[0].ReplaceAllString(value, `$1=<redacted>`)
	value = sensitiveErrorPatterns[1].ReplaceAllString(value, `<redacted-access-key>`)
	value = sensitiveErrorPatterns[2].ReplaceAllString(value, `<redacted-access-key>`)
	value = sensitiveErrorPatterns[3].ReplaceAllString(value, `$1?<redacted>`)
	return value
}
