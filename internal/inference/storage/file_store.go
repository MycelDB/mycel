package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/encryption"
	"github.com/myceldb/mycel/internal/fsperm"

	"github.com/google/uuid"

	"github.com/myceldb/mycel/internal/filestore"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

var (
	ErrInvalidInput        = errors.New("invalid inference storage input")
	ErrBudgetScopeNotFound = errors.New("inference budget scope not found")
)

const (
	inferenceDirName                 = "inference"
	secretsDirName                   = "secrets"
	credentialsDirName               = "credentials"
	packagesFileName                 = "packages.json"
	endpointsFileName                = "endpoints.json"
	modelsFileName                   = "models.json"
	capabilitiesFileName             = "capabilities.json"
	vectorStoresFileName             = "vector_stores.json"
	secretsFileName                  = "secrets.json"
	credentialsFileName              = "credentials.json"
	profilesFileName                 = "profiles.json"
	grantsFileName                   = "credential_grants.json"
	policiesFileName                 = "policies.json"
	decisionsFileName                = "policy_decisions.json"
	usageFileName                    = "usage_events.json"
	budgetScopesFileName             = "budget_scopes.json"
	inferenceSnapshotJournalFileName = "snapshot_restore_journal.json"
)

type packagesState struct {
	Packages []domaininference.InferencePackage `json:"packages"`
}
type endpointsState struct {
	Endpoints []domaininference.Endpoint `json:"endpoints"`
}
type modelsState struct {
	Models []domaininference.Model `json:"models"`
}
type capabilitiesState struct {
	Capabilities []domaininference.Capability `json:"capabilities"`
}
type vectorStoresState struct {
	VectorStores []domaininference.VectorStore `json:"vector_stores"`
}
type secretsState struct {
	Secrets []domaininference.Secret `json:"secrets"`
}
type credentialsState struct {
	Credentials []domaininference.Credential `json:"credentials"`
}
type profilesState struct {
	Profiles []domaininference.Profile `json:"profiles"`
}
type grantsState struct {
	Grants []domaininference.CredentialGrant `json:"credential_grants"`
}
type policiesState struct {
	Policies []domaininference.Policy `json:"policies"`
}
type decisionsState struct {
	Decisions []domaininference.PolicyDecision `json:"policy_decisions"`
}
type usageState struct {
	Events []domaininference.UsageEvent `json:"usage_events"`
}

type budgetScopesState struct {
	Scopes            []domaininference.BudgetScope `json:"budget_scopes"`
	HighWaterSequence uint64                        `json:"high_water_sequence,omitempty"`
}

type inferenceSnapshotRestoreJournal struct {
	Budgets budgetScopesState `json:"budgets"`
	Usage   usageState        `json:"usage"`
}

type globalManager struct {
	mu           sync.RWMutex
	encryption   *encryption.Service
	metaDir      string
	packages     packagesState
	endpoints    endpointsState
	models       modelsState
	capabilities capabilitiesState
	vectorStores vectorStoresState
	secrets      secretsState
	credentials  credentialsState
}

func (m *globalManager) Init(ctx context.Context, metaDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(metaDir) == "" {
		return fmt.Errorf("%w: metaDir is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metaDir = metaDir
	for _, dir := range []string{filepath.Join(metaDir, inferenceDirName), filepath.Join(metaDir, secretsDirName), filepath.Join(metaDir, credentialsDirName)} {
		if err := os.MkdirAll(dir, fsperm.SharedDir); err != nil {
			return err
		}
	}
	if err := m.readJSON(ctx, m.packagesPath(), &m.packages); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.endpointsPath(), &m.endpoints); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.modelsPath(), &m.models); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.capabilitiesPath(), &m.capabilities); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.vectorStoresPath(), &m.vectorStores); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.secretsPath(), &m.secrets); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.credentialsPath(), &m.credentials); err != nil {
		return err
	}
	return nil
}

func (m *globalManager) packagesPath() string {
	return filepath.Join(m.metaDir, inferenceDirName, packagesFileName)
}
func (m *globalManager) endpointsPath() string {
	return filepath.Join(m.metaDir, inferenceDirName, endpointsFileName)
}
func (m *globalManager) modelsPath() string {
	return filepath.Join(m.metaDir, inferenceDirName, modelsFileName)
}
func (m *globalManager) capabilitiesPath() string {
	return filepath.Join(m.metaDir, inferenceDirName, capabilitiesFileName)
}
func (m *globalManager) vectorStoresPath() string {
	return filepath.Join(m.metaDir, inferenceDirName, vectorStoresFileName)
}
func (m *globalManager) secretsPath() string {
	return filepath.Join(m.metaDir, secretsDirName, secretsFileName)
}
func (m *globalManager) credentialsPath() string {
	return filepath.Join(m.metaDir, credentialsDirName, credentialsFileName)
}

func (m *globalManager) UpsertPackage(ctx context.Context, item domaininference.InferencePackage) (domaininference.InferencePackage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.InstalledAt.IsZero() {
		item.InstalledAt = now
	}
	m.packages.Packages = upsert(m.packages.Packages, item, func(v domaininference.InferencePackage) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.packagesPath(), m.packages)
}
func (m *globalManager) ListPackages(ctx context.Context) ([]domaininference.InferencePackage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.InferencePackage(nil), m.packages.Packages...), nil
}
func (m *globalManager) DeletePackage(ctx context.Context, id domaininference.InferencePackageID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.packages.Packages = deleteByID(m.packages.Packages, id, func(v domaininference.InferencePackage) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.packagesPath(), m.packages)
}

func (m *globalManager) UpsertEndpoint(ctx context.Context, item domaininference.Endpoint) (domaininference.Endpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	m.endpoints.Endpoints = upsert(m.endpoints.Endpoints, item, func(v domaininference.Endpoint) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.endpointsPath(), m.endpoints)
}
func (m *globalManager) ListEndpoints(ctx context.Context) ([]domaininference.Endpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.Endpoint(nil), m.endpoints.Endpoints...), nil
}
func (m *globalManager) DeleteEndpoint(ctx context.Context, id domaininference.EndpointID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.endpoints.Endpoints = deleteByID(m.endpoints.Endpoints, id, func(v domaininference.Endpoint) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.endpointsPath(), m.endpoints)
}

func (m *globalManager) UpsertModel(ctx context.Context, item domaininference.Model) (domaininference.Model, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	m.models.Models = upsert(m.models.Models, item, func(v domaininference.Model) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.modelsPath(), m.models)
}
func (m *globalManager) ListModels(ctx context.Context) ([]domaininference.Model, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.Model(nil), m.models.Models...), nil
}
func (m *globalManager) DeleteModel(ctx context.Context, id domaininference.ModelID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.models.Models = deleteByID(m.models.Models, id, func(v domaininference.Model) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.modelsPath(), m.models)
}

func (m *globalManager) UpsertCapability(ctx context.Context, item domaininference.Capability) (domaininference.Capability, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	m.capabilities.Capabilities = upsert(m.capabilities.Capabilities, item, func(v domaininference.Capability) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.capabilitiesPath(), m.capabilities)
}
func (m *globalManager) ListCapabilities(ctx context.Context) ([]domaininference.Capability, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.Capability(nil), m.capabilities.Capabilities...), nil
}
func (m *globalManager) DeleteCapability(ctx context.Context, id domaininference.CapabilityID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.capabilities.Capabilities = deleteByID(m.capabilities.Capabilities, id, func(v domaininference.Capability) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.capabilitiesPath(), m.capabilities)
}

func (m *globalManager) UpsertVectorStore(ctx context.Context, item domaininference.VectorStore) (domaininference.VectorStore, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	m.vectorStores.VectorStores = upsert(m.vectorStores.VectorStores, item, func(v domaininference.VectorStore) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.vectorStoresPath(), m.vectorStores)
}
func (m *globalManager) ListVectorStores(ctx context.Context) ([]domaininference.VectorStore, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.VectorStore(nil), m.vectorStores.VectorStores...), nil
}
func (m *globalManager) DeleteVectorStore(ctx context.Context, id domaininference.VectorStoreID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.vectorStores.VectorStores = deleteByID(m.vectorStores.VectorStores, id, func(v domaininference.VectorStore) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.vectorStoresPath(), m.vectorStores)
}

func (m *globalManager) UpsertSecret(ctx context.Context, item domaininference.Secret) (domaininference.Secret, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	m.secrets.Secrets = upsert(m.secrets.Secrets, item, func(v domaininference.Secret) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.secretsPath(), m.secrets)
}
func (m *globalManager) ListSecrets(ctx context.Context) ([]domaininference.Secret, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.Secret(nil), m.secrets.Secrets...), nil
}
func (m *globalManager) DeleteSecret(ctx context.Context, id domaininference.SecretID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, credential := range m.credentials.Credentials {
		if credential.SecretID == id {
			return fmt.Errorf("%w: secret is referenced by credential %s", ErrInvalidInput, credential.ID)
		}
	}
	m.secrets.Secrets = deleteByID(m.secrets.Secrets, id, func(v domaininference.Secret) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.secretsPath(), m.secrets)
}

func (m *globalManager) UpsertCredential(ctx context.Context, item domaininference.Credential) (domaininference.Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	m.credentials.Credentials = upsert(m.credentials.Credentials, item, func(v domaininference.Credential) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.credentialsPath(), m.credentials)
}
func (m *globalManager) ListCredentials(ctx context.Context) ([]domaininference.Credential, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.Credential(nil), m.credentials.Credentials...), nil
}
func (m *globalManager) DeleteCredential(ctx context.Context, id domaininference.CredentialID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.credentials.Credentials = deleteByID(m.credentials.Credentials, id, func(v domaininference.Credential) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.credentialsPath(), m.credentials)
}

type spaceManager struct {
	mu         sync.RWMutex
	encryption *encryption.Service
	location   string
	spaceID    string
	profiles   profilesState
	grants     grantsState
	policies   policiesState
	decisions  decisionsState
}

func (m *spaceManager) Init(ctx context.Context, location string, spaceID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(location) == "" {
		return fmt.Errorf("%w: location is required", ErrInvalidInput)
	}
	if strings.TrimSpace(spaceID) == "" {
		return fmt.Errorf("%w: spaceID is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.location = location
	m.spaceID = spaceID
	if err := os.MkdirAll(location, fsperm.SharedDir); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.profilesPath(), &m.profiles); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.grantsPath(), &m.grants); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.policiesPath(), &m.policies); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.decisionsPath(), &m.decisions); err != nil {
		return err
	}
	return nil
}
func (m *spaceManager) profilesPath() string  { return filepath.Join(m.location, profilesFileName) }
func (m *spaceManager) grantsPath() string    { return filepath.Join(m.location, grantsFileName) }
func (m *spaceManager) policiesPath() string  { return filepath.Join(m.location, policiesFileName) }
func (m *spaceManager) decisionsPath() string { return filepath.Join(m.location, decisionsFileName) }

func (m *spaceManager) UpsertProfile(ctx context.Context, item domaininference.Profile) (domaininference.Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.SpaceID == "" {
		item.SpaceID = m.spaceID
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	m.profiles.Profiles = upsert(m.profiles.Profiles, item, func(v domaininference.Profile) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.profilesPath(), m.profiles)
}
func (m *spaceManager) ListProfiles(ctx context.Context) ([]domaininference.Profile, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.Profile(nil), m.profiles.Profiles...), nil
}
func (m *spaceManager) DeleteProfile(ctx context.Context, id domaininference.ProfileID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.profiles.Profiles = deleteByID(m.profiles.Profiles, id, func(v domaininference.Profile) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.profilesPath(), m.profiles)
}

func (m *spaceManager) DeleteCachedProfile(ctx context.Context, id domaininference.ProfileID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.profiles.Profiles = deleteByID(m.profiles.Profiles, id, func(v domaininference.Profile) uuid.UUID { return v.ID })
	return nil
}

func (m *spaceManager) UpsertCredentialGrant(ctx context.Context, item domaininference.CredentialGrant) (domaininference.CredentialGrant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.SpaceID == "" {
		item.SpaceID = m.spaceID
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	m.grants.Grants = upsert(m.grants.Grants, item, func(v domaininference.CredentialGrant) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.grantsPath(), m.grants)
}
func (m *spaceManager) ListCredentialGrants(ctx context.Context) ([]domaininference.CredentialGrant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.CredentialGrant(nil), m.grants.Grants...), nil
}
func (m *spaceManager) DeleteCredentialGrant(ctx context.Context, id domaininference.CredentialGrantID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.grants.Grants = deleteByID(m.grants.Grants, id, func(v domaininference.CredentialGrant) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.grantsPath(), m.grants)
}

func (m *spaceManager) UpsertPolicy(ctx context.Context, item domaininference.Policy) (domaininference.Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.SpaceID == "" {
		item.SpaceID = m.spaceID
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	m.policies.Policies = upsert(m.policies.Policies, item, func(v domaininference.Policy) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.policiesPath(), m.policies)
}
func (m *spaceManager) ListPolicies(ctx context.Context) ([]domaininference.Policy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.Policy(nil), m.policies.Policies...), nil
}
func (m *spaceManager) DeletePolicy(ctx context.Context, id domaininference.PolicyID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.policies.Policies = deleteByID(m.policies.Policies, id, func(v domaininference.Policy) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.policiesPath(), m.policies)
}

func (m *spaceManager) UpsertPolicyDecision(ctx context.Context, item domaininference.PolicyDecision) (domaininference.PolicyDecision, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.SpaceID == "" {
		item.SpaceID = m.spaceID
	}
	if item.DecidedAt.IsZero() {
		item.DecidedAt = time.Now().UTC()
	}
	m.decisions.Decisions = upsert(m.decisions.Decisions, item, func(v domaininference.PolicyDecision) uuid.UUID { return v.ID })
	return item, m.writeJSON(ctx, m.decisionsPath(), m.decisions)
}
func (m *spaceManager) ListPolicyDecisions(ctx context.Context) ([]domaininference.PolicyDecision, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.PolicyDecision(nil), m.decisions.Decisions...), nil
}
func (m *spaceManager) DeletePolicyDecision(ctx context.Context, id domaininference.PolicyDecisionID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.decisions.Decisions = deleteByID(m.decisions.Decisions, id, func(v domaininference.PolicyDecision) uuid.UUID { return v.ID })
	return m.writeJSON(ctx, m.decisionsPath(), m.decisions)
}

type usageLedger struct {
	mu            sync.RWMutex
	encryption    *encryption.Service
	location      string
	state         usageState
	budgets       budgetScopesState
	writeOverride func(context.Context, string, any) error
}

func (m *usageLedger) Init(ctx context.Context, location string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(location) == "" {
		return fmt.Errorf("%w: location is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.location = location
	if err := os.MkdirAll(location, fsperm.SharedDir); err != nil {
		return err
	}
	if err := m.readJSON(ctx, m.path(), &m.state); err != nil {
		return err
	}
	var budgets budgetScopesState
	if err := m.readJSON(ctx, m.budgetScopesPath(), &budgets); err != nil {
		return err
	}
	migrated, changed, err := migrateLegacyBudgetScopeSequences(budgets)
	if err != nil {
		return err
	}
	if changed {
		if err := m.writeJSON(ctx, m.budgetScopesPath(), migrated); err != nil {
			return err
		}
	}
	m.budgets = migrated
	if err := m.recoverInferenceSnapshotRestore(ctx); err != nil {
		return err
	}
	return nil
}
func (m *usageLedger) path() string { return filepath.Join(m.location, usageFileName) }
func (m *usageLedger) budgetScopesPath() string {
	return filepath.Join(m.location, budgetScopesFileName)
}
func (m *usageLedger) snapshotJournalPath() string {
	return filepath.Join(m.location, inferenceSnapshotJournalFileName)
}
func (m *usageLedger) AppendUsageEvent(ctx context.Context, item domaininference.UsageEvent) (domaininference.UsageEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return item, err
	}
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.StartedAt.IsZero() {
		item.StartedAt = time.Now().UTC()
	}
	for i, existing := range m.state.Events {
		if existing.ID == item.ID {
			m.state.Events[i] = item
			return item, m.writeJSON(ctx, m.path(), m.state)
		}
	}
	m.state.Events = append(m.state.Events, item)
	return item, m.writeJSON(ctx, m.path(), m.state)
}
func (m *usageLedger) ListUsageEvents(ctx context.Context) ([]domaininference.UsageEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.UsageEvent(nil), m.state.Events...), nil
}

func (m *usageLedger) ReplaceUsageEvents(ctx context.Context, events []domaininference.UsageEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	replaced := usageState{Events: make([]domaininference.UsageEvent, 0, len(events))}
	seen := make(map[uuid.UUID]struct{}, len(events))
	for _, event := range events {
		if event.ID == uuid.Nil {
			return fmt.Errorf("invalid replacement usage event")
		}
		if _, exists := seen[event.ID]; exists {
			return fmt.Errorf("duplicate replacement usage event")
		}
		seen[event.ID] = struct{}{}
		replaced.Events = append(replaced.Events, event)
	}
	if err := m.writeJSON(ctx, m.path(), replaced); err != nil {
		return err
	}
	m.state = replaced
	return nil
}

func (m *usageLedger) UpsertBudgetScope(ctx context.Context, scope domaininference.BudgetScope) (domaininference.BudgetScope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return scope, err
	}
	if strings.TrimSpace(string(scope.ID)) == "" {
		return scope, fmt.Errorf("%w: budget scope id is required", ErrInvalidInput)
	}
	scope = cloneBudgetScope(scope)
	next := cloneBudgetScopesState(m.budgets)
	if scope.ScopeSequence > next.HighWaterSequence {
		next.HighWaterSequence = scope.ScopeSequence
	}
	replaced := false
	for i, existing := range next.Scopes {
		if existing.ID == scope.ID {
			next.Scopes[i] = scope
			replaced = true
			break
		}
	}
	if !replaced {
		next.Scopes = append(next.Scopes, scope)
	}
	if err := m.writeJSON(ctx, m.budgetScopesPath(), next); err != nil {
		return domaininference.BudgetScope{}, err
	}
	m.budgets = next
	return cloneBudgetScope(scope), nil
}

func (m *usageLedger) GetBudgetScope(ctx context.Context, id domaininference.BudgetScopeID) (domaininference.BudgetScope, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return domaininference.BudgetScope{}, err
	}
	for _, scope := range m.budgets.Scopes {
		if scope.ID == id {
			return cloneBudgetScope(scope), nil
		}
	}
	return domaininference.BudgetScope{}, ErrBudgetScopeNotFound
}

func (m *usageLedger) ListBudgetScopes(ctx context.Context) ([]domaininference.BudgetScope, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]domaininference.BudgetScope, 0, len(m.budgets.Scopes))
	for _, scope := range m.budgets.Scopes {
		out = append(out, cloneBudgetScope(scope))
	}
	return out, nil
}

func (m *usageLedger) BudgetScopeHighWater(ctx context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return m.budgets.HighWaterSequence, nil
}

func (m *usageLedger) DeleteBudgetScope(ctx context.Context, id domaininference.BudgetScopeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	next := cloneBudgetScopesState(m.budgets)
	out := make([]domaininference.BudgetScope, 0, len(next.Scopes))
	for _, scope := range next.Scopes {
		if scope.ID != id {
			out = append(out, scope)
		}
	}
	next.Scopes = out
	if err := m.writeJSON(ctx, m.budgetScopesPath(), next); err != nil {
		return err
	}
	m.budgets = next
	return nil
}

func (m *usageLedger) ReplaceBudgetScopes(ctx context.Context, scopes []domaininference.BudgetScope, highWater uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	replaced, _, err := migrateLegacyBudgetScopeSequences(budgetScopesState{HighWaterSequence: highWater, Scopes: scopes})
	if err != nil {
		return err
	}
	seen := make(map[domaininference.BudgetScopeID]struct{}, len(replaced.Scopes))
	for _, scope := range replaced.Scopes {
		if scope.ID == "" || scope.ScopeSequence == 0 || scope.ScopeSequence > replaced.HighWaterSequence {
			return fmt.Errorf("invalid replacement budget scope")
		}
		if _, exists := seen[scope.ID]; exists {
			return fmt.Errorf("duplicate replacement budget scope")
		}
		seen[scope.ID] = struct{}{}
	}
	if err := m.writeJSON(ctx, m.budgetScopesPath(), replaced); err != nil {
		return err
	}
	m.budgets = replaced
	return nil
}

func prepareInferenceSnapshot(scopes []domaininference.BudgetScope, highWater uint64, events []domaininference.UsageEvent) (budgetScopesState, usageState, error) {
	replacedBudgets, _, err := migrateLegacyBudgetScopeSequences(budgetScopesState{HighWaterSequence: highWater, Scopes: scopes})
	if err != nil {
		return budgetScopesState{}, usageState{}, err
	}
	seenScopes := make(map[domaininference.BudgetScopeID]struct{}, len(replacedBudgets.Scopes))
	for _, scope := range replacedBudgets.Scopes {
		if scope.ID == "" || scope.ScopeSequence == 0 || scope.ScopeSequence > replacedBudgets.HighWaterSequence {
			return budgetScopesState{}, usageState{}, fmt.Errorf("invalid replacement budget scope")
		}
		if _, exists := seenScopes[scope.ID]; exists {
			return budgetScopesState{}, usageState{}, fmt.Errorf("duplicate replacement budget scope")
		}
		seenScopes[scope.ID] = struct{}{}
	}
	replacedUsage := usageState{Events: make([]domaininference.UsageEvent, 0, len(events))}
	seenEvents := make(map[uuid.UUID]struct{}, len(events))
	for _, event := range events {
		if event.ID == uuid.Nil {
			return budgetScopesState{}, usageState{}, fmt.Errorf("invalid replacement usage event")
		}
		if _, exists := seenEvents[event.ID]; exists {
			return budgetScopesState{}, usageState{}, fmt.Errorf("duplicate replacement usage event")
		}
		seenEvents[event.ID] = struct{}{}
		replacedUsage.Events = append(replacedUsage.Events, event)
	}
	return replacedBudgets, replacedUsage, nil
}

func (m *usageLedger) ReplaceInferenceSnapshot(ctx context.Context, scopes []domaininference.BudgetScope, highWater uint64, events []domaininference.UsageEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	budgets, usage, err := prepareInferenceSnapshot(scopes, highWater, events)
	if err != nil {
		return err
	}
	journal := inferenceSnapshotRestoreJournal{Budgets: budgets, Usage: usage}
	if err := m.writeJSON(ctx, m.snapshotJournalPath(), journal); err != nil {
		return err
	}
	if err := m.writeJSON(ctx, m.budgetScopesPath(), budgets); err != nil {
		return err
	}
	if err := m.writeJSON(ctx, m.path(), usage); err != nil {
		return err
	}
	if err := os.Remove(m.snapshotJournalPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	m.budgets = budgets
	m.state = usage
	return nil
}

func (m *usageLedger) recoverInferenceSnapshotRestore(ctx context.Context) error {
	if _, err := os.Stat(m.snapshotJournalPath()); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	var journal inferenceSnapshotRestoreJournal
	if err := m.readJSON(ctx, m.snapshotJournalPath(), &journal); err != nil {
		return err
	}
	budgets, usage, err := prepareInferenceSnapshot(journal.Budgets.Scopes, journal.Budgets.HighWaterSequence, journal.Usage.Events)
	if err != nil {
		return err
	}
	if err := m.writeJSON(ctx, m.budgetScopesPath(), budgets); err != nil {
		return err
	}
	if err := m.writeJSON(ctx, m.path(), usage); err != nil {
		return err
	}
	if err := os.Remove(m.snapshotJournalPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	m.budgets = budgets
	m.state = usage
	return nil
}

func cloneBudgetScope(scope domaininference.BudgetScope) domaininference.BudgetScope {
	scope.Attempts = append([]domaininference.BudgetAttempt(nil), scope.Attempts...)
	scope.AttemptEvents = append([]domaininference.BudgetAttemptEvent(nil), scope.AttemptEvents...)
	return scope
}

func migrateLegacyBudgetScopeSequences(state budgetScopesState) (budgetScopesState, bool, error) {
	out := cloneBudgetScopesState(state)
	highWater := out.HighWaterSequence
	legacy := make([]int, 0)
	seen := make(map[uint64]struct{}, len(out.Scopes))
	for i, scope := range out.Scopes {
		if scope.ScopeSequence == 0 {
			legacy = append(legacy, i)
			continue
		}
		if _, duplicate := seen[scope.ScopeSequence]; duplicate {
			return budgetScopesState{}, false, fmt.Errorf("duplicate budget scope sequence %d", scope.ScopeSequence)
		}
		seen[scope.ScopeSequence] = struct{}{}
		if scope.ScopeSequence > highWater {
			highWater = scope.ScopeSequence
		}
	}
	sort.Slice(legacy, func(i, j int) bool {
		left, right := out.Scopes[legacy[i]], out.Scopes[legacy[j]]
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.ID < right.ID
	})
	for _, index := range legacy {
		if highWater == ^uint64(0) {
			return budgetScopesState{}, false, fmt.Errorf("budget scope sequence is exhausted")
		}
		highWater++
		out.Scopes[index].ScopeSequence = highWater
	}
	changed := len(legacy) != 0 || out.HighWaterSequence != highWater
	out.HighWaterSequence = highWater
	return out, changed, nil
}

func cloneBudgetScopesState(state budgetScopesState) budgetScopesState {
	out := budgetScopesState{HighWaterSequence: state.HighWaterSequence, Scopes: make([]domaininference.BudgetScope, 0, len(state.Scopes))}
	for _, scope := range state.Scopes {
		out.Scopes = append(out.Scopes, cloneBudgetScope(scope))
	}
	return out
}

func upsert[T any](items []T, item T, id func(T) uuid.UUID) []T {
	itemID := id(item)
	for i, existing := range items {
		if id(existing) == itemID {
			items[i] = item
			return items
		}
	}
	return append(items, item)
}
func deleteByID[T any](items []T, id uuid.UUID, getID func(T) uuid.UUID) []T {
	out := items[:0]
	for _, item := range items {
		if getID(item) != id {
			out = append(out, item)
		}
	}
	return out
}

func (m *globalManager) readJSON(ctx context.Context, path string, target any) error {
	return readJSONEncrypted(ctx, path, target, m.encryption)
}

func (m *globalManager) writeJSON(ctx context.Context, path string, value any) error {
	return writeJSONEncrypted(ctx, path, value, m.encryption)
}

func (m *spaceManager) readJSON(ctx context.Context, path string, target any) error {
	return readJSONEncrypted(ctx, path, target, m.encryption)
}

func (m *spaceManager) writeJSON(ctx context.Context, path string, value any) error {
	return writeJSONEncrypted(ctx, path, value, m.encryption)
}

func (m *usageLedger) readJSON(ctx context.Context, path string, target any) error {
	return readJSONEncrypted(ctx, path, target, m.encryption)
}

func (m *usageLedger) writeJSON(ctx context.Context, path string, value any) error {
	if m.writeOverride != nil {
		return m.writeOverride(ctx, path, value)
	}
	return writeJSONEncrypted(ctx, path, value, m.encryption)
}

func readJSON(path string, target any) error {
	return readJSONEncrypted(context.Background(), path, target, nil)
}

func writeJSON(path string, value any) error {
	return writeJSONEncrypted(context.Background(), path, value, nil)
}

func readJSONEncrypted(ctx context.Context, path string, target any, enc *encryption.Service) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	if enc != nil && enc.Enabled() {
		data, err = enc.DecryptRecord(ctx, data, []byte("inference-store-json:v1:path="+filepath.ToSlash(path)))
		if err != nil {
			return err
		}
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	return json.Unmarshal(data, target)
}

func writeJSONEncrypted(ctx context.Context, path string, value any, enc *encryption.Service) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if enc != nil && enc.Enabled() {
		data, err = enc.EncryptRecord(ctx, data, []byte("inference-store-json:v1:path="+filepath.ToSlash(path)))
		if err != nil {
			return err
		}
	}
	return filestore.WriteFileAtomic(path, data, fsperm.PrivateFile)
}
