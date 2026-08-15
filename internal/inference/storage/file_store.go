package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/filestore"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

var ErrInvalidInput = errors.New("invalid inference storage input")

const (
	inferenceDirName     = "inference"
	secretsDirName       = "secrets"
	credentialsDirName   = "credentials"
	packagesFileName     = "packages.json"
	endpointsFileName    = "endpoints.json"
	modelsFileName       = "models.json"
	capabilitiesFileName = "capabilities.json"
	vectorStoresFileName = "vector_stores.json"
	secretsFileName      = "secrets.json"
	credentialsFileName  = "credentials.json"
	profilesFileName     = "profiles.json"
	grantsFileName       = "credential_grants.json"
	policiesFileName     = "policies.json"
	decisionsFileName    = "policy_decisions.json"
	usageFileName        = "usage_events.json"
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

type globalManager struct {
	mu           sync.RWMutex
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
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	if err := readJSON(m.packagesPath(), &m.packages); err != nil {
		return err
	}
	if err := readJSON(m.endpointsPath(), &m.endpoints); err != nil {
		return err
	}
	if err := readJSON(m.modelsPath(), &m.models); err != nil {
		return err
	}
	if err := readJSON(m.capabilitiesPath(), &m.capabilities); err != nil {
		return err
	}
	if err := readJSON(m.vectorStoresPath(), &m.vectorStores); err != nil {
		return err
	}
	if err := readJSON(m.secretsPath(), &m.secrets); err != nil {
		return err
	}
	if err := readJSON(m.credentialsPath(), &m.credentials); err != nil {
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
	return item, writeJSON(m.packagesPath(), m.packages)
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
	return writeJSON(m.packagesPath(), m.packages)
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
	return item, writeJSON(m.endpointsPath(), m.endpoints)
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
	return writeJSON(m.endpointsPath(), m.endpoints)
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
	return item, writeJSON(m.modelsPath(), m.models)
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
	return writeJSON(m.modelsPath(), m.models)
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
	return item, writeJSON(m.capabilitiesPath(), m.capabilities)
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
	return writeJSON(m.capabilitiesPath(), m.capabilities)
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
	return item, writeJSON(m.vectorStoresPath(), m.vectorStores)
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
	return writeJSON(m.vectorStoresPath(), m.vectorStores)
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
	return item, writeJSON(m.secretsPath(), m.secrets)
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
	m.secrets.Secrets = deleteByID(m.secrets.Secrets, id, func(v domaininference.Secret) uuid.UUID { return v.ID })
	return writeJSON(m.secretsPath(), m.secrets)
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
	return item, writeJSON(m.credentialsPath(), m.credentials)
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
	return writeJSON(m.credentialsPath(), m.credentials)
}

type spaceManager struct {
	mu        sync.RWMutex
	location  string
	spaceID   string
	profiles  profilesState
	grants    grantsState
	policies  policiesState
	decisions decisionsState
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
	if err := os.MkdirAll(location, 0o755); err != nil {
		return err
	}
	if err := readJSON(m.profilesPath(), &m.profiles); err != nil {
		return err
	}
	if err := readJSON(m.grantsPath(), &m.grants); err != nil {
		return err
	}
	if err := readJSON(m.policiesPath(), &m.policies); err != nil {
		return err
	}
	if err := readJSON(m.decisionsPath(), &m.decisions); err != nil {
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
	return item, writeJSON(m.profilesPath(), m.profiles)
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
	return writeJSON(m.profilesPath(), m.profiles)
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
	return item, writeJSON(m.grantsPath(), m.grants)
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
	return writeJSON(m.grantsPath(), m.grants)
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
	return item, writeJSON(m.policiesPath(), m.policies)
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
	return writeJSON(m.policiesPath(), m.policies)
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
	return item, writeJSON(m.decisionsPath(), m.decisions)
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
	return writeJSON(m.decisionsPath(), m.decisions)
}

type usageLedger struct {
	mu       sync.RWMutex
	location string
	state    usageState
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
	if err := os.MkdirAll(location, 0o755); err != nil {
		return err
	}
	return readJSON(m.path(), &m.state)
}
func (m *usageLedger) path() string { return filepath.Join(m.location, usageFileName) }
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
			return item, writeJSON(m.path(), m.state)
		}
	}
	m.state.Events = append(m.state.Events, item)
	return item, writeJSON(m.path(), m.state)
}
func (m *usageLedger) ListUsageEvents(ctx context.Context) ([]domaininference.UsageEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]domaininference.UsageEvent(nil), m.state.Events...), nil
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

func readJSON(path string, target any) error {
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
	return json.Unmarshal(data, target)
}
func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return filestore.WriteFileAtomic(path, data, 0o600)
}
