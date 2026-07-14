package semantic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/filestore"
	"github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const (
	inferenceDirName          = "inference"
	secretsDirName            = "secrets"
	credentialsDirName        = "credentials"
	packagesFileName          = "packages.json"
	modelEndpointsFileName    = "model_endpoints.json"
	modelsFileName            = "models.json"
	modelEndpointCapsFileName = "model_endpoint_capabilities.json"
	vectorStoresFileName      = "vector_stores.json"
	secretsFileName           = "secrets.json"
	credentialsFileName       = "credentials.json"
	semanticIndexesFileName   = "indexes.json"
	credentialGrantsFileName  = "credential_grants.json"
	inferencePoliciesFileName = "inference_policies.json"
	dirtyQueueFileName        = "dirty_queue.json"
	indexStateFileName        = "index_state.json"
	policyDecisionsFileName   = "policy_decisions.json"
	maintenanceDirName        = "maintenance"
	graphDirtyEventsDirName   = "dirty"
	graphDirtyEventsFileName  = "graph-dirty-000001.ksem"
	workStateDirName          = "work"
	workStateFileName         = "state.json"
	workEventsFileName        = "work-000001.ksem"
	checkpointsFileName       = "checkpoints.json"
	defaultVectorStoreKey     = "mycel-file"
	defaultVectorStoreName    = "Mycel embedded file vector store"
)

type packagesState struct {
	Packages []domainsemantic.InferencePackage `json:"packages"`
}

type modelEndpointsState struct {
	ModelEndpoints []domainsemantic.ModelEndpoint `json:"model_endpoints"`
}

type modelsState struct {
	Models []domainsemantic.InferenceModel `json:"models"`
}

type modelEndpointCapabilitiesState struct {
	Capabilities []domainsemantic.ModelEndpointCapability `json:"capabilities"`
}

type vectorStoresState struct {
	VectorStores []domainsemantic.VectorStoreBackend `json:"vector_stores"`
}

type secretsState struct {
	Secrets []domainsemantic.Secret `json:"secrets"`
}

type credentialsState struct {
	Credentials []domainsemantic.InferenceCredential `json:"credentials"`
}

type semanticIndexesState struct {
	Indexes []domainsemantic.SemanticIndex `json:"indexes"`
}

type credentialGrantsState struct {
	Grants []domainsemantic.CredentialGrant `json:"grants"`
}

type inferencePoliciesState struct {
	Policies []domainsemantic.InferencePolicy `json:"policies"`
}

type dirtyQueueState struct {
	Items []domainsemantic.SemanticDirtyWorkItem `json:"items"`
}

type maintenanceCheckpointState struct {
	Checkpoints []MaintenanceCheckpoint `json:"checkpoints"`
}

type workLogRecord struct {
	Kind string                               `json:"kind"`
	At   time.Time                            `json:"at"`
	Item domainsemantic.SemanticDirtyWorkItem `json:"item"`
}

type indexStatesState struct {
	States []domainsemantic.SemanticIndexState `json:"states"`
}

type policyDecisionsState struct {
	Decisions []domainsemantic.PolicyDecision `json:"decisions"`
}

type globalManager struct {
	mu           sync.RWMutex
	metaDir      string
	packages     packagesState
	endpoints    modelEndpointsState
	models       modelsState
	capabilities modelEndpointCapabilitiesState
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
	if err := os.MkdirAll(filepath.Join(metaDir, inferenceDirName), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(metaDir, secretsDirName), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(metaDir, credentialsDirName), 0o755); err != nil {
		return err
	}
	if err := loadJSON(m.path(packagesFileName), &m.packages, packagesState{Packages: []domainsemantic.InferencePackage{}}); err != nil {
		return err
	}
	if err := loadJSON(m.path(modelEndpointsFileName), &m.endpoints, modelEndpointsState{ModelEndpoints: []domainsemantic.ModelEndpoint{}}); err != nil {
		return err
	}
	if err := loadJSON(m.path(modelsFileName), &m.models, modelsState{Models: []domainsemantic.InferenceModel{}}); err != nil {
		return err
	}
	if err := loadJSON(m.path(modelEndpointCapsFileName), &m.capabilities, modelEndpointCapabilitiesState{Capabilities: []domainsemantic.ModelEndpointCapability{}}); err != nil {
		return err
	}
	if err := loadJSON(m.path(vectorStoresFileName), &m.vectorStores, vectorStoresState{VectorStores: []domainsemantic.VectorStoreBackend{}}); err != nil {
		return err
	}
	if err := loadJSON(m.path(secretsFileName), &m.secrets, secretsState{Secrets: []domainsemantic.Secret{}}); err != nil {
		return err
	}
	return loadJSON(m.path(credentialsFileName), &m.credentials, credentialsState{Credentials: []domainsemantic.InferenceCredential{}})
}

func (m *globalManager) EnsureDefaultVectorStore(ctx context.Context) (domainsemantic.VectorStoreBackend, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.VectorStoreBackend{}, err
	}
	m.mu.RLock()
	for _, existing := range m.vectorStores.VectorStores {
		if normalizeKey(existing.Key) == defaultVectorStoreKey {
			m.mu.RUnlock()
			return existing, nil
		}
	}
	m.mu.RUnlock()
	return m.UpsertVectorStore(ctx, domainsemantic.VectorStoreBackend{
		Key:          defaultVectorStoreKey,
		Name:         defaultVectorStoreName,
		Type:         domainsemantic.VectorStoreMycelFile,
		Config:       map[string]any{},
		PrivacyClass: domainsemantic.PrivacyClassLocalOnly,
		Enabled:      true,
	})
}

func (m *globalManager) UpsertPackage(ctx context.Context, pkg domainsemantic.InferencePackage) (domainsemantic.InferencePackage, error) {
	if err := validatePackage(ctx, pkg); err != nil {
		return domainsemantic.InferencePackage{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	pkg.Name = normalizeKey(pkg.Name)
	pkg.Version = strings.TrimSpace(pkg.Version)
	if pkg.ID == uuid.Nil {
		pkg.ID = newID()
	}
	if pkg.InstalledAt.IsZero() {
		pkg.InstalledAt = now
	}
	for i, existing := range m.packages.Packages {
		if normalizeKey(existing.Name) == pkg.Name && strings.TrimSpace(existing.Version) == pkg.Version {
			pkg.ID = existing.ID
			m.packages.Packages[i] = pkg
			return pkg, m.persistLocked(m.path(packagesFileName), m.packages)
		}
	}
	m.packages.Packages = append(m.packages.Packages, pkg)
	return pkg, m.persistLocked(m.path(packagesFileName), m.packages)
}

func (m *globalManager) ListPackages(ctx context.Context) ([]domainsemantic.InferencePackage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.InferencePackage(nil), m.packages.Packages...), nil
}

func (m *globalManager) UpsertModelEndpoint(ctx context.Context, endpoint domainsemantic.ModelEndpoint) (domainsemantic.ModelEndpoint, error) {
	if err := validateModelEndpoint(ctx, endpoint); err != nil {
		return domainsemantic.ModelEndpoint{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	endpoint.Key = normalizeKey(endpoint.Key)
	if endpoint.Name == "" {
		endpoint.Name = endpoint.Key
	}
	if endpoint.ID == uuid.Nil {
		endpoint.ID = newID()
	}
	if endpoint.CreatedAt.IsZero() {
		endpoint.CreatedAt = now
	}
	endpoint.UpdatedAt = now
	for i, existing := range m.endpoints.ModelEndpoints {
		if normalizeKey(existing.Key) == endpoint.Key {
			endpoint.ID = existing.ID
			endpoint.CreatedAt = existing.CreatedAt
			m.endpoints.ModelEndpoints[i] = endpoint
			return endpoint, m.persistLocked(m.path(modelEndpointsFileName), m.endpoints)
		}
	}
	m.endpoints.ModelEndpoints = append(m.endpoints.ModelEndpoints, endpoint)
	return endpoint, m.persistLocked(m.path(modelEndpointsFileName), m.endpoints)
}

func (m *globalManager) ListModelEndpoints(ctx context.Context) ([]domainsemantic.ModelEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.ModelEndpoint(nil), m.endpoints.ModelEndpoints...), nil
}

func (m *globalManager) UpsertModel(ctx context.Context, model domainsemantic.InferenceModel) (domainsemantic.InferenceModel, error) {
	if err := validateModel(ctx, model); err != nil {
		return domainsemantic.InferenceModel{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	model.Key = normalizeKey(model.Key)
	if strings.TrimSpace(model.ModelName) == "" {
		model.ModelName = model.Key
	}
	if model.ID == uuid.Nil {
		model.ID = newID()
	}
	if model.CreatedAt.IsZero() {
		model.CreatedAt = now
	}
	model.UpdatedAt = now
	for i, existing := range m.models.Models {
		if normalizeKey(existing.Key) == model.Key {
			model.ID = existing.ID
			model.CreatedAt = existing.CreatedAt
			m.models.Models[i] = model
			return model, m.persistLocked(m.path(modelsFileName), m.models)
		}
	}
	m.models.Models = append(m.models.Models, model)
	return model, m.persistLocked(m.path(modelsFileName), m.models)
}

func (m *globalManager) ListModels(ctx context.Context) ([]domainsemantic.InferenceModel, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.InferenceModel(nil), m.models.Models...), nil
}

func (m *globalManager) UpsertModelEndpointCapability(ctx context.Context, capability domainsemantic.ModelEndpointCapability) (domainsemantic.ModelEndpointCapability, error) {
	if err := validateCapability(ctx, capability); err != nil {
		return domainsemantic.ModelEndpointCapability{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	if capability.ID == uuid.Nil {
		capability.ID = newID()
	}
	if capability.CreatedAt.IsZero() {
		capability.CreatedAt = now
	}
	capability.UpdatedAt = now
	for i, existing := range m.capabilities.Capabilities {
		if existing.ModelEndpointID == capability.ModelEndpointID && existing.ModelID == capability.ModelID && existing.Operation == capability.Operation {
			capability.ID = existing.ID
			capability.CreatedAt = existing.CreatedAt
			m.capabilities.Capabilities[i] = capability
			return capability, m.persistLocked(m.path(modelEndpointCapsFileName), m.capabilities)
		}
	}
	m.capabilities.Capabilities = append(m.capabilities.Capabilities, capability)
	return capability, m.persistLocked(m.path(modelEndpointCapsFileName), m.capabilities)
}

func (m *globalManager) ListModelEndpointCapabilities(ctx context.Context) ([]domainsemantic.ModelEndpointCapability, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.ModelEndpointCapability(nil), m.capabilities.Capabilities...), nil
}

func (m *globalManager) UpsertVectorStore(ctx context.Context, vectorStore domainsemantic.VectorStoreBackend) (domainsemantic.VectorStoreBackend, error) {
	if err := validateVectorStore(ctx, vectorStore); err != nil {
		return domainsemantic.VectorStoreBackend{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	vectorStore.Key = normalizeKey(vectorStore.Key)
	if vectorStore.Name == "" {
		vectorStore.Name = vectorStore.Key
	}
	if vectorStore.ID == uuid.Nil {
		vectorStore.ID = newID()
	}
	if vectorStore.CreatedAt.IsZero() {
		vectorStore.CreatedAt = now
	}
	vectorStore.UpdatedAt = now
	for i, existing := range m.vectorStores.VectorStores {
		if normalizeKey(existing.Key) == vectorStore.Key {
			vectorStore.ID = existing.ID
			vectorStore.CreatedAt = existing.CreatedAt
			m.vectorStores.VectorStores[i] = vectorStore
			return vectorStore, m.persistLocked(m.path(vectorStoresFileName), m.vectorStores)
		}
	}
	m.vectorStores.VectorStores = append(m.vectorStores.VectorStores, vectorStore)
	return vectorStore, m.persistLocked(m.path(vectorStoresFileName), m.vectorStores)
}

func (m *globalManager) ListVectorStores(ctx context.Context) ([]domainsemantic.VectorStoreBackend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.VectorStoreBackend(nil), m.vectorStores.VectorStores...), nil
}

func (m *globalManager) UpsertSecret(ctx context.Context, secret domainsemantic.Secret) (domainsemantic.Secret, error) {
	if err := validateSecret(ctx, secret); err != nil {
		return domainsemantic.Secret{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	if secret.ID == uuid.Nil {
		secret.ID = newID()
	}
	if secret.CreatedAt.IsZero() {
		secret.CreatedAt = now
	}
	secret.UpdatedAt = now
	for i, existing := range m.secrets.Secrets {
		if existing.ID == secret.ID {
			secret.CreatedAt = existing.CreatedAt
			m.secrets.Secrets[i] = secret
			return secret, m.persistLocked(m.path(secretsFileName), m.secrets)
		}
	}
	m.secrets.Secrets = append(m.secrets.Secrets, secret)
	return secret, m.persistLocked(m.path(secretsFileName), m.secrets)
}

func (m *globalManager) ListSecrets(ctx context.Context) ([]domainsemantic.Secret, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.Secret(nil), m.secrets.Secrets...), nil
}

func (m *globalManager) UpsertCredential(ctx context.Context, credential domainsemantic.InferenceCredential) (domainsemantic.InferenceCredential, error) {
	if err := validateCredential(ctx, credential); err != nil {
		return domainsemantic.InferenceCredential{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	credential.Key = normalizeKey(credential.Key)
	if credential.Name == "" {
		credential.Name = credential.Key
	}
	if credential.Status == "" {
		credential.Status = domainsemantic.CredentialStatusActive
	}
	if credential.ID == uuid.Nil {
		credential.ID = newID()
	}
	if credential.CreatedAt.IsZero() {
		credential.CreatedAt = now
	}
	credential.UpdatedAt = now
	for i, existing := range m.credentials.Credentials {
		if normalizeKey(existing.Key) == credential.Key {
			credential.ID = existing.ID
			credential.CreatedAt = existing.CreatedAt
			m.credentials.Credentials[i] = credential
			return credential, m.persistLocked(m.path(credentialsFileName), m.credentials)
		}
	}
	m.credentials.Credentials = append(m.credentials.Credentials, credential)
	return credential, m.persistLocked(m.path(credentialsFileName), m.credentials)
}

func (m *globalManager) ListCredentials(ctx context.Context) ([]domainsemantic.InferenceCredential, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.InferenceCredential(nil), m.credentials.Credentials...), nil
}

func (m *globalManager) DeleteModelEndpoint(ctx context.Context, id domainsemantic.ModelEndpointID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.endpoints.ModelEndpoints {
		if item.ID == id {
			m.endpoints.ModelEndpoints = append(m.endpoints.ModelEndpoints[:i], m.endpoints.ModelEndpoints[i+1:]...)
			return m.persistLocked(m.path(modelEndpointsFileName), m.endpoints)
		}
	}
	return ErrNotFound
}

func (m *globalManager) DeleteModel(ctx context.Context, id domainsemantic.InferenceModelID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.models.Models {
		if item.ID == id {
			m.models.Models = append(m.models.Models[:i], m.models.Models[i+1:]...)
			return m.persistLocked(m.path(modelsFileName), m.models)
		}
	}
	return ErrNotFound
}

func (m *globalManager) DeleteModelEndpointCapability(ctx context.Context, id domainsemantic.ModelEndpointCapabilityID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.capabilities.Capabilities {
		if item.ID == id {
			m.capabilities.Capabilities = append(m.capabilities.Capabilities[:i], m.capabilities.Capabilities[i+1:]...)
			return m.persistLocked(m.path(modelEndpointCapsFileName), m.capabilities)
		}
	}
	return ErrNotFound
}

func (m *globalManager) DeleteVectorStore(ctx context.Context, id domainsemantic.VectorStoreID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.vectorStores.VectorStores {
		if item.ID == id {
			m.vectorStores.VectorStores = append(m.vectorStores.VectorStores[:i], m.vectorStores.VectorStores[i+1:]...)
			return m.persistLocked(m.path(vectorStoresFileName), m.vectorStores)
		}
	}
	return ErrNotFound
}

func (m *globalManager) DeleteSecret(ctx context.Context, id domainsemantic.SecretID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.secrets.Secrets {
		if item.ID == id {
			m.secrets.Secrets = append(m.secrets.Secrets[:i], m.secrets.Secrets[i+1:]...)
			return m.persistLocked(m.path(secretsFileName), m.secrets)
		}
	}
	return ErrNotFound
}

func (m *globalManager) DeleteCredential(ctx context.Context, id domainsemantic.InferenceCredentialID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.credentials.Credentials {
		if item.ID == id {
			m.credentials.Credentials = append(m.credentials.Credentials[:i], m.credentials.Credentials[i+1:]...)
			return m.persistLocked(m.path(credentialsFileName), m.credentials)
		}
	}
	return ErrNotFound
}

func (m *globalManager) path(name string) string {
	switch name {
	case packagesFileName, modelEndpointsFileName, modelsFileName, modelEndpointCapsFileName, vectorStoresFileName:
		return filepath.Join(m.metaDir, inferenceDirName, name)
	case secretsFileName:
		return filepath.Join(m.metaDir, secretsDirName, name)
	case credentialsFileName:
		return filepath.Join(m.metaDir, credentialsDirName, name)
	default:
		return filepath.Join(m.metaDir, name)
	}
}

func (m *globalManager) persistLocked(path string, v any) error { return persistJSON(path, v) }

type spaceManager struct {
	mu              sync.RWMutex
	location        string
	spaceID         domainspace.SpaceID
	indexes         semanticIndexesState
	grants          credentialGrantsState
	policies        inferencePoliciesState
	indexStates     indexStatesState
	policyDecisions policyDecisionsState
}

type maintenanceManager struct {
	mu          sync.RWMutex
	location    string
	spaceID     domainspace.SpaceID
	dirtyQueue  dirtyQueueState
	checkpoints maintenanceCheckpointState
}

func (m *spaceManager) Init(ctx context.Context, location string, spaceID domainspace.SpaceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(location) == "" {
		return fmt.Errorf("%w: location is required", ErrInvalidInput)
	}
	if spaceID == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.location = location
	m.spaceID = spaceID
	if err := os.MkdirAll(location, 0o755); err != nil {
		return err
	}
	if err := loadJSON(m.path(semanticIndexesFileName), &m.indexes, semanticIndexesState{Indexes: []domainsemantic.SemanticIndex{}}); err != nil {
		return err
	}
	if err := loadJSON(m.path(credentialGrantsFileName), &m.grants, credentialGrantsState{Grants: []domainsemantic.CredentialGrant{}}); err != nil {
		return err
	}
	if err := loadJSON(m.path(inferencePoliciesFileName), &m.policies, inferencePoliciesState{Policies: []domainsemantic.InferencePolicy{}}); err != nil {
		return err
	}
	if err := loadJSON(m.path(indexStateFileName), &m.indexStates, indexStatesState{States: []domainsemantic.SemanticIndexState{}}); err != nil {
		return err
	}
	if err := loadJSON(m.path(policyDecisionsFileName), &m.policyDecisions, policyDecisionsState{Decisions: []domainsemantic.PolicyDecision{}}); err != nil {
		return err
	}
	return os.MkdirAll(filepath.Join(location, maintenanceDirName), 0o755)
}

func (m *spaceManager) UpsertSemanticIndex(ctx context.Context, index domainsemantic.SemanticIndex) (domainsemantic.SemanticIndex, error) {
	index.Purpose = domainsemantic.NormalizeSemanticIndexPurpose(index.Purpose)
	if err := validateSemanticIndex(ctx, m.spaceID, index); err != nil {
		return domainsemantic.SemanticIndex{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	index.Key = normalizeKey(index.Key)
	if index.Name == "" {
		index.Name = index.Key
	}
	if index.ID == uuid.Nil {
		index.ID = newID()
	}
	if index.CreatedAt.IsZero() {
		index.CreatedAt = now
	}
	index.UpdatedAt = now
	for i, existing := range m.indexes.Indexes {
		if existing.SpaceID == index.SpaceID && existing.DomainID == index.DomainID && normalizeKey(existing.Key) == index.Key {
			index.ID = existing.ID
			index.CreatedAt = existing.CreatedAt
			m.indexes.Indexes[i] = index
			return index, persistJSON(m.path(semanticIndexesFileName), m.indexes)
		}
	}
	m.indexes.Indexes = append(m.indexes.Indexes, index)
	return index, persistJSON(m.path(semanticIndexesFileName), m.indexes)
}

func (m *spaceManager) ListSemanticIndexes(ctx context.Context) ([]domainsemantic.SemanticIndex, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.SemanticIndex(nil), m.indexes.Indexes...), nil
}

func (m *spaceManager) UpsertCredentialGrant(ctx context.Context, grant domainsemantic.CredentialGrant) (domainsemantic.CredentialGrant, error) {
	if err := validateGrant(ctx, m.spaceID, grant); err != nil {
		return domainsemantic.CredentialGrant{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if grant.ID == uuid.Nil {
		grant.ID = newID()
	}
	if grant.CreatedAt.IsZero() {
		grant.CreatedAt = time.Now().UTC()
	}
	for i, existing := range m.grants.Grants {
		if existing.ID == grant.ID {
			m.grants.Grants[i] = grant
			return grant, persistJSON(m.path(credentialGrantsFileName), m.grants)
		}
	}
	m.grants.Grants = append(m.grants.Grants, grant)
	return grant, persistJSON(m.path(credentialGrantsFileName), m.grants)
}

func (m *spaceManager) ListCredentialGrants(ctx context.Context) ([]domainsemantic.CredentialGrant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.CredentialGrant(nil), m.grants.Grants...), nil
}

func (m *spaceManager) UpsertInferencePolicy(ctx context.Context, policy domainsemantic.InferencePolicy) (domainsemantic.InferencePolicy, error) {
	if err := validatePolicy(ctx, m.spaceID, policy); err != nil {
		return domainsemantic.InferencePolicy{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if policy.ID == uuid.Nil {
		policy.ID = newID()
	}
	if policy.CreatedAt.IsZero() {
		policy.CreatedAt = time.Now().UTC()
	}
	for i, existing := range m.policies.Policies {
		if existing.ID == policy.ID {
			m.policies.Policies[i] = policy
			return policy, persistJSON(m.path(inferencePoliciesFileName), m.policies)
		}
	}
	m.policies.Policies = append(m.policies.Policies, policy)
	return policy, persistJSON(m.path(inferencePoliciesFileName), m.policies)
}

func (m *spaceManager) ListInferencePolicies(ctx context.Context) ([]domainsemantic.InferencePolicy, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.InferencePolicy(nil), m.policies.Policies...), nil
}

func (m *spaceManager) DeleteSemanticIndex(ctx context.Context, id domainsemantic.SemanticIndexID, purgeDependents bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	found := false
	for i, item := range m.indexes.Indexes {
		if item.ID == id {
			m.indexes.Indexes = append(m.indexes.Indexes[:i], m.indexes.Indexes[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return ErrNotFound
	}
	if purgeDependents {
		grants := m.grants.Grants[:0]
		for _, grant := range m.grants.Grants {
			if grant.Scope.SemanticIndexID != id {
				grants = append(grants, grant)
			}
		}
		m.grants.Grants = grants
		policies := m.policies.Policies[:0]
		for _, policy := range m.policies.Policies {
			if policy.Scope.SemanticIndexID != id {
				policies = append(policies, policy)
			}
		}
		m.policies.Policies = policies
		states := m.indexStates.States[:0]
		for _, state := range m.indexStates.States {
			if state.SemanticIndexID != id {
				states = append(states, state)
			}
		}
		m.indexStates.States = states
		decisions := m.policyDecisions.Decisions[:0]
		for _, decision := range m.policyDecisions.Decisions {
			if decision.Scope.SemanticIndexID != id {
				decisions = append(decisions, decision)
			}
		}
		m.policyDecisions.Decisions = decisions
	}
	if err := persistJSON(m.path(semanticIndexesFileName), m.indexes); err != nil {
		return err
	}
	if purgeDependents {
		if err := persistJSON(m.path(credentialGrantsFileName), m.grants); err != nil {
			return err
		}
		if err := persistJSON(m.path(inferencePoliciesFileName), m.policies); err != nil {
			return err
		}
		if err := persistJSON(m.path(indexStateFileName), m.indexStates); err != nil {
			return err
		}
		if err := persistJSON(m.path(policyDecisionsFileName), m.policyDecisions); err != nil {
			return err
		}
	}
	return nil
}

func (m *spaceManager) DeleteCredentialGrant(ctx context.Context, id domainsemantic.CredentialGrantID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.grants.Grants {
		if item.ID == id {
			m.grants.Grants = append(m.grants.Grants[:i], m.grants.Grants[i+1:]...)
			return persistJSON(m.path(credentialGrantsFileName), m.grants)
		}
	}
	return ErrNotFound
}

func (m *spaceManager) DeleteInferencePolicy(ctx context.Context, id domainsemantic.InferencePolicyID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.policies.Policies {
		if item.ID == id {
			m.policies.Policies = append(m.policies.Policies[:i], m.policies.Policies[i+1:]...)
			return persistJSON(m.path(inferencePoliciesFileName), m.policies)
		}
	}
	return ErrNotFound
}

func (m *maintenanceManager) Init(ctx context.Context, location string, spaceID domainspace.SpaceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(location) == "" {
		return fmt.Errorf("%w: location is required", ErrInvalidInput)
	}
	if spaceID == uuid.Nil {
		return fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.location = location
	m.spaceID = spaceID
	if err := os.MkdirAll(filepath.Join(location, graphDirtyEventsDirName), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(location, workStateDirName), 0o755); err != nil {
		return err
	}
	if err := loadJSON(m.checkpointsPath(), &m.checkpoints, maintenanceCheckpointState{Checkpoints: []MaintenanceCheckpoint{}}); err != nil {
		return err
	}
	if err := loadJSON(m.workStatePath(), &m.dirtyQueue, dirtyQueueState{Items: []domainsemantic.SemanticDirtyWorkItem{}}); err != nil {
		rebuilt, rebuildErr := readWorkItemsFromLog(m.workEventsPath())
		if rebuildErr != nil {
			return err
		}
		m.dirtyQueue = dirtyQueueState{Items: rebuilt}
		return persistJSON(m.workStatePath(), m.dirtyQueue)
	}
	if len(m.dirtyQueue.Items) == 0 {
		rebuilt, err := readWorkItemsFromLog(m.workEventsPath())
		if err != nil {
			return err
		}
		if len(rebuilt) > 0 {
			m.dirtyQueue = dirtyQueueState{Items: rebuilt}
			return persistJSON(m.workStatePath(), m.dirtyQueue)
		}
	}
	return nil
}

func (m *maintenanceManager) AppendGraphDirtyEvent(ctx context.Context, event domainsemantic.GraphDirtyEvent) (domainsemantic.GraphDirtyEvent, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.GraphDirtyEvent{}, err
	}
	if event.TxnID == uuid.Nil || event.SpaceID != m.spaceID {
		return domainsemantic.GraphDirtyEvent{}, fmt.Errorf("%w: txn_id and matching space_id are required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, err := readGraphDirtyEvents(m.graphDirtyEventsPath())
	if err != nil {
		return domainsemantic.GraphDirtyEvent{}, err
	}
	for _, e := range existing {
		if e.TxnID == event.TxnID {
			return e, nil
		}
	}
	if event.ID == uuid.Nil {
		event.ID = newID()
	}
	if event.CommittedAt.IsZero() {
		event.CommittedAt = time.Now().UTC()
	}
	if err := os.MkdirAll(filepath.Dir(m.graphDirtyEventsPath()), 0o755); err != nil {
		return domainsemantic.GraphDirtyEvent{}, err
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return domainsemantic.GraphDirtyEvent{}, err
	}
	f, err := os.OpenFile(m.graphDirtyEventsPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return domainsemantic.GraphDirtyEvent{}, err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return domainsemantic.GraphDirtyEvent{}, err
	}
	return event, f.Sync()
}

func (m *maintenanceManager) ListGraphDirtyEvents(ctx context.Context) ([]domainsemantic.GraphDirtyEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return readGraphDirtyEvents(m.graphDirtyEventsPath())
}

func (m *maintenanceManager) GetCheckpoint(ctx context.Context, consumer string) (MaintenanceCheckpoint, error) {
	if err := ctx.Err(); err != nil {
		return MaintenanceCheckpoint{}, err
	}
	consumer = strings.TrimSpace(consumer)
	if consumer == "" {
		return MaintenanceCheckpoint{}, fmt.Errorf("%w: consumer is required", ErrInvalidInput)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, checkpoint := range m.checkpoints.Checkpoints {
		if checkpoint.Consumer == consumer {
			return checkpoint, nil
		}
	}
	return MaintenanceCheckpoint{Consumer: consumer, SpaceID: m.spaceID}, nil
}

func (m *maintenanceManager) SaveCheckpoint(ctx context.Context, checkpoint MaintenanceCheckpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	checkpoint.Consumer = strings.TrimSpace(checkpoint.Consumer)
	if checkpoint.Consumer == "" || checkpoint.SpaceID != m.spaceID {
		return fmt.Errorf("%w: consumer and matching space_id are required", ErrInvalidInput)
	}
	if checkpoint.UpdatedAt.IsZero() {
		checkpoint.UpdatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.checkpoints.Checkpoints {
		if existing.Consumer == checkpoint.Consumer {
			m.checkpoints.Checkpoints[i] = checkpoint
			return persistJSON(m.checkpointsPath(), m.checkpoints)
		}
	}
	m.checkpoints.Checkpoints = append(m.checkpoints.Checkpoints, checkpoint)
	return persistJSON(m.checkpointsPath(), m.checkpoints)
}

func (m *maintenanceManager) UpsertDirtyWorkItem(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) (domainsemantic.SemanticDirtyWorkItem, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.SemanticDirtyWorkItem{}, err
	}
	if item.SemanticIndexID == uuid.Nil || item.SpaceID != m.spaceID || item.TargetNodeID == uuid.Nil {
		return domainsemantic.SemanticDirtyWorkItem{}, fmt.Errorf("%w: semantic_index_id, matching space_id, and target_node_id are required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	if item.ID == uuid.Nil {
		item.ID = newID()
	}
	if item.Status == "" {
		item.Status = domainsemantic.SemanticDirtyWorkStatusPending
	}
	if item.Action == "" {
		item.Action = domainsemantic.SemanticDirtyWorkActionRefresh
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	if item.Status == domainsemantic.SemanticDirtyWorkStatusPending {
		item.ClaimedBy = ""
		item.ClaimedUntil = nil
		item.CompletedAt = nil
		item.FailedAt = nil
	}
	item.UpdatedAt = now
	for i, existing := range m.dirtyQueue.Items {
		if existing.SemanticIndexID == item.SemanticIndexID && existing.TargetNodeID == item.TargetNodeID {
			item.ID = existing.ID
			item.CreatedAt = existing.CreatedAt
			if existing.FirstGraphRevision != 0 && (item.FirstGraphRevision == 0 || existing.FirstGraphRevision < item.FirstGraphRevision) {
				item.FirstGraphRevision = existing.FirstGraphRevision
			}
			item.SourceTxnIDs = mergeTxnIDs(existing.SourceTxnIDs, item.SourceTxnIDs)
			if err := m.appendWorkLogRecord("upsert", item); err != nil {
				return domainsemantic.SemanticDirtyWorkItem{}, err
			}
			m.dirtyQueue.Items[i] = item
			return item, persistJSON(m.workStatePath(), m.dirtyQueue)
		}
	}
	if err := m.appendWorkLogRecord("upsert", item); err != nil {
		return domainsemantic.SemanticDirtyWorkItem{}, err
	}
	m.dirtyQueue.Items = append(m.dirtyQueue.Items, item)
	return item, persistJSON(m.workStatePath(), m.dirtyQueue)
}

func (m *maintenanceManager) ListDirtyWorkItems(ctx context.Context) ([]domainsemantic.SemanticDirtyWorkItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.SemanticDirtyWorkItem(nil), m.dirtyQueue.Items...), nil
}

func (m *maintenanceManager) ClaimReadyWork(ctx context.Context, in ClaimReadyWorkInput) ([]domainsemantic.SemanticDirtyWorkItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	lease := in.LeaseDuration
	if lease <= 0 {
		lease = time.Minute
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 1
	}
	claimedBy := strings.TrimSpace(in.ClaimedBy)
	if claimedBy == "" {
		claimedBy = "semantic-worker"
	}
	claimedUntil := now.Add(lease)
	m.mu.Lock()
	defer m.mu.Unlock()
	claimed := []domainsemantic.SemanticDirtyWorkItem{}
	for i := range m.dirtyQueue.Items {
		if len(claimed) >= limit {
			break
		}
		item := m.dirtyQueue.Items[i]
		if !workItemReady(item, now) {
			continue
		}
		item.Status = domainsemantic.SemanticDirtyWorkStatusRunning
		item.Attempts++
		item.ClaimedBy = claimedBy
		item.ClaimedUntil = &claimedUntil
		item.UpdatedAt = now
		if err := m.appendWorkLogRecord("claim", item); err != nil {
			return nil, err
		}
		m.dirtyQueue.Items[i] = item
		claimed = append(claimed, item)
	}
	if len(claimed) == 0 {
		return []domainsemantic.SemanticDirtyWorkItem{}, nil
	}
	return claimed, persistJSON(m.workStatePath(), m.dirtyQueue)
}

func (m *maintenanceManager) CompleteWork(ctx context.Context, id uuid.UUID, result WorkResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == uuid.Nil {
		return fmt.Errorf("%w: work item id is required", ErrInvalidInput)
	}
	now := result.CompletedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.dirtyQueue.Items {
		if item.ID != id {
			continue
		}
		item.Status = domainsemantic.SemanticDirtyWorkStatusComplete
		item.ClaimedBy = ""
		item.ClaimedUntil = nil
		item.LastError = ""
		item.LastErrorCategory = ""
		item.CompletedAt = &now
		item.FailedAt = nil
		item.UpdatedAt = now
		if err := m.appendWorkLogRecord("complete", item); err != nil {
			return err
		}
		m.dirtyQueue.Items[i] = item
		return persistJSON(m.workStatePath(), m.dirtyQueue)
	}
	return ErrNotFound
}

func (m *maintenanceManager) FailWork(ctx context.Context, id uuid.UUID, failure WorkFailure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == uuid.Nil {
		return fmt.Errorf("%w: work item id is required", ErrInvalidInput)
	}
	now := failure.FailedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, item := range m.dirtyQueue.Items {
		if item.ID != id {
			continue
		}
		if failure.Retryable {
			item.Status = domainsemantic.SemanticDirtyWorkStatusPending
			item.EarliestRunAt = failure.NextRunAt
		} else {
			item.Status = domainsemantic.SemanticDirtyWorkStatusFailed
		}
		item.ClaimedBy = ""
		item.ClaimedUntil = nil
		item.LastError = failure.Message
		item.LastErrorCategory = failure.Category
		item.FailedAt = &now
		item.CompletedAt = nil
		item.UpdatedAt = now
		if err := m.appendWorkLogRecord("fail", item); err != nil {
			return err
		}
		m.dirtyQueue.Items[i] = item
		return persistJSON(m.workStatePath(), m.dirtyQueue)
	}
	return ErrNotFound
}

func workItemReady(item domainsemantic.SemanticDirtyWorkItem, now time.Time) bool {
	if item.Status == domainsemantic.SemanticDirtyWorkStatusPending {
		return item.EarliestRunAt == nil || !item.EarliestRunAt.After(now)
	}
	if item.Status == domainsemantic.SemanticDirtyWorkStatusRunning && item.ClaimedUntil != nil && item.ClaimedUntil.Before(now) {
		return item.EarliestRunAt == nil || !item.EarliestRunAt.After(now)
	}
	return false
}

func (m *spaceManager) UpsertIndexState(ctx context.Context, state domainsemantic.SemanticIndexState) (domainsemantic.SemanticIndexState, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.SemanticIndexState{}, err
	}
	if state.SemanticIndexID == uuid.Nil {
		return domainsemantic.SemanticIndexState{}, fmt.Errorf("%w: semantic_index_id is required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if state.UpdatedAt.IsZero() {
		state.UpdatedAt = time.Now().UTC()
	}
	for i, existing := range m.indexStates.States {
		if existing.SemanticIndexID == state.SemanticIndexID {
			m.indexStates.States[i] = state
			return state, persistJSON(m.path(indexStateFileName), m.indexStates)
		}
	}
	m.indexStates.States = append(m.indexStates.States, state)
	return state, persistJSON(m.path(indexStateFileName), m.indexStates)
}

func (m *spaceManager) ListIndexStates(ctx context.Context) ([]domainsemantic.SemanticIndexState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.SemanticIndexState(nil), m.indexStates.States...), nil
}

func (m *spaceManager) UpsertPolicyDecision(ctx context.Context, decision domainsemantic.PolicyDecision) (domainsemantic.PolicyDecision, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.PolicyDecision{}, err
	}
	if decision.ID == uuid.Nil {
		decision.ID = newID()
	}
	if decision.CreatedAt.IsZero() {
		decision.CreatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.policyDecisions.Decisions {
		if existing.ID == decision.ID {
			m.policyDecisions.Decisions[i] = decision
			return decision, persistJSON(m.path(policyDecisionsFileName), m.policyDecisions)
		}
	}
	m.policyDecisions.Decisions = append(m.policyDecisions.Decisions, decision)
	return decision, persistJSON(m.path(policyDecisionsFileName), m.policyDecisions)
}

func (m *spaceManager) ListPolicyDecisions(ctx context.Context) ([]domainsemantic.PolicyDecision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domainsemantic.PolicyDecision(nil), m.policyDecisions.Decisions...), nil
}

func (m *maintenanceManager) graphDirtyEventsPath() string {
	return filepath.Join(m.location, graphDirtyEventsDirName, graphDirtyEventsFileName)
}

func (m *maintenanceManager) workStatePath() string {
	return filepath.Join(m.location, workStateDirName, workStateFileName)
}

func (m *maintenanceManager) workEventsPath() string {
	return filepath.Join(m.location, workStateDirName, workEventsFileName)
}

func (m *maintenanceManager) checkpointsPath() string {
	return filepath.Join(m.location, checkpointsFileName)
}

func (m *maintenanceManager) appendWorkLogRecord(kind string, item domainsemantic.SemanticDirtyWorkItem) error {
	if err := os.MkdirAll(filepath.Dir(m.workEventsPath()), 0o755); err != nil {
		return err
	}
	record := workLogRecord{Kind: kind, At: time.Now().UTC(), Item: item}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(m.workEventsPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func (m *spaceManager) path(name string) string { return filepath.Join(m.location, name) }

func readGraphDirtyEvents(path string) ([]domainsemantic.GraphDirtyEvent, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []domainsemantic.GraphDirtyEvent{}, nil
		}
		return nil, err
	}
	out := []domainsemantic.GraphDirtyEvent{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event domainsemantic.GraphDirtyEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, nil
}

func readWorkItemsFromLog(path string) ([]domainsemantic.SemanticDirtyWorkItem, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []domainsemantic.SemanticDirtyWorkItem{}, nil
		}
		return nil, err
	}
	items := []domainsemantic.SemanticDirtyWorkItem{}
	index := map[uuid.UUID]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record workLogRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, err
		}
		item := record.Item
		if item.ID == uuid.Nil {
			continue
		}
		if pos, ok := index[item.ID]; ok {
			items[pos] = item
			continue
		}
		index[item.ID] = len(items)
		items = append(items, item)
	}
	return items, nil
}

func mergeTxnIDs(a, b []uuid.UUID) []uuid.UUID {
	seen := map[uuid.UUID]bool{}
	out := []uuid.UUID{}
	for _, id := range append(append([]uuid.UUID{}, a...), b...) {
		if id == uuid.Nil || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}

func loadJSON[T any](path string, target *T, empty T) error {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			*target = empty
			return persistJSON(path, empty)
		}
		return err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(raw)) == "" {
		*target = empty
		return nil
	}
	return json.Unmarshal(raw, target)
}

func persistJSON(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return filestore.WriteFileAtomic(path, raw, 0o600)
}

func newID() uuid.UUID {
	id, err := uuid.NewV7()
	if err == nil {
		return id
	}
	return uuid.New()
}

func normalizeKey(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

func validatePackage(ctx context.Context, pkg domainsemantic.InferencePackage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(pkg.Name) == "" {
		return fmt.Errorf("%w: package name is required", ErrInvalidInput)
	}
	if strings.TrimSpace(pkg.Version) == "" {
		return fmt.Errorf("%w: package version is required", ErrInvalidInput)
	}
	return nil
}

func validateModelEndpoint(ctx context.Context, endpoint domainsemantic.ModelEndpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(endpoint.Key) == "" {
		return fmt.Errorf("%w: model endpoint key is required", ErrInvalidInput)
	}
	if endpoint.ConnectorType == "" {
		return fmt.Errorf("%w: connector_type is required", ErrInvalidInput)
	}
	if endpoint.PrivacyClass == "" {
		return fmt.Errorf("%w: privacy_class is required", ErrInvalidInput)
	}
	return nil
}

func validateModel(ctx context.Context, model domainsemantic.InferenceModel) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(model.Key) == "" {
		return fmt.Errorf("%w: model key is required", ErrInvalidInput)
	}
	if model.Operation == "" {
		return fmt.Errorf("%w: operation is required", ErrInvalidInput)
	}
	if model.Operation == domainsemantic.OperationEmbeddings {
		if model.Dimensions <= 0 {
			return fmt.Errorf("%w: embedding dimensions must be positive", ErrInvalidInput)
		}
		if strings.TrimSpace(model.VectorSpaceKey) == "" {
			return fmt.Errorf("%w: vector_space_key is required for embedding models", ErrInvalidInput)
		}
	}
	return nil
}

func validateCapability(ctx context.Context, capability domainsemantic.ModelEndpointCapability) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if capability.ModelEndpointID == uuid.Nil {
		return fmt.Errorf("%w: model_endpoint_id is required", ErrInvalidInput)
	}
	if capability.ModelID == uuid.Nil {
		return fmt.Errorf("%w: model_id is required", ErrInvalidInput)
	}
	if capability.Operation == "" {
		return fmt.Errorf("%w: operation is required", ErrInvalidInput)
	}
	return nil
}

func validateVectorStore(ctx context.Context, vectorStore domainsemantic.VectorStoreBackend) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(vectorStore.Key) == "" {
		return fmt.Errorf("%w: vector store key is required", ErrInvalidInput)
	}
	if vectorStore.Type == "" {
		return fmt.Errorf("%w: vector store type is required", ErrInvalidInput)
	}
	if vectorStore.PrivacyClass == "" {
		return fmt.Errorf("%w: privacy_class is required", ErrInvalidInput)
	}
	return nil
}

func validateSecret(ctx context.Context, secret domainsemantic.Secret) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if secret.OwnerType == "" {
		return fmt.Errorf("%w: owner_type is required", ErrInvalidInput)
	}
	if strings.TrimSpace(secret.OwnerID) == "" {
		return fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	if secret.Kind == "" {
		return fmt.Errorf("%w: secret kind is required", ErrInvalidInput)
	}
	hasCipher := secret.Ciphertext != nil && strings.TrimSpace(secret.Ciphertext.CipherB64) != ""
	hasExternal := strings.TrimSpace(secret.ExternalRef) != ""
	if hasCipher == hasExternal {
		return fmt.Errorf("%w: exactly one of ciphertext or external_ref is required", ErrInvalidInput)
	}
	return nil
}

func validateCredential(ctx context.Context, credential domainsemantic.InferenceCredential) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(credential.Key) == "" {
		return fmt.Errorf("%w: credential key is required", ErrInvalidInput)
	}
	if credential.ModelEndpointID == uuid.Nil {
		return fmt.Errorf("%w: model_endpoint_id is required", ErrInvalidInput)
	}
	if credential.OwnerType == "" {
		return fmt.Errorf("%w: owner_type is required", ErrInvalidInput)
	}
	if strings.TrimSpace(credential.OwnerID) == "" {
		return fmt.Errorf("%w: owner_id is required", ErrInvalidInput)
	}
	if credential.AuthType == "" {
		return fmt.Errorf("%w: auth_type is required", ErrInvalidInput)
	}
	return nil
}

func validateSemanticIndex(ctx context.Context, spaceID domainspace.SpaceID, index domainsemantic.SemanticIndex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if index.SpaceID == uuid.Nil {
		return fmt.Errorf("%w: index space_id is required", ErrInvalidInput)
	}
	if index.SpaceID != spaceID {
		return fmt.Errorf("%w: index space_id does not match store", ErrInvalidInput)
	}
	if index.DomainID == uuid.Nil {
		return fmt.Errorf("%w: domain_id is required", ErrInvalidInput)
	}
	if strings.TrimSpace(index.Key) == "" {
		return fmt.Errorf("%w: index key is required", ErrInvalidInput)
	}
	if index.Purpose == "" {
		return fmt.Errorf("%w: purpose is required", ErrInvalidInput)
	}
	if index.SourcePolicy.Extraction == "" {
		return fmt.Errorf("%w: source extraction is required", ErrInvalidInput)
	}
	if index.ModelEndpointID == uuid.Nil {
		return fmt.Errorf("%w: model_endpoint_id is required", ErrInvalidInput)
	}
	if index.ModelID == uuid.Nil {
		return fmt.Errorf("%w: model_id is required", ErrInvalidInput)
	}
	if index.VectorStoreID == uuid.Nil {
		return fmt.Errorf("%w: vector_store_id is required", ErrInvalidInput)
	}
	return nil
}

func validateGrant(ctx context.Context, spaceID domainspace.SpaceID, grant domainsemantic.CredentialGrant) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if grant.CredentialID == uuid.Nil {
		return fmt.Errorf("%w: credential_id is required", ErrInvalidInput)
	}
	return validateScope(spaceID, grant.Scope)
}

func validatePolicy(ctx context.Context, spaceID domainspace.SpaceID, policy domainsemantic.InferencePolicy) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if policy.Effect == "" {
		return fmt.Errorf("%w: policy effect is required", ErrInvalidInput)
	}
	return validateScope(spaceID, policy.Scope)
}

func validateScope(spaceID domainspace.SpaceID, scope domainsemantic.ProcessingScope) error {
	if scope.SpaceID == uuid.Nil {
		return fmt.Errorf("%w: scope space_id is required", ErrInvalidInput)
	}
	if scope.SpaceID != spaceID {
		return fmt.Errorf("%w: scope space_id does not match store", ErrInvalidInput)
	}
	if scope.DomainID == graph.DomainID(uuid.Nil) && scope.SemanticIndexID == uuid.Nil && scope.NodeID == uuid.Nil {
		// Space-wide scope is valid.
		return nil
	}
	return nil
}
