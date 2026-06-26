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
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	"github.com/myceldb/mycel/internal/filestore"
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
	mu       sync.RWMutex
	location string
	spaceID  domainspace.SpaceID
	indexes  semanticIndexesState
	grants   credentialGrantsState
	policies inferencePoliciesState
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
	return loadJSON(m.path(inferencePoliciesFileName), &m.policies, inferencePoliciesState{Policies: []domainsemantic.InferencePolicy{}})
}

func (m *spaceManager) UpsertSemanticIndex(ctx context.Context, index domainsemantic.SemanticIndex) (domainsemantic.SemanticIndex, error) {
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

func (m *spaceManager) path(name string) string { return filepath.Join(m.location, name) }

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
