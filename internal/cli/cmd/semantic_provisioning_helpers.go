package cmd

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	storesemantic "github.com/myceldb/mycel/store/semantic"
)

type semanticConfigEvent struct {
	ID        uuid.UUID      `json:"id"`
	Type      string         `json:"type"`
	SpaceID   *uuid.UUID     `json:"space_id,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

func authenticatedSemanticGlobalManager(ctx context.Context, a *app.App) (storesemantic.GlobalManager, error) {
	tok, err := a.AccessToken(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := a.Engine.ListSystemAccess(ctx, mycelengine.ListSystemAccessInput{AccessToken: tok}); err != nil {
		return nil, err
	}
	mgr := storesemantic.NewGlobalManager()
	if err := mgr.Init(ctx, filepath.Join(a.DataDir, "meta")); err != nil {
		return nil, err
	}
	return mgr, nil
}

func authenticatedSemanticSpaceManager(ctx context.Context, a *app.App, spaceID domainspace.SpaceID) (storesemantic.SpaceManager, error) {
	tok, err := a.AccessToken(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := a.Engine.ListSpaceAccess(ctx, mycelengine.ListSpaceAccessInput{AccessToken: tok, SpaceID: spaceID}); err != nil {
		return nil, err
	}
	mgr := storesemantic.NewSpaceManager()
	if err := mgr.Init(ctx, filepath.Join(a.DataDir, "graphs", spaceID.String(), "semantic"), spaceID); err != nil {
		return nil, err
	}
	return mgr, nil
}

func appendSemanticConfigEvent(dataDir, typ string, spaceID *domainspace.SpaceID, payload map[string]any) error {
	if strings.TrimSpace(dataDir) == "" {
		return fmt.Errorf("data dir is required")
	}
	dir := filepath.Join(dataDir, "meta", "semantic_events")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var eventSpaceID *uuid.UUID
	if spaceID != nil {
		id := uuid.UUID(*spaceID)
		eventSpaceID = &id
	}
	evt := semanticConfigEvent{ID: uuid.New(), Type: typ, SpaceID: eventSpaceID, Payload: payload, CreatedAt: time.Now().UTC()}
	raw, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "semantic-config-000001.ksem"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func resolveModelEndpointID(ctx context.Context, mgr storesemantic.GlobalManager, raw string) (domainsemantic.ModelEndpointID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		return domainsemantic.ModelEndpointID(id), nil
	}
	endpoints, err := mgr.ListModelEndpoints(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	key := normalizeCLIKey(raw)
	for _, endpoint := range endpoints {
		if normalizeCLIKey(endpoint.Key) == key {
			return endpoint.ID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("model endpoint %q not found", raw)
}

func resolveModelID(ctx context.Context, mgr storesemantic.GlobalManager, raw string) (domainsemantic.InferenceModelID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		return domainsemantic.InferenceModelID(id), nil
	}
	models, err := mgr.ListModels(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	key := normalizeCLIKey(raw)
	for _, model := range models {
		if normalizeCLIKey(model.Key) == key {
			return model.ID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("model %q not found", raw)
}

func resolveVectorStoreID(ctx context.Context, mgr storesemantic.GlobalManager, raw string) (domainsemantic.VectorStoreID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		return domainsemantic.VectorStoreID(id), nil
	}
	stores, err := mgr.ListVectorStores(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	key := normalizeCLIKey(raw)
	for _, store := range stores {
		if normalizeCLIKey(store.Key) == key {
			return store.ID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("vector store %q not found", raw)
}

func resolveCredentialID(ctx context.Context, mgr storesemantic.GlobalManager, raw string) (domainsemantic.InferenceCredentialID, error) {
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		return domainsemantic.InferenceCredentialID(id), nil
	}
	credentials, err := mgr.ListCredentials(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	key := normalizeCLIKey(raw)
	for _, credential := range credentials {
		if normalizeCLIKey(credential.Key) == key {
			return credential.ID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("credential %q not found", raw)
}

func resolveSemanticIndexID(ctx context.Context, mgr storesemantic.SpaceManager, raw string) (domainsemantic.SemanticIndexID, error) {
	if strings.TrimSpace(raw) == "" {
		return uuid.Nil, nil
	}
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		return domainsemantic.SemanticIndexID(id), nil
	}
	indexes, err := mgr.ListSemanticIndexes(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	key := normalizeCLIKey(raw)
	for _, index := range indexes {
		if normalizeCLIKey(index.Key) == key {
			return index.ID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("semantic index %q not found", raw)
}

func resolveDomainID(ctx context.Context, a *app.App, token mycelengine.AccessToken, spaceID domainspace.SpaceID, raw string) (graph.DomainID, error) {
	if strings.TrimSpace(raw) == "" {
		domain, err := a.Engine.GetDomain(ctx, mycelengine.GetDomainInput{AccessToken: token, SpaceID: spaceID, Key: graph.DefaultDomainKey})
		if err != nil {
			return uuid.Nil, err
		}
		return domain.ID, nil
	}
	if id, err := uuid.Parse(strings.TrimSpace(raw)); err == nil {
		domain, err := a.Engine.GetDomain(ctx, mycelengine.GetDomainInput{AccessToken: token, SpaceID: spaceID, DomainID: graph.DomainID(id)})
		if err != nil {
			return uuid.Nil, err
		}
		return domain.ID, nil
	}
	domain, err := a.Engine.GetDomain(ctx, mycelengine.GetDomainInput{AccessToken: token, SpaceID: spaceID, Key: raw})
	if err != nil {
		return uuid.Nil, err
	}
	return domain.ID, nil
}

func capabilityFor(ctx context.Context, mgr storesemantic.GlobalManager, endpointID domainsemantic.ModelEndpointID, modelID domainsemantic.InferenceModelID, op domainsemantic.Operation) (domainsemantic.ModelEndpointCapabilityID, error) {
	caps, err := mgr.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	for _, cap := range caps {
		if cap.ModelEndpointID == endpointID && cap.ModelID == modelID && cap.Operation == op && cap.Enabled {
			return cap.ID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("enabled capability not found for endpoint=%s model=%s operation=%s", endpointID, modelID, op)
}

func semanticScope(spaceID domainspace.SpaceID, domainID graph.DomainID, indexID domainsemantic.SemanticIndexID, nodeText string, includeDescendants bool) (domainsemantic.ProcessingScope, error) {
	scope := domainsemantic.ProcessingScope{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, IncludeDescendants: includeDescendants}
	if strings.TrimSpace(nodeText) != "" {
		nodeID, err := app.ParseUUID[graph.NodeID](nodeText)
		if err != nil {
			return domainsemantic.ProcessingScope{}, err
		}
		scope.NodeID = nodeID
	}
	return scope, nil
}

func operationsFromStrings(values []string) []domainsemantic.Operation {
	out := make([]domainsemantic.Operation, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, domainsemantic.Operation(value))
		}
	}
	return out
}

func privacyClassesFromStrings(values []string) []domainsemantic.PrivacyClass {
	out := make([]domainsemantic.PrivacyClass, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, domainsemantic.PrivacyClass(value))
		}
	}
	return out
}

func encryptSecretForCLI(dataDir, configuredKeyB64, plain string) (*domainsemantic.EncryptedSecretPayload, error) {
	if strings.TrimSpace(plain) == "" {
		return nil, fmt.Errorf("secret value is required")
	}
	var key []byte
	if strings.TrimSpace(configuredKeyB64) != "" {
		decoded, err := base64.StdEncoding.DecodeString(configuredKeyB64)
		if err != nil {
			return nil, fmt.Errorf("invalid secret encryption key: %w", err)
		}
		if len(decoded) != 32 {
			return nil, fmt.Errorf("secret encryption key must be 32 bytes")
		}
		key = decoded
	} else {
		return nil, fmt.Errorf("inline secrets require --user-store-encryption-key-b64 or MYCELDB_USER_STORE_ENCRYPTION_KEY_B64; use --external-ref for external secret managers")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, []byte(plain), nil)
	return &domainsemantic.EncryptedSecretPayload{Algorithm: "AES-256-GCM", NonceB64: base64.StdEncoding.EncodeToString(nonce), CipherB64: base64.StdEncoding.EncodeToString(ciphertext)}, nil
}

func normalizeCLIKey(value string) string { return strings.ToLower(strings.TrimSpace(value)) }
