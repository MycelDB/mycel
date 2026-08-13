package connectors

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	storeaccounting "github.com/myceldb/mycel/internal/semantic/accounting"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestOpenAICompatibleEmbeddingAndAccounting(t *testing.T) {
	ctx := context.Background()
	var seenAuth, seenModel string
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		seenAuth = r.Header.Get("Authorization")
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request failed: %v", err)
		}
		seenModel, _ = body["model"].(string)
		w.Header().Set("X-Request-Id", "req-ok")
		_, _ = w.Write([]byte(`{"data":[{"embedding":[1,0,0]}],"usage":{"prompt_tokens":3,"total_tokens":3}}`))
	}))
	defer httpServer.Close()

	global, acct, keyB64, ids := provisionConnectorTestResources(t, httpServer.URL+"/v1", domainsemantic.CredentialStatusActive, true)
	service := Service{GlobalManager: global, Accounting: acct, SecretKeyB64: keyB64}
	resp, err := service.Embed(ctx, EmbedInput{ModelEndpointID: ids.endpointID, ModelID: ids.modelID, CredentialID: ids.credentialID, CredentialGrantID: ids.grantID, SpaceID: ids.spaceID, DomainID: ids.domainID, SemanticIndexID: ids.indexID, TargetNodeID: ids.nodeID, ActorPrincipalID: ids.userID, Input: "hello"})
	if err != nil {
		t.Fatalf("embed failed: %v", err)
	}
	if len(resp.Vector) != 3 || resp.TotalTokens != 3 || seenAuth != "Bearer sk-test" || seenModel != "text-embedding-3-small" {
		t.Fatalf("unexpected response/auth/model: resp=%+v auth=%q model=%q", resp, seenAuth, seenModel)
	}
	events, err := acct.List(ctx, storeaccounting.Filter{})
	if err != nil {
		t.Fatalf("list accounting failed: %v", err)
	}
	if len(events) != 1 || events[0].Status != "success" || events[0].TotalTokens != 3 || events[0].ProviderRequestID != "req-ok" {
		t.Fatalf("unexpected accounting events: %+v", events)
	}
}

func TestConnectorValidationAndFailureAccounting(t *testing.T) {
	ctx := context.Background()
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"rate limited","code":"rate_limit"}}`))
	}))
	defer httpServer.Close()

	global, acct, keyB64, ids := provisionConnectorTestResources(t, httpServer.URL, domainsemantic.CredentialStatusActive, true)
	service := Service{GlobalManager: global, Accounting: acct, SecretKeyB64: keyB64}
	if _, err := service.Embed(ctx, EmbedInput{ModelEndpointID: ids.endpointID, ModelID: ids.modelID, CredentialID: ids.credentialID, ActorPrincipalID: ids.userID, Input: "hello"}); err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("expected provider failure, got %v", err)
	}
	events, err := acct.List(ctx, storeaccounting.Filter{})
	if err != nil {
		t.Fatalf("list accounting failed: %v", err)
	}
	if len(events) != 1 || events[0].Status != "failed" || !strings.Contains(events[0].ErrorMessage, "rate limited") {
		t.Fatalf("expected failed accounting event, got %+v", events)
	}

	revoked, _, _, ids2 := provisionConnectorTestResources(t, httpServer.URL, domainsemantic.CredentialStatusRevoked, true)
	if _, err := (Service{GlobalManager: revoked}).Embed(ctx, EmbedInput{ModelEndpointID: ids2.endpointID, ModelID: ids2.modelID, CredentialID: ids2.credentialID, Input: "hello"}); err == nil || !strings.Contains(err.Error(), "not active") {
		t.Fatalf("expected revoked credential error, got %v", err)
	}

	missingCapability, _, _, ids3 := provisionConnectorTestResources(t, httpServer.URL, domainsemantic.CredentialStatusActive, false)
	if _, err := (Service{GlobalManager: missingCapability}).Embed(ctx, EmbedInput{ModelEndpointID: ids3.endpointID, ModelID: ids3.modelID, CredentialID: ids3.credentialID, Input: "hello"}); err == nil || !strings.Contains(err.Error(), "enabled capability not found") {
		t.Fatalf("expected capability error, got %v", err)
	}

	global2, _, keyB64b, ids4 := provisionConnectorTestResources(t, httpServer.URL, domainsemantic.CredentialStatusActive, true)
	service = Service{GlobalManager: global2, Accounting: failingAccounting{}, SecretKeyB64: keyB64b, Connectors: map[domainsemantic.ConnectorType]Connector{domainsemantic.ConnectorOpenAICompatible: failingConnector{}}}
	if _, err := service.Embed(ctx, EmbedInput{ModelEndpointID: ids4.endpointID, ModelID: ids4.modelID, CredentialID: ids4.credentialID, Input: "hello"}); err == nil || !strings.Contains(err.Error(), "additionally failed to append accounting event") {
		t.Fatalf("expected combined provider/accounting error, got %v", err)
	}
}

type connectorTestIDs struct {
	endpointID, modelID, credentialID, grantID, indexID uuid.UUID
	spaceID                                             domainspace.SpaceID
	domainID                                            graph.DomainID
	nodeID                                              graph.NodeID
	userID                                              identity.PrincipalID
}

func provisionConnectorTestResources(t *testing.T, endpointURL string, credentialStatus domainsemantic.CredentialStatus, capabilityEnabled bool) (storesemantic.GlobalManager, storeaccounting.Manager, string, connectorTestIDs) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	global := storesemantic.NewGlobalManager()
	if err := global.Init(ctx, filepath.Join(root, "meta")); err != nil {
		t.Fatalf("global init failed: %v", err)
	}
	acct := storeaccounting.NewManager()
	if err := acct.Init(ctx, filepath.Join(root, "meta", "accounting")); err != nil {
		t.Fatalf("accounting init failed: %v", err)
	}
	ids := connectorTestIDs{endpointID: uuid.New(), modelID: uuid.New(), credentialID: uuid.New(), grantID: uuid.New(), indexID: uuid.New(), spaceID: uuid.New(), domainID: uuid.New(), nodeID: uuid.New(), userID: identity.PrincipalID(uuid.NewString())}
	now := time.Now().UTC()
	if _, err := global.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{ID: ids.endpointID, Key: "openai", Name: "OpenAI", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: endpointURL, NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, AuthModes: []domainsemantic.AuthMode{domainsemantic.AuthModeBearerToken}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("endpoint upsert failed: %v", err)
	}
	if _, err := global.UpsertModel(ctx, domainsemantic.InferenceModel{ID: ids.modelID, Key: "openai/text-embedding-3-small", Operation: domainsemantic.OperationEmbeddings, ModelName: "text-embedding-3-small", ConnectorTypes: []domainsemantic.ConnectorType{domainsemantic.ConnectorOpenAICompatible}, Dimensions: 3, VectorSpaceKey: "openai/text-embedding-3-small", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("model upsert failed: %v", err)
	}
	if _, err := global.UpsertModelEndpointCapability(ctx, domainsemantic.ModelEndpointCapability{ID: uuid.New(), ModelEndpointID: ids.endpointID, ModelID: ids.modelID, Operation: domainsemantic.OperationEmbeddings, Enabled: capabilityEnabled, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("capability upsert failed: %v", err)
	}
	key := []byte("0123456789abcdef0123456789abcdef")
	keyB64 := base64.StdEncoding.EncodeToString(key)
	secretID := domainsemantic.SecretID(uuid.New())
	payload := encryptConnectorTestSecret(t, key, "sk-test")
	if _, err := global.UpsertSecret(ctx, domainsemantic.Secret{ID: secretID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: ids.userID.String(), Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: payload, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("secret upsert failed: %v", err)
	}
	if _, err := global.UpsertCredential(ctx, domainsemantic.InferenceCredential{ID: ids.credentialID, Key: "openai-key", Name: "OpenAI key", ModelEndpointID: ids.endpointID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: ids.userID.String(), AuthType: domainsemantic.AuthModeBearerToken, SecretRef: secretID, Status: credentialStatus, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("credential upsert failed: %v", err)
	}
	return global, acct, keyB64, ids
}

type failingConnector struct{}

func (failingConnector) Embed(ctx context.Context, in EmbeddingRequest) (EmbeddingResponse, error) {
	return EmbeddingResponse{}, errors.New("provider down")
}

type failingAccounting struct{}

func (failingAccounting) Init(ctx context.Context, location string) error { return nil }
func (failingAccounting) Append(ctx context.Context, event domainsemantic.InferenceUsageEvent) (domainsemantic.InferenceUsageEvent, error) {
	return domainsemantic.InferenceUsageEvent{}, errors.New("ledger unavailable")
}
func (failingAccounting) List(ctx context.Context, filter storeaccounting.Filter) ([]domainsemantic.InferenceUsageEvent, error) {
	return nil, nil
}
func (failingAccounting) Summarize(ctx context.Context, filter storeaccounting.Filter, groupBy []string) ([]storeaccounting.SummaryRow, error) {
	return nil, nil
}
func (failingAccounting) RebuildIndexes(ctx context.Context) error { return nil }
func (failingAccounting) RebuildRollups(ctx context.Context) error { return nil }

func encryptConnectorTestSecret(t *testing.T, key []byte, plain string) *domainsemantic.EncryptedSecretPayload {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("cipher failed: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("gcm failed: %v", err)
	}
	nonce := []byte("123456789012")
	cipherText := gcm.Seal(nil, nonce, []byte(plain), nil)
	return &domainsemantic.EncryptedSecretPayload{Algorithm: "AES-256-GCM", NonceB64: base64.StdEncoding.EncodeToString(nonce), CipherB64: base64.StdEncoding.EncodeToString(cipherText)}
}
