package connectors

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	storeaccounting "github.com/myceldb/mycel/internal/semantic/accounting"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type Service struct {
	GlobalManager    storesemantic.GlobalManager
	Accounting       storeaccounting.Manager
	SecretKeyB64     string
	Connectors       map[domainsemantic.ConnectorType]Connector
	ActorPrincipalID identity.PrincipalID
}

type EmbedInput struct {
	ModelEndpointID       domainsemantic.ModelEndpointID
	ModelID               domainsemantic.InferenceModelID
	CredentialID          domainsemantic.InferenceCredentialID
	CredentialGrantID     domainsemantic.CredentialGrantID
	PolicyDecisionID      domainsemantic.PolicyDecisionID
	SpaceID               domainspace.SpaceID
	DomainID              graph.DomainID
	SemanticIndexID       domainsemantic.SemanticIndexID
	TargetNodeID          graph.NodeID
	ActorPrincipalID      identity.PrincipalID
	EffectivePrincipalID  identity.PrincipalID
	OnBehalfOfPrincipalID identity.PrincipalID
	Input                 string
	Reason                string
}

func (s Service) Embed(ctx context.Context, in EmbedInput) (EmbeddingResponse, error) {
	endpoint, model, cap, credential, err := s.resolve(ctx, in)
	if err != nil {
		return EmbeddingResponse{}, err
	}
	secret, err := s.ResolveCredentialSecret(ctx, credential)
	if err != nil {
		return EmbeddingResponse{}, err
	}
	connector := s.connector(endpoint.ConnectorType)
	if connector == nil {
		return EmbeddingResponse{}, fmt.Errorf("connector %q is not registered", endpoint.ConnectorType)
	}
	base := s.accountingBase(in, endpoint, model, cap, credential)
	resp, callErr := connector.Embed(ctx, EmbeddingRequest{Endpoint: endpoint, Model: model, Capability: cap, Credential: credential, Secret: secret, Input: in.Input})
	event := base
	event.ProviderRequestID = resp.ProviderRequestID
	event.InputTokens = resp.InputTokens
	event.OutputTokens = resp.OutputTokens
	event.TotalTokens = resp.TotalTokens
	event.TokenCountSource = resp.TokenCountSource
	if event.TokenCountSource == "" {
		event.TokenCountSource = "unavailable"
	}
	if callErr != nil {
		event.Status = "failed"
		event.ErrorMessage = callErr.Error()
	} else {
		event.Status = "success"
	}
	if s.Accounting != nil {
		if _, err := s.Accounting.Append(ctx, event); err != nil {
			if callErr != nil {
				return resp, fmt.Errorf("%w; additionally failed to append accounting event: %v", callErr, err)
			}
			return EmbeddingResponse{}, err
		}
	}
	return resp, callErr
}

func (s Service) ResolveCredentialSecret(ctx context.Context, credential domainsemantic.InferenceCredential) (string, error) {
	if credential.Status != domainsemantic.CredentialStatusActive {
		return "", fmt.Errorf("credential %s is not active", credential.ID)
	}
	if credential.AuthType == domainsemantic.AuthModeNone {
		return "", nil
	}
	if credential.SecretRef == uuid.Nil {
		return "", fmt.Errorf("credential %s has no secret", credential.ID)
	}
	secrets, err := s.GlobalManager.ListSecrets(ctx)
	if err != nil {
		return "", err
	}
	for _, secret := range secrets {
		if secret.ID != credential.SecretRef {
			continue
		}
		switch secret.Kind {
		case domainsemantic.SecretKindExternalRef:
			return resolveExternalSecret(secret.ExternalRef)
		case domainsemantic.SecretKindInlineEncrypted:
			if secret.Ciphertext == nil {
				return "", fmt.Errorf("secret %s is missing encrypted payload", secret.ID)
			}
			return decryptInlineSecret(s.SecretKeyB64, *secret.Ciphertext)
		default:
			return "", fmt.Errorf("unsupported secret kind %q", secret.Kind)
		}
	}
	return "", fmt.Errorf("secret %s not found", credential.SecretRef)
}

func (s Service) resolve(ctx context.Context, in EmbedInput) (domainsemantic.ModelEndpoint, domainsemantic.InferenceModel, domainsemantic.ModelEndpointCapability, domainsemantic.InferenceCredential, error) {
	if s.GlobalManager == nil {
		return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, domainsemantic.InferenceCredential{}, fmt.Errorf("global semantic manager is required")
	}
	endpoint, err := findEndpoint(ctx, s.GlobalManager, in.ModelEndpointID)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, domainsemantic.InferenceCredential{}, err
	}
	model, err := findModel(ctx, s.GlobalManager, in.ModelID)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, domainsemantic.InferenceCredential{}, err
	}
	cap, err := findCapability(ctx, s.GlobalManager, endpoint.ID, model.ID, domainsemantic.OperationEmbeddings)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, domainsemantic.InferenceCredential{}, err
	}
	credential, err := findCredential(ctx, s.GlobalManager, in.CredentialID)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, domainsemantic.InferenceCredential{}, err
	}
	return endpoint, model, cap, credential, nil
}

func (s Service) connector(typ domainsemantic.ConnectorType) Connector {
	if s.Connectors != nil && s.Connectors[typ] != nil {
		return s.Connectors[typ]
	}
	if typ == domainsemantic.ConnectorOpenAICompatible {
		return OpenAICompatible{}
	}
	return nil
}

func (s Service) accountingBase(in EmbedInput, endpoint domainsemantic.ModelEndpoint, model domainsemantic.InferenceModel, cap domainsemantic.ModelEndpointCapability, credential domainsemantic.InferenceCredential) domainsemantic.InferenceUsageEvent {
	actor := in.ActorPrincipalID
	if actor == "" {
		actor = s.ActorPrincipalID
	}
	return domainsemantic.InferenceUsageEvent{CreatedAt: time.Now().UTC(), Operation: string(domainsemantic.OperationEmbeddings), Reason: in.Reason, ActorPrincipalID: actor, EffectivePrincipalID: in.EffectivePrincipalID, OnBehalfOfPrincipalID: in.OnBehalfOfPrincipalID, SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticIndexID: in.SemanticIndexID, TargetNodeID: in.TargetNodeID, ModelEndpointID: endpoint.ID, ModelEndpointKey: endpoint.Key, ModelID: model.ID, ModelKey: model.Key, ModelEndpointCapabilityID: cap.ID, CredentialID: credential.ID, CredentialGrantID: in.CredentialGrantID, PolicyDecisionID: in.PolicyDecisionID}
}

func findEndpoint(ctx context.Context, mgr storesemantic.GlobalManager, id domainsemantic.ModelEndpointID) (domainsemantic.ModelEndpoint, error) {
	items, err := mgr.ListModelEndpoints(ctx)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, err
	}
	for _, item := range items {
		if item.ID == id && item.Enabled {
			return item, nil
		}
	}
	return domainsemantic.ModelEndpoint{}, fmt.Errorf("enabled model endpoint %s not found", id)
}

func findModel(ctx context.Context, mgr storesemantic.GlobalManager, id domainsemantic.InferenceModelID) (domainsemantic.InferenceModel, error) {
	items, err := mgr.ListModels(ctx)
	if err != nil {
		return domainsemantic.InferenceModel{}, err
	}
	for _, item := range items {
		if item.ID == id && item.Operation == domainsemantic.OperationEmbeddings {
			return item, nil
		}
	}
	return domainsemantic.InferenceModel{}, fmt.Errorf("embedding model %s not found", id)
}

func findCapability(ctx context.Context, mgr storesemantic.GlobalManager, endpointID domainsemantic.ModelEndpointID, modelID domainsemantic.InferenceModelID, op domainsemantic.Operation) (domainsemantic.ModelEndpointCapability, error) {
	items, err := mgr.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return domainsemantic.ModelEndpointCapability{}, err
	}
	for _, item := range items {
		if item.ModelEndpointID == endpointID && item.ModelID == modelID && item.Operation == op && item.Enabled {
			return item, nil
		}
	}
	return domainsemantic.ModelEndpointCapability{}, fmt.Errorf("enabled capability not found for endpoint=%s model=%s operation=%s", endpointID, modelID, op)
}

func findCredential(ctx context.Context, mgr storesemantic.GlobalManager, id domainsemantic.InferenceCredentialID) (domainsemantic.InferenceCredential, error) {
	items, err := mgr.ListCredentials(ctx)
	if err != nil {
		return domainsemantic.InferenceCredential{}, err
	}
	for _, item := range items {
		if item.ID == id {
			if item.Status != domainsemantic.CredentialStatusActive {
				return domainsemantic.InferenceCredential{}, fmt.Errorf("credential %s is not active", id)
			}
			return item, nil
		}
	}
	return domainsemantic.InferenceCredential{}, fmt.Errorf("credential %s not found", id)
}

func resolveExternalSecret(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if strings.HasPrefix(ref, "env://") {
		name := strings.TrimSpace(strings.TrimPrefix(ref, "env://"))
		value := os.Getenv(name)
		if value == "" {
			return "", fmt.Errorf("environment secret %s is not set", name)
		}
		return value, nil
	}
	if strings.HasPrefix(ref, "env:") {
		name := strings.TrimSpace(strings.TrimPrefix(ref, "env:"))
		if name == "" {
			return "", fmt.Errorf("environment secret reference is empty")
		}
		value := os.Getenv(name)
		if value == "" {
			return "", fmt.Errorf("environment secret %s is not set", name)
		}
		return value, nil
	}
	return "", fmt.Errorf("unsupported external secret reference %q", ref)
}

func decryptInlineSecret(keyB64 string, payload domainsemantic.EncryptedSecretPayload) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(keyB64))
	if err != nil || len(decoded) != 32 {
		return "", fmt.Errorf("inline secret decryption requires a 32-byte base64 key")
	}
	nonce, err := base64.StdEncoding.DecodeString(payload.NonceB64)
	if err != nil {
		return "", err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(payload.CipherB64)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(decoded)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
