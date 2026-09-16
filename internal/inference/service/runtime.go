package service

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/encryption"
	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

type SecretResolver interface {
	ResolveSecret(ctx context.Context, secret domaininference.Secret) (string, error)
}

type EnvelopeSecretResolver struct {
	encryption *encryption.Service
}

func NewEnvelopeSecretResolver(enc *encryption.Service) EnvelopeSecretResolver {
	return EnvelopeSecretResolver{encryption: enc}
}

func (r EnvelopeSecretResolver) ResolveSecret(ctx context.Context, secret domaininference.Secret) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	switch strings.TrimSpace(secret.Kind) {
	case "", "none":
		return "", nil
	case "inline_encrypted":
		if secret.Ciphertext == nil || strings.TrimSpace(secret.Ciphertext.CipherB64) == "" {
			return "", fmt.Errorf("encrypted secret payload is empty")
		}
		if !strings.EqualFold(strings.TrimSpace(secret.Ciphertext.Algorithm), "MYCEL-ENVELOPE-V1") {
			return "", fmt.Errorf("unsupported secret encryption algorithm %q", secret.Ciphertext.Algorithm)
		}
		if r.encryption == nil || !r.encryption.Enabled() {
			return "", fmt.Errorf("envelope encrypted secret requires encryption at rest")
		}
		payload, err := base64.StdEncoding.DecodeString(secret.Ciphertext.CipherB64)
		if err != nil {
			return "", fmt.Errorf("decode secret ciphertext: %w", err)
		}
		plain, err := r.encryption.DecryptRecord(ctx, payload, []byte("inference-secret:v1"))
		if err != nil {
			return "", fmt.Errorf("decrypt secret: %w", err)
		}
		return string(plain), nil
	default:
		return "", fmt.Errorf("unsupported secret kind %q", secret.Kind)
	}
}

func (m *Module) SetConnector(typ domaininference.ConnectorType, connector connectors.Connector) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.connectors == nil {
		m.connectors = defaultConnectors()
	}
	if connector == nil {
		delete(m.connectors, typ)
		return
	}
	m.connectors[typ] = connector
}

func (m *Module) SetSecretResolver(resolver SecretResolver) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.secretResolver = resolver
	if m.secretResolver == nil {
		m.secretResolver = EnvelopeSecretResolver{}
	}
}

func (m *Module) connector(typ domaininference.ConnectorType) connectors.Connector {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.connectors == nil {
		m.connectors = defaultConnectors()
	}
	return m.connectors[typ]
}

func (m *Module) resolveSecret(ctx context.Context, secret domaininference.Secret) (string, error) {
	m.mu.Lock()
	resolver := m.secretResolver
	m.mu.Unlock()
	if resolver == nil {
		resolver = EnvelopeSecretResolver{}
	}
	return resolver.ResolveSecret(ctx, secret)
}

func defaultConnectors() map[domaininference.ConnectorType]connectors.Connector {
	return map[domaininference.ConnectorType]connectors.Connector{
		domaininference.ConnectorOpenAICompatible: connectors.OpenAICompatible{},
		domaininference.ConnectorFake:             &connectors.FakeConnector{},
	}
}
