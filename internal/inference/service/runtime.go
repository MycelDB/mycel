package service

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

type SecretResolver interface {
	ResolveSecret(ctx context.Context, secret domaininference.Secret) (string, error)
}

type EncryptedSecretResolver struct {
	secretKeyB64 string
}

func NewEncryptedSecretResolver(secretKeyB64 string) EncryptedSecretResolver {
	return EncryptedSecretResolver{secretKeyB64: secretKeyB64}
}

func (r EncryptedSecretResolver) ResolveSecret(ctx context.Context, secret domaininference.Secret) (string, error) {
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
		if !strings.EqualFold(strings.TrimSpace(secret.Ciphertext.Algorithm), "AES-256-GCM") {
			return "", fmt.Errorf("unsupported secret encryption algorithm %q", secret.Ciphertext.Algorithm)
		}
		key, err := base64.StdEncoding.DecodeString(r.secretKeyB64)
		if err != nil || len(key) != 32 {
			return "", fmt.Errorf("valid 32-byte secret encryption key is required")
		}
		nonce, err := base64.StdEncoding.DecodeString(secret.Ciphertext.NonceB64)
		if err != nil {
			return "", fmt.Errorf("decode secret nonce: %w", err)
		}
		ciphertext, err := base64.StdEncoding.DecodeString(secret.Ciphertext.CipherB64)
		if err != nil {
			return "", fmt.Errorf("decode secret ciphertext: %w", err)
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			return "", err
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return "", err
		}
		plain, err := gcm.Open(nil, nonce, ciphertext, nil)
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
		m.secretResolver = EncryptedSecretResolver{}
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
		resolver = EncryptedSecretResolver{}
	}
	return resolver.ResolveSecret(ctx, secret)
}

func defaultConnectors() map[domaininference.ConnectorType]connectors.Connector {
	return map[domaininference.ConnectorType]connectors.Connector{
		domaininference.ConnectorOpenAICompatible: connectors.OpenAICompatible{},
		domaininference.ConnectorFake:             &connectors.FakeConnector{},
	}
}
