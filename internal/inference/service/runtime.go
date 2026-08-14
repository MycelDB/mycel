package service

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

type SecretResolver interface {
	ResolveSecret(ctx context.Context, secret domaininference.Secret) (string, error)
}

type EnvSecretResolver struct{}

func (EnvSecretResolver) ResolveSecret(ctx context.Context, secret domaininference.Secret) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	switch strings.TrimSpace(secret.Kind) {
	case "", "none":
		return "", nil
	case "external_ref":
		ref := strings.TrimSpace(secret.ExternalRef)
		if !strings.HasPrefix(ref, "env://") {
			return "", fmt.Errorf("unsupported external secret reference")
		}
		name := strings.TrimSpace(strings.TrimPrefix(ref, "env://"))
		if name == "" {
			return "", fmt.Errorf("environment secret reference is empty")
		}
		value := os.Getenv(name)
		if value == "" {
			return "", fmt.Errorf("environment secret %s is not set", name)
		}
		return value, nil
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
		m.secretResolver = EnvSecretResolver{}
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
		resolver = EnvSecretResolver{}
	}
	return resolver.ResolveSecret(ctx, secret)
}

func defaultConnectors() map[domaininference.ConnectorType]connectors.Connector {
	return map[domaininference.ConnectorType]connectors.Connector{
		domaininference.ConnectorOpenAICompatible: connectors.OpenAICompatible{},
		domaininference.ConnectorFake:             &connectors.FakeConnector{},
	}
}
