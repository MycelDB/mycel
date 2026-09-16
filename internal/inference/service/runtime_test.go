package service

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/encryption"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

func TestEnvelopeSecretResolverRoundTrip(t *testing.T) {
	ctx := context.Background()
	enc := testEncryptionService(t)
	ciphertext, err := enc.EncryptRecord(ctx, []byte("sk-test-secret"), []byte("inference-secret:v1"))
	if err != nil {
		t.Fatalf("EncryptRecord() error = %v", err)
	}
	resolver := NewEnvelopeSecretResolver(enc)
	got, err := resolver.ResolveSecret(ctx, domaininference.Secret{Kind: "inline_encrypted", Ciphertext: &domaininference.EncryptedSecretPayload{Algorithm: "MYCEL-ENVELOPE-V1", CipherB64: base64.StdEncoding.EncodeToString(ciphertext)}})
	if err != nil {
		t.Fatalf("ResolveSecret() error = %v", err)
	}
	if got != "sk-test-secret" {
		t.Fatalf("ResolveSecret() = %q", got)
	}
}

func TestEnvelopeSecretResolverRejectsLegacyAESPayload(t *testing.T) {
	resolver := NewEnvelopeSecretResolver(testEncryptionService(t))
	_, err := resolver.ResolveSecret(context.Background(), domaininference.Secret{Kind: "inline_encrypted", Ciphertext: &domaininference.EncryptedSecretPayload{Algorithm: "AES-256-GCM", NonceB64: "nonce", CipherB64: "cipher"}})
	if err == nil || !strings.Contains(err.Error(), "unsupported secret encryption algorithm") {
		t.Fatalf("ResolveSecret() error = %v, want unsupported algorithm", err)
	}
}

func testEncryptionService(t *testing.T) *encryption.Service {
	t.Helper()
	enc, err := encryption.NewService(context.Background(), encryption.Config{AtRest: encryption.ModeEnabled, KEKProvider: encryption.ProviderStaticEnv, StaticKeyB64: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return enc
}
