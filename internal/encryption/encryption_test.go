package encryption

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func testKeyB64() string {
	return base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
}

func TestStaticEnvEncryptDecryptRoundTrip(t *testing.T) {
	ctx := context.Background()
	svc, err := NewService(ctx, Config{AtRest: ModeEnabled, KEKProvider: ProviderStaticEnv, StaticKeyB64: testKeyB64()})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	plaintext := []byte("distinctive user plaintext")
	aad := []byte("scope:a")
	ciphertext, err := svc.EncryptRecord(ctx, plaintext, aad)
	if err != nil {
		t.Fatalf("EncryptRecord() error = %v", err)
	}
	if bytes.Contains(ciphertext, plaintext) {
		t.Fatalf("ciphertext contains plaintext: %q", ciphertext)
	}
	got, err := svc.DecryptRecord(ctx, ciphertext, aad)
	if err != nil {
		t.Fatalf("DecryptRecord() error = %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("decrypt=%q want %q", got, plaintext)
	}
}

func TestDecryptWrongAADFails(t *testing.T) {
	ctx := context.Background()
	svc, err := NewService(ctx, Config{AtRest: ModeEnabled, KEKProvider: ProviderStaticEnv, StaticKeyB64: testKeyB64()})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	ciphertext, err := svc.EncryptRecord(ctx, []byte("secret"), []byte("scope:a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DecryptRecord(ctx, ciphertext, []byte("scope:b")); err == nil {
		t.Fatal("expected wrong AAD to fail")
	}
}

func TestStaticFileProvider(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "kek.b64")
	if err := os.WriteFile(path, []byte(testKeyB64()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	svc, err := NewService(ctx, Config{AtRest: ModeEnabled, KEKProvider: ProviderStaticFile, StaticKeyFile: path})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	ciphertext, err := svc.EncryptRecord(ctx, []byte("secret"), []byte("scope"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.DecryptRecord(ctx, ciphertext, []byte("scope"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "secret" {
		t.Fatalf("got %q", got)
	}
}

func TestEnabledRequiresProvider(t *testing.T) {
	if _, err := NewService(context.Background(), Config{AtRest: ModeEnabled}); err == nil {
		t.Fatal("expected missing provider error")
	}
}

func TestEnabledRejectsPlaintextPayload(t *testing.T) {
	svc, err := NewService(context.Background(), Config{AtRest: ModeEnabled, KEKProvider: ProviderStaticEnv, StaticKeyB64: testKeyB64()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DecryptRecord(context.Background(), []byte("plaintext"), []byte("scope")); err == nil {
		t.Fatal("expected plaintext rejection")
	}
}
