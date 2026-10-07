package auth

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPersistentTokenManagerReusesGeneratedSigningSecret(t *testing.T) {
	secretFile := filepath.Join(t.TempDir(), "secrets", "access-token-signing-secret.b64")
	first, err := NewPersistentTokenManager("", secretFile, time.Minute)
	if err != nil {
		t.Fatalf("NewPersistentTokenManager(first) error = %v", err)
	}
	principal := Principal{Kind: PrincipalKindHuman, PrincipalID: "principal-1", AuthSessionID: "session-1", Username: "alice", CreatedAt: time.Unix(10, 0).UTC()}
	token, _, err := first.Issue(principal)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	if info, err := os.Stat(secretFile); err != nil {
		t.Fatalf("secret file was not created: %v", err)
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("secret file mode = %v, want 0600", info.Mode().Perm())
	}

	second, err := NewPersistentTokenManager("", secretFile, time.Minute)
	if err != nil {
		t.Fatalf("NewPersistentTokenManager(second) error = %v", err)
	}
	verified, err := second.Verify(token)
	if err != nil {
		t.Fatalf("Verify() after manager restart error = %v", err)
	}
	if verified.PrincipalID != principal.PrincipalID || verified.AuthSessionID != principal.AuthSessionID || verified.Username != principal.Username {
		t.Fatalf("unexpected verified principal: %+v", verified)
	}
}

func TestPersistentTokenManagerInlineSecretOverridesFile(t *testing.T) {
	fileSecret := make([]byte, SigningSecretBytes)
	for i := range fileSecret {
		fileSecret[i] = byte(i + 1)
	}
	secretFile := filepath.Join(t.TempDir(), "token.b64")
	if err := os.WriteFile(secretFile, []byte(base64.RawStdEncoding.EncodeToString(fileSecret)), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	inlineSecret := make([]byte, SigningSecretBytes)
	for i := range inlineSecret {
		inlineSecret[i] = byte(200 - i)
	}
	inlineB64 := base64.RawStdEncoding.EncodeToString(inlineSecret)
	issuer, err := NewPersistentTokenManager(inlineB64, secretFile, time.Minute)
	if err != nil {
		t.Fatalf("NewPersistentTokenManager(inline) error = %v", err)
	}
	token, _, err := issuer.Issue(Principal{PrincipalID: "principal-1", Username: "alice", CreatedAt: time.Unix(10, 0).UTC()})
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	fileBacked, err := NewPersistentTokenManager("", secretFile, time.Minute)
	if err != nil {
		t.Fatalf("NewPersistentTokenManager(file) error = %v", err)
	}
	if _, err := fileBacked.Verify(token); err == nil {
		t.Fatal("file-backed manager verified token signed by inline override secret")
	}
}

func TestLoadOrCreateSigningSecretRejectsShortSecret(t *testing.T) {
	short := base64.RawStdEncoding.EncodeToString([]byte("too-short"))
	if _, err := LoadOrCreateSigningSecret(short, ""); err == nil {
		t.Fatal("expected short inline signing secret to fail")
	}
}

func TestLoadOrCreateSigningSecretConcurrentCreateReusesWinner(t *testing.T) {
	secretFile := filepath.Join(t.TempDir(), "secrets", "access-token-signing-secret.b64")
	const workers = 16
	secrets := make([][]byte, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			secrets[i], errs[i] = LoadOrCreateSigningSecret("", secretFile)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("LoadOrCreateSigningSecret(%d) error = %v", i, err)
		}
	}
	for i := 1; i < workers; i++ {
		if !bytes.Equal(secrets[0], secrets[i]) {
			t.Fatalf("worker %d returned a different secret", i)
		}
	}
	fromFile, err := LoadOrCreateSigningSecret("", secretFile)
	if err != nil {
		t.Fatalf("LoadOrCreateSigningSecret(file) error = %v", err)
	}
	if !bytes.Equal(secrets[0], fromFile) {
		t.Fatal("file secret differs from generated winner")
	}
}
