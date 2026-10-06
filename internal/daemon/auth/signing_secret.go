package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const SigningSecretBytes = 32

// NewPersistentTokenManager returns a token manager backed by either an inline
// base64 secret, an existing secret file, or a newly generated secret written to
// secretFile. The returned manager can verify access tokens minted by later
// daemon processes using the same secret material.
func NewPersistentTokenManager(secretB64 string, secretFile string, ttl time.Duration) (*TokenManager, error) {
	secret, err := LoadOrCreateSigningSecret(secretB64, secretFile)
	if err != nil {
		return nil, err
	}
	return NewTokenManager(secret, ttl), nil
}

func LoadOrCreateSigningSecret(secretB64 string, secretFile string) ([]byte, error) {
	secretB64 = strings.TrimSpace(secretB64)
	secretFile = strings.TrimSpace(secretFile)
	if secretB64 != "" {
		secret, err := decodeSigningSecret(secretB64)
		if err != nil {
			return nil, fmt.Errorf("MYCELD_ACCESS_TOKEN_SIGNING_SECRET_B64: %w", err)
		}
		return secret, nil
	}
	if secretFile == "" {
		return nil, fmt.Errorf("access-token signing secret file path is required when no inline secret is configured")
	}
	if secret, err := readSigningSecretFile(secretFile); err == nil {
		return secret, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	secret := make([]byte, SigningSecretBytes)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("generate access-token signing secret: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(secretFile), 0o700); err != nil {
		return nil, fmt.Errorf("create access-token signing secret directory: %w", err)
	}
	encoded := base64.RawStdEncoding.EncodeToString(secret) + "\n"
	if err := writeNewSigningSecretFile(secretFile, []byte(encoded)); err != nil {
		if errors.Is(err, os.ErrExist) {
			secret, readErr := readSigningSecretFile(secretFile)
			if readErr != nil {
				return nil, readErr
			}
			return secret, nil
		}
		return nil, fmt.Errorf("write access-token signing secret %s: %w", secretFile, err)
	}
	return secret, nil
}

func readSigningSecretFile(secretFile string) ([]byte, error) {
	raw, err := os.ReadFile(secretFile)
	if err != nil {
		return nil, fmt.Errorf("read access-token signing secret %s: %w", secretFile, err)
	}
	secret, err := decodeSigningSecret(string(raw))
	if err != nil {
		return nil, fmt.Errorf("read access-token signing secret %s: %w", secretFile, err)
	}
	return secret, nil
}

func writeNewSigningSecretFile(secretFile string, encoded []byte) error {
	dir := filepath.Dir(secretFile)
	file, err := os.CreateTemp(dir, ".access-token-signing-secret-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := file.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Link(tmpPath, secretFile); err != nil {
		return err
	}
	if err := os.Remove(tmpPath); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func decodeSigningSecret(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("secret is empty")
	}
	secret, err := base64.RawStdEncoding.DecodeString(value)
	if err != nil {
		secret, err = base64.StdEncoding.DecodeString(value)
	}
	if err != nil {
		return nil, fmt.Errorf("secret must be base64 encoded: %w", err)
	}
	if len(secret) < SigningSecretBytes {
		return nil, fmt.Errorf("secret must decode to at least %d bytes", SigningSecretBytes)
	}
	return secret, nil
}
