package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
)

const (
	// RefreshTokenHashAlgorithmSHA256 is the initial refresh-token hash format.
	RefreshTokenHashAlgorithmSHA256 = "sha256"
	minRefreshTokenBytes            = 32
)

// RefreshToken is a plaintext bearer token returned once to callers. It must
// never be persisted; store only HashRefreshToken output.
type RefreshToken string

// NewRefreshToken returns a base64url-encoded cryptographically random refresh
// token. byteLen is the amount of random entropy before encoding and must be at
// least 32 bytes.
func NewRefreshToken(byteLen int) (RefreshToken, error) {
	if byteLen < minRefreshTokenBytes {
		return "", fmt.Errorf("refresh token byte length must be at least %d", minRefreshTokenBytes)
	}
	raw := make([]byte, byteLen)
	if _, err := io.ReadFull(rand.Reader, raw); err != nil {
		return "", err
	}
	return RefreshToken(base64.RawURLEncoding.EncodeToString(raw)), nil
}

// HashRefreshToken returns an algorithm-prefixed SHA-256 hash for a plaintext
// refresh token.
func HashRefreshToken(token RefreshToken) (string, error) {
	plain := string(token)
	if plain == "" || strings.TrimSpace(plain) != plain {
		return "", fmt.Errorf("refresh token is required")
	}
	sum := sha256.Sum256([]byte(plain))
	return RefreshTokenHashAlgorithmSHA256 + ":" + hex.EncodeToString(sum[:]), nil
}

// IsRefreshTokenHash reports whether hash is a supported algorithm-prefixed
// refresh-token hash.
func IsRefreshTokenHash(hash string) bool {
	_, ok := normalizeRefreshTokenHash(hash)
	return ok
}

// VerifyRefreshTokenHash verifies a plaintext refresh token against a supported
// stored hash using constant-time comparison for the normalized hash strings.
func VerifyRefreshTokenHash(token RefreshToken, storedHash string) bool {
	computed, err := HashRefreshToken(token)
	if err != nil {
		return false
	}
	normalized, ok := normalizeRefreshTokenHash(storedHash)
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(computed), []byte(normalized)) == 1
}

func normalizeRefreshTokenHash(hash string) (string, bool) {
	hash = strings.TrimSpace(hash)
	algorithm, encoded, ok := strings.Cut(hash, ":")
	if !ok || algorithm != RefreshTokenHashAlgorithmSHA256 {
		return "", false
	}
	encoded = strings.ToLower(strings.TrimSpace(encoded))
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != encoded {
		return "", false
	}
	return algorithm + ":" + encoded, true
}
