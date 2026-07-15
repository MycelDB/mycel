package membership

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

const tokenPrefix = "mycel_join_v1_"

func GenerateToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

func HashToken(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func VerifyToken(hash string, token string) bool {
	expected := strings.TrimSpace(hash)
	if expected == "" {
		return false
	}
	actual := HashToken(token)
	return subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) == 1
}

func ValidateTokenFormat(token string) error {
	if !strings.HasPrefix(strings.TrimSpace(token), tokenPrefix) {
		return fmt.Errorf("join token must have %s prefix", tokenPrefix)
	}
	return nil
}
