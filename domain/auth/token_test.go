package auth

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestNewRefreshToken(t *testing.T) {
	if _, err := NewRefreshToken(31); err == nil {
		t.Fatal("expected token byte length validation error")
	}

	first, err := NewRefreshToken(32)
	if err != nil {
		t.Fatalf("new refresh token failed: %v", err)
	}
	second, err := NewRefreshToken(32)
	if err != nil {
		t.Fatalf("second new refresh token failed: %v", err)
	}
	if first == "" || second == "" {
		t.Fatal("expected non-empty tokens")
	}
	if first == second {
		t.Fatal("expected independently generated tokens to differ")
	}
	if strings.ContainsAny(string(first), "=+/ ") {
		t.Fatalf("expected unpadded base64url token, got %q", first)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(string(first))
	if err != nil {
		t.Fatalf("decode token failed: %v", err)
	}
	if len(decoded) != 32 {
		t.Fatalf("expected 32 random bytes, got %d", len(decoded))
	}
}

func TestHashAndVerifyRefreshToken(t *testing.T) {
	token, err := NewRefreshToken(32)
	if err != nil {
		t.Fatalf("new refresh token failed: %v", err)
	}
	hash, err := HashRefreshToken(token)
	if err != nil {
		t.Fatalf("hash refresh token failed: %v", err)
	}
	if !strings.HasPrefix(hash, RefreshTokenHashAlgorithmSHA256+":") {
		t.Fatalf("expected algorithm prefix, got %q", hash)
	}
	if len(strings.TrimPrefix(hash, RefreshTokenHashAlgorithmSHA256+":")) != 64 {
		t.Fatalf("expected sha256 hex digest, got %q", hash)
	}
	if strings.Contains(hash, string(token)) {
		t.Fatalf("hash must not contain plaintext token")
	}
	if !IsRefreshTokenHash(hash) {
		t.Fatalf("expected hash to validate")
	}
	if !VerifyRefreshTokenHash(token, hash) {
		t.Fatal("expected token to verify against hash")
	}
	upperHexHash := RefreshTokenHashAlgorithmSHA256 + ":" + strings.ToUpper(strings.TrimPrefix(hash, RefreshTokenHashAlgorithmSHA256+":"))
	if !VerifyRefreshTokenHash(token, upperHexHash) {
		t.Fatal("expected token to verify against uppercase hex hash")
	}
	other, err := NewRefreshToken(32)
	if err != nil {
		t.Fatalf("new other token failed: %v", err)
	}
	if VerifyRefreshTokenHash(other, hash) {
		t.Fatal("expected different token not to verify")
	}
	if VerifyRefreshTokenHash(token, "sha256:not-hex") {
		t.Fatal("expected malformed hash not to verify")
	}
	if VerifyRefreshTokenHash("", hash) {
		t.Fatal("expected empty token not to verify")
	}
}

func TestHashRefreshTokenRejectsEmptyOrWhitespace(t *testing.T) {
	for _, token := range []RefreshToken{"", " token", "token ", "token\n"} {
		if _, err := HashRefreshToken(token); err == nil {
			t.Fatalf("expected error for token %q", token)
		}
	}
}
