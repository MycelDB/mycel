package membership

import "testing"

func TestGenerateTokenUniqueAndVerifiable(t *testing.T) {
	a, err := GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	b, err := GenerateToken()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("tokens should be unique")
	}
	if err := ValidateTokenFormat(a); err != nil {
		t.Fatal(err)
	}
	h := HashToken(a)
	if h == a {
		t.Fatal("hash should not equal plaintext token")
	}
	if !VerifyToken(h, a) {
		t.Fatal("expected token verification")
	}
	if VerifyToken(h, b) {
		t.Fatal("unexpected verification for different token")
	}
}
