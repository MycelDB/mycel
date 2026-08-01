package routing

import (
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/consensus"
)

func TestRoutedSessionAndTransactionIDsEncodeHomeNode(t *testing.T) {
	sessionID := NewSessionID(consensus.NodeID(7))
	if !strings.HasPrefix(sessionID, "s.7.") {
		t.Fatalf("session id %q does not encode home node", sessionID)
	}
	home, ok, err := ParseSessionHomeNode(sessionID)
	if err != nil || !ok || home != 7 {
		t.Fatalf("ParseSessionHomeNode()=(%d,%v,%v), want (7,true,nil)", home, ok, err)
	}

	txID := NewTransactionID(consensus.NodeID(9))
	if !strings.HasPrefix(txID, "tx.9.") {
		t.Fatalf("transaction id %q does not encode home node", txID)
	}
	home, ok, err = ParseTransactionHomeNode(txID)
	if err != nil || !ok || home != 9 {
		t.Fatalf("ParseTransactionHomeNode()=(%d,%v,%v), want (9,true,nil)", home, ok, err)
	}
}

func TestRoutedIDStandaloneRemainsUUIDCompatible(t *testing.T) {
	sessionID := NewSessionID(0)
	if strings.Contains(sessionID, ".") {
		t.Fatalf("standalone session id %q should remain UUID-compatible", sessionID)
	}
	home, ok, err := ParseSessionHomeNode(sessionID)
	if err != nil || ok || home != 0 {
		t.Fatalf("ParseSessionHomeNode(standalone)=(%d,%v,%v), want (0,false,nil)", home, ok, err)
	}
}

func TestParseRoutedIDRejectsMalformedPrefixedIDs(t *testing.T) {
	if _, _, err := ParseSessionHomeNode("s.nope.00000000-0000-0000-0000-000000000001"); err == nil {
		t.Fatal("expected malformed session home node to fail")
	}
	if _, _, err := ParseTransactionHomeNode("tx.1.not-a-uuid"); err == nil {
		t.Fatal("expected malformed transaction uuid to fail")
	}
	if _, _, err := ParseSessionHomeNode("tx.1.00000000-0000-0000-0000-000000000001"); err == nil {
		t.Fatal("expected wrong prefix to fail")
	}
}
