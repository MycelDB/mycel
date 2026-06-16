package filesession

import (
	"context"
	"errors"
	"testing"

	sessionapi "martinbeauvais.com/mbgit/knotbase/knotdb/session/api"
)

func TestFileSessionTransactionPhase1Stubs(t *testing.T) {
	sess, _ := newHierarchyTestSession(t)
	ctx := context.Background()

	tx, err := sess.Begin(ctx, sessionapi.TxOptions{})
	if tx != nil {
		t.Fatalf("expected nil transaction while phase 1 stub is active")
	}
	if !errors.Is(err, sessionapi.ErrTransactionsUnsupported) {
		t.Fatalf("expected ErrTransactionsUnsupported, got %v", err)
	}

	called := false
	err = sess.Tx(ctx, sessionapi.TxOptions{}, func(tx sessionapi.Tx) error {
		called = true
		return nil
	})
	if called {
		t.Fatalf("transaction callback should not be called while Begin is unsupported")
	}
	if !errors.Is(err, sessionapi.ErrTransactionsUnsupported) {
		t.Fatalf("expected ErrTransactionsUnsupported from Tx, got %v", err)
	}
}
