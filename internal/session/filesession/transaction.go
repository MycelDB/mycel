package filesession

import (
	"context"
	"fmt"

	sessionapi "martinbeauvais.com/mbgit/knotbase/knotdb/session/api"
)

// Begin starts a session transaction. Phase 1 exposes the public API only; the
// file-backed transaction overlay and durable commit implementation are planned
// for later phases.
func (s *FileSession) Begin(ctx context.Context, opts sessionapi.TxOptions) (sessionapi.Tx, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if opts.ReadOnly {
		if err := s.ensureRead(); err != nil {
			return nil, err
		}
	} else if err := s.ensureWrite(); err != nil {
		return nil, err
	}
	return nil, sessionapi.ErrTransactionsUnsupported
}

// Tx runs fn inside a session transaction. Until Begin is implemented, this
// returns ErrTransactionsUnsupported and does not invoke fn.
func (s *FileSession) Tx(ctx context.Context, opts sessionapi.TxOptions, fn func(sessionapi.Tx) error) error {
	tx, err := s.Begin(ctx, opts)
	if err != nil {
		return err
	}
	if fn == nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("transaction callback is required")
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
