package wal

import "context"

// ReadNextBlocking returns the next record at or after lsn. If the requested
// LSN has not been committed yet, it waits for a future append or context
// cancellation. This is intended for future replica streaming loops.
func (m *Manager) ReadNextBlocking(ctx context.Context, lsn LSN) (Record, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return Record{}, false, err
		}
		last := m.LastCommittedLSN()
		if last >= lsn {
			it, err := m.ReadFrom(ctx, lsn)
			if err != nil {
				return Record{}, false, err
			}
			rec, ok, err := it.Next()
			_ = it.Close()
			return rec, ok, err
		}
		if err := m.WaitUntilCommitted(ctx, lsn); err != nil {
			return Record{}, false, err
		}
	}
}
