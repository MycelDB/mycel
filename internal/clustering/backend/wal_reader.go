package backend

import (
	"context"
	"io"

	"github.com/myceldb/mycel/internal/clustering/replsnapshot"
	"github.com/myceldb/mycel/internal/wal"
)

type WALReader interface {
	ReadFrom(ctx context.Context, lsn wal.LSN) (*wal.Iterator, error)
	ReadNextBlocking(ctx context.Context, lsn wal.LSN) (wal.Record, bool, error)
	RetainedRange(ctx context.Context) (wal.RetainedRange, error)
}

type CheckpointProvider interface {
	Load(ctx context.Context) (wal.Checkpoint, error)
}

type SnapshotInstaller interface {
	InstallSnapshot(ctx context.Context, desc replsnapshot.SnapshotDescriptor, r io.Reader) (wal.LSN, error)
}

func (s *Service) WithWAL(reader WALReader) *Service {
	s.WAL = reader
	return s
}

func (s *Service) WithCheckpoint(provider CheckpointProvider) *Service {
	s.Checkpoint = provider
	return s
}

func (s *Service) WithSnapshotInstaller(installer SnapshotInstaller) *Service {
	s.SnapshotInstaller = installer
	return s
}
