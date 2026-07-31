package runtime

import (
	"log/slog"

	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/wal"
)

// Host is the minimal common runtime host surface. Concrete composition roots
// may implement additional capability interfaces for WAL, quiesce, clustering,
// and service lookup.
type Host interface {
	Log() *slog.Logger
	DataDir() string
}

// ServiceLookup is implemented by hosts that can look up registered services.
type ServiceLookup interface {
	Service(name string) (Service, bool)
}

// QuiesceRegistrar is implemented by hosts that coordinate service quiesce
// participants.
type QuiesceRegistrar interface {
	RegisterQuiesceParticipant(quiesce.Participant) error
}

// QuiesceCoordinatorProvider is implemented by hosts that expose the shared
// coordinator to services that need to orchestrate whole-runtime quiesce.
type QuiesceCoordinatorProvider interface {
	QuiesceCoordinator() *quiesce.Coordinator
}

// LocalWriteGate is implemented by hosts that can decide whether subsystem
// local mutation paths are safe. Clustered runtimes use this to fail closed
// when a subsystem has not been wired to its Raft executor yet.
type LocalWriteGate interface {
	RequireLocalWriteAllowed() error
}

// WALProvider is implemented by hosts that expose WAL infrastructure to
// services that own WAL-backed state.
type WALProvider interface {
	WALManager() *wal.Manager
	WALRegistryStore() *wal.Registry
	WALProgressStore() wal.AppliedLSNStore
	WALWaiterStore() *wal.ApplyWaiter
	WALCheckpointStore() *wal.CheckpointStore
}
