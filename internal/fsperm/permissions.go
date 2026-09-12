package fsperm

const (
	// PrivateDir is used for daemon-owned directories that may contain secrets,
	// credentials, raft state, WAL files, backups, or other local-only state.
	PrivateDir = 0o700

	// PrivateFile is used for daemon-owned files that may contain secrets,
	// credentials, raft state, WAL records, backups, or other local-only state.
	PrivateFile = 0o600

	// SharedDir is used for non-secret directories that may be traversed by
	// tooling running as the current user or group.
	SharedDir = 0o755

	// SharedFile is used for non-secret files that may be read by tooling running
	// as the current user or group.
	SharedFile = 0o644
)
