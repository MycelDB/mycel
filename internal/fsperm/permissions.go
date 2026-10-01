package fsperm

import (
	"errors"
	"fmt"
	"os"
)

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

// EnsureDir verifies path is a directory or creates it with perm. It returns
// true only when the directory was created by this call.
func EnsureDir(path string, perm os.FileMode) (bool, error) {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return false, fmt.Errorf("%s exists and is not a directory", path)
		}
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.MkdirAll(path, perm); err != nil {
		return false, err
	}
	return true, nil
}
