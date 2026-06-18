package internal

import (
	"os"
	"path/filepath"
	"strings"
)

// EnvDataDir is the environment variable used by MycelDB tools and services to
// locate the MycelDB data directory when no explicit data directory is supplied.
const EnvDataDir = "MYCELDB_DATA_DIR"

// ResolveDataDir returns the explicit data directory when non-empty, otherwise
// the value of MYCELDB_DATA_DIR. A leading ~/ is expanded for convenience.
func ResolveDataDir(explicit string) string {
	dataDir := strings.TrimSpace(explicit)
	if dataDir == "" {
		dataDir = strings.TrimSpace(os.Getenv(EnvDataDir))
	}
	return expandHome(dataDir)
}

func expandHome(path string) string {
	if path == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
		return path
	}
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[2:])
		}
	}
	return path
}
