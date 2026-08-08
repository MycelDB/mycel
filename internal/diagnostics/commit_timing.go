package diagnostics

import (
	"log/slog"
	"os"
	"strings"
)

const CommitTimingEnv = "MYCEL_COMMIT_TIMING"

func CommitTimingEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(CommitTimingEnv)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func LogCommitTiming(message string, args ...any) {
	if !CommitTimingEnabled() {
		return
	}
	attrs := make([]any, 0, len(args)+2)
	attrs = append(attrs, "diagnostic", "commit_timing")
	attrs = append(attrs, args...)
	slog.Default().Info(message, attrs...)
}
