package cmd

import (
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/cli/app"
)

func TestBackupClusterRestoreCommandsAreOfflineCLIPrimitives(t *testing.T) {
	root := NewRootCommand(&app.App{}, false)
	for _, tc := range []struct {
		path  []string
		flags []string
	}{
		{path: []string{"admin", "backup", "cluster", "restore-plan"}, flags: []string{"backup-set"}},
		{path: []string{"admin", "backup", "cluster", "restore-local"}, flags: []string{"backup-set", "ordinal", restoreLocalDataDirFlag}},
	} {
		cmd, _, err := root.Find(tc.path)
		if err != nil {
			t.Fatalf("command path %v not found: %v", tc.path, err)
		}
		for _, flag := range tc.flags {
			if cmd.Flags().Lookup(flag) == nil {
				t.Fatalf("command path %v missing --%s", tc.path, flag)
			}
		}
	}
}

func TestBackupClusterRestorePlanRequiresBackupSetBeforeDaemonLogin(t *testing.T) {
	root := NewRootCommand(&app.App{}, false)
	root.SetArgs([]string{"admin", "backup", "cluster", "restore-plan"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "--backup-set is required") {
		t.Fatalf("Execute() error = %v, want local backup-set validation", err)
	}
}
