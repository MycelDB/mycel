package cmd

import (
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
)

func TestBackupRestoreCommandsUseRepositoryBackedInputs(t *testing.T) {
	root := NewRootCommand(&app.App{}, false)
	for _, tc := range []struct {
		path  []string
		flags []string
	}{
		{path: []string{"admin", "backup", "restore-plan"}, flags: []string{"repository-id", "backup-set-id"}},
		{path: []string{"admin", "backup", "restore-repository-local"}, flags: []string{"bucket", "repository-prefix", "backup-set-id", "ordinal", restoreLocalDataDirFlag}},
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

func TestBackupRestorePlanRequiresBackupSetIDBeforeDaemonLogin(t *testing.T) {
	root := NewRootCommand(&app.App{}, false)
	root.SetArgs([]string{"admin", "backup", "restore-plan"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "--backup-set-id is required") {
		t.Fatalf("Execute() error = %v, want backup-set-id validation", err)
	}
}

func TestBackupRestoreTargetModeAcceptsDeploymentNames(t *testing.T) {
	if got := parseRestoreTargetMode("new-deployment-bootstrap"); got != adminv1.RestoreTargetMode_RESTORE_TARGET_MODE_NEW_DEPLOYMENT_BOOTSTRAP {
		t.Fatalf("new-deployment-bootstrap parsed as %s", got)
	}
	if got := parseRestoreTargetMode("existing-deployment-replace"); got != adminv1.RestoreTargetMode_RESTORE_TARGET_MODE_EXISTING_DEPLOYMENT_REPLACE {
		t.Fatalf("existing-deployment-replace parsed as %s", got)
	}
}
