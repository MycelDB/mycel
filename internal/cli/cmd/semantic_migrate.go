package cmd

import (
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"github.com/spf13/cobra"
)

func newSemanticMigrateCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "migrate", Short: "Legacy semantic migration commands"}
	cmd.AddCommand(newSemanticMigrateLegacyEmbeddingsCommand(a))
	return cmd
}

func newSemanticMigrateLegacyEmbeddingsCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, profileRef, ownerUserID string
	var limit int
	var allowBackgroundUse, addAllowPolicy, strict, dryRun bool
	cmd := &cobra.Command{Use: "legacy-embeddings", Short: "Deprecated: legacy embedding migration window is closed", Long: "Deprecated: the legacy embedding migration window is closed. Configure inference credentials, grants, policies, and semantic indexes directly.", RunE: func(cmd *cobra.Command, args []string) error {
		return runDaemonSemanticMigrateLegacyEmbeddings(cmd, a, spaceIDText, domainRef, profileRef, ownerUserID, allowBackgroundUse, addAllowPolicy, strict, dryRun, limit)
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&profileRef, "profile", "", "optional legacy embedding profile ID or name")
	cmd.Flags().StringVar(&ownerUserID, "owner-user-id", "", "legacy embedding owner user UUID (defaults to the space owner)")
	cmd.Flags().IntVar(&limit, "limit", 0, "maximum profiles to migrate (0 for all)")
	cmd.Flags().BoolVar(&allowBackgroundUse, "allow-background-use", true, "allow migrated grants to be used by background semantic maintenance")
	cmd.Flags().BoolVar(&addAllowPolicy, "add-allow-policy", true, "add a domain allow policy for embeddings using the migrated endpoint privacy class")
	cmd.Flags().BoolVar(&strict, "strict", false, "fail instead of warning when a profile cannot be migrated")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate and report migratable legacy profiles without writing resources")
	return cmd
}

func runDaemonSemanticMigrateLegacyEmbeddings(cmd *cobra.Command, a *app.App, spaceIDText, domainRef, profileRef, ownerUserID string, allowBackgroundUse, addAllowPolicy, strict, dryRun bool, limit int) error {
	conn, authCtx, _, err := loginDaemonOperator(cmd.Context(), a)
	if err != nil {
		return err
	}
	defer conn.Close()
	spaceID, err := a.ResolveSpaceID(spaceIDText)
	if err != nil {
		return err
	}
	domainID, err := daemonResolveAdminDomainID(cmd.Context(), conn, authCtx, spaceID.String(), domainRef)
	if err != nil {
		return err
	}
	res, err := adminv1.NewAdminSemanticMigrationServiceClient(conn).MigrateLegacyEmbeddings(authCtx, &adminv1.MigrateLegacyEmbeddingsRequest{SpaceId: spaceID.String(), DomainId: domainID, OwnerUserId: ownerUserID, ProfileRef: profileRef, AllowBackgroundUse: allowBackgroundUse, AddAllowPolicy: addAllowPolicy, Strict: strict, DryRun: dryRun, Limit: int32(limit)})
	if err != nil {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "profiles_seen=%d migrated=%d skipped=%d\n", res.GetProfilesSeen(), res.GetProfilesMigrated(), res.GetProfilesSkipped())
	for _, warning := range res.GetWarnings() {
		fmt.Fprintf(&b, "warning\t%s\n", warning)
	}
	for _, id := range res.GetSemanticIndexIds() {
		fmt.Fprintf(&b, "semantic_index=%s\n", id)
	}
	return a.Print(res, b.String())
}
