package cmd

import (
	"fmt"
	"os"

	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	mycelconfig "github.com/myceldb/mycel/internal/config"
	"github.com/spf13/cobra"
)

func Execute() error {
	a := &app.App{}
	cmd := NewRootCommand(a, false)
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return err
	}
	return nil
}

func NewRootCommand(a *app.App, repl bool) *cobra.Command {
	root := &cobra.Command{
		Use:           "mycel",
		Short:         "MycelDB embedded engine CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := mycelconfig.Load(mycelconfig.Options{ConfigFile: a.ConfigFile, Flags: cmd.Root().PersistentFlags()})
			if err != nil {
				return err
			}
			a.Config = cfg
			if cfg.DataDir != "" {
				a.DataDir = mycelengine.ResolveDataDir(cfg.DataDir)
			}
			if cfg.Output != "" {
				a.Output = app.DefaultOutput(cfg.Output)
			}
			a.UserStoreEncryptionKeyB64 = cfg.UserStoreEncryptionKeyB64
			a.AdvancedSemanticEnabled = cfg.AdvancedSemanticEnabled
			return nil
		},
	}
	a.DataDir = mycelengine.ResolveDataDir(a.DataDir)
	root.PersistentFlags().StringVar(&a.ConfigFile, "config", a.ConfigFile, "optional MycelDB config file (defaults to MYCELDB_CONFIG)")
	root.PersistentFlags().StringVarP(&a.DataDir, "data-dir", "d", a.DataDir, "MycelDB data directory (defaults to MYCELDB_DATA_DIR)")
	root.PersistentFlags().StringVarP(&a.UserRef, "username", "u", a.UserRef, "username/user_ref for non-REPL authentication")
	root.PersistentFlags().StringVarP(&a.Password, "password", "p", a.Password, "password for non-REPL authentication")
	root.PersistentFlags().StringVar(&a.Output, "output", app.DefaultOutput(a.Output), "output format: text or json")
	root.PersistentFlags().StringVar(&a.UserStoreEncryptionKeyB64, "user-store-encryption-key-b64", a.UserStoreEncryptionKeyB64, "base64 AES-256 key for the user store")
	root.PersistentFlags().StringVar(&a.AuthTokenTTL, "auth-token-ttl", a.AuthTokenTTL, "access token TTL (for example 1h)")
	root.PersistentFlags().StringVar(&a.AuthRefreshIdleTTL, "auth-refresh-idle-ttl", a.AuthRefreshIdleTTL, "refresh-session idle TTL (for example 720h)")
	root.PersistentFlags().StringVar(&a.AuthRefreshAbsoluteTTL, "auth-refresh-absolute-ttl", a.AuthRefreshAbsoluteTTL, "refresh-session absolute TTL (for example 2160h)")
	root.PersistentFlags().StringVar(&a.AuthRefreshAuditRetentionTTL, "auth-refresh-audit-retention-ttl", a.AuthRefreshAuditRetentionTTL, "refresh-session audit retention TTL (for example 720h)")
	root.PersistentFlags().IntVar(&a.AuthRefreshTokenBytes, "auth-refresh-token-bytes", a.AuthRefreshTokenBytes, "refresh token entropy in bytes")
	root.PersistentFlags().StringVar(&a.BlobStaleTmpAge, "blob-stale-tmp-age", a.BlobStaleTmpAge, "age before stale blob temp files are swept")
	root.PersistentFlags().Int64Var(&a.BlobMaxSizeBytes, "blob-max-size-bytes", a.BlobMaxSizeBytes, "global blob upload cap in bytes (-1 unlimited, 0 disallowed)")
	root.PersistentFlags().Int64Var(&a.BlobMaxImageBytes, "blob-max-image-bytes", a.BlobMaxImageBytes, "image blob upload cap in bytes")
	root.PersistentFlags().Int64Var(&a.BlobMaxPDFBytes, "blob-max-pdf-bytes", a.BlobMaxPDFBytes, "PDF blob upload cap in bytes")
	root.PersistentFlags().Int64Var(&a.BlobMaxAudioBytes, "blob-max-audio-bytes", a.BlobMaxAudioBytes, "audio blob upload cap in bytes")
	root.PersistentFlags().Int64Var(&a.BlobMaxVideoBytes, "blob-max-video-bytes", a.BlobMaxVideoBytes, "video blob upload cap in bytes")
	root.PersistentFlags().Int64Var(&a.BlobMaxOtherBytes, "blob-max-other-bytes", a.BlobMaxOtherBytes, "uncategorized blob upload cap in bytes")
	root.PersistentFlags().BoolVar(&a.AdvancedSemanticEnabled, "semantic-advanced-enabled", a.AdvancedSemanticEnabled, "enable advanced semantic implementation paths as they are introduced")

	root.AddCommand(NewInitCommand(a), NewAdminCommand(a), NewUserCommand(a), NewSpaceCommand(a), NewDomainCommand(a), NewNodeCommand(a), NewBlobCommand(a), NewTemplateCommand(a), NewACLCommand(a), NewAuthCommand(a), NewEmbeddingsCommand(a), NewInferenceCommand(a), NewSemanticCommand(a), NewAccountingCommand(a), NewReplCommand(a))
	if repl {
		root.Use = ""
	}
	return root
}
