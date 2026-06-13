package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	knotengine "martinbeauvais.com/mbgit/knotbase/knotdb/engine"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/cli/app"
	knotconfig "martinbeauvais.com/mbgit/knotbase/knotdb/internal/config"
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
		Use:           "knotdb",
		Short:         "KnotDB embedded engine CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := knotconfig.Load(knotconfig.Options{ConfigFile: a.ConfigFile, Flags: cmd.Flags()})
			if err != nil {
				return err
			}
			a.Config = cfg
			if cfg.DataDir != "" {
				a.DataDir = knotengine.ResolveDataDir(cfg.DataDir)
			}
			if cfg.Output != "" {
				a.Output = app.DefaultOutput(cfg.Output)
			}
			a.UserStoreEncryptionKeyB64 = cfg.UserStoreEncryptionKeyB64
			return nil
		},
	}
	a.DataDir = knotengine.ResolveDataDir(a.DataDir)
	root.PersistentFlags().StringVar(&a.ConfigFile, "config", a.ConfigFile, "optional KnotDB config file (defaults to KNOTDB_CONFIG)")
	root.PersistentFlags().StringVarP(&a.DataDir, "data-dir", "d", a.DataDir, "KnotDB data directory (defaults to KNOTDB_DATA_DIR)")
	root.PersistentFlags().StringVarP(&a.UserRef, "username", "u", a.UserRef, "username/user_ref for non-REPL authentication")
	root.PersistentFlags().StringVarP(&a.Password, "password", "p", a.Password, "password for non-REPL authentication")
	root.PersistentFlags().StringVar(&a.Output, "output", app.DefaultOutput(a.Output), "output format: text or json")
	root.PersistentFlags().StringVar(&a.UserStoreEncryptionKeyB64, "user-store-encryption-key-b64", a.UserStoreEncryptionKeyB64, "base64 AES-256 key for the user store")
	root.PersistentFlags().StringVar(&a.AuthTokenTTL, "auth-token-ttl", a.AuthTokenTTL, "access token TTL (for example 1h)")
	root.PersistentFlags().StringVar(&a.BlobStaleTmpAge, "blob-stale-tmp-age", a.BlobStaleTmpAge, "age before stale blob temp files are swept")
	root.PersistentFlags().Int64Var(&a.BlobMaxSizeBytes, "blob-max-size-bytes", a.BlobMaxSizeBytes, "global blob upload cap in bytes (-1 unlimited, 0 disallowed)")
	root.PersistentFlags().Int64Var(&a.BlobMaxImageBytes, "blob-max-image-bytes", a.BlobMaxImageBytes, "image blob upload cap in bytes")
	root.PersistentFlags().Int64Var(&a.BlobMaxPDFBytes, "blob-max-pdf-bytes", a.BlobMaxPDFBytes, "PDF blob upload cap in bytes")
	root.PersistentFlags().Int64Var(&a.BlobMaxAudioBytes, "blob-max-audio-bytes", a.BlobMaxAudioBytes, "audio blob upload cap in bytes")
	root.PersistentFlags().Int64Var(&a.BlobMaxVideoBytes, "blob-max-video-bytes", a.BlobMaxVideoBytes, "video blob upload cap in bytes")
	root.PersistentFlags().Int64Var(&a.BlobMaxOtherBytes, "blob-max-other-bytes", a.BlobMaxOtherBytes, "uncategorized blob upload cap in bytes")

	root.AddCommand(NewInitCommand(a), NewUserCommand(a), NewSpaceCommand(a), NewNodeCommand(a), NewBlobCommand(a), NewTemplateCommand(a), NewACLCommand(a), NewEmbeddingsCommand(a), NewReplCommand(a))
	if repl {
		root.Use = ""
	}
	return root
}
