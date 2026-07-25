package cmd

import (
	"fmt"
	"os"

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
		Short:         "MycelDB daemon CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := mycelconfig.Load(mycelconfig.Options{ConfigFile: a.ConfigFile, Flags: cmd.Root().PersistentFlags()})
			if err != nil {
				return err
			}
			if cfg.Output != "" {
				a.Output = app.DefaultOutput(cfg.Output)
			}
			if cfg.Username != "" {
				a.UserRef = cfg.Username
			}
			if cfg.Password != "" {
				a.Password = cfg.Password
			}
			if cfg.DaemonAddr != "" {
				a.DaemonAddr = cfg.DaemonAddr
			}
			a.DaemonTLS = cfg.DaemonTLS
			if cfg.DaemonTLSCAFile != "" {
				a.DaemonTLSCAFile = cfg.DaemonTLSCAFile
			}
			if cfg.DaemonTLSServerName != "" {
				a.DaemonTLSServerName = cfg.DaemonTLSServerName
			}
			a.DaemonTLSInsecureSkipVerify = cfg.DaemonTLSInsecureSkipVerify
			if cfg.DaemonTLSClientCertFile != "" {
				a.DaemonTLSClientCertFile = cfg.DaemonTLSClientCertFile
			}
			if cfg.DaemonTLSClientKeyFile != "" {
				a.DaemonTLSClientKeyFile = cfg.DaemonTLSClientKeyFile
			}
			return nil
		},
	}
	root.PersistentFlags().StringVar(&a.ConfigFile, "config", a.ConfigFile, "optional MycelDB CLI config file (defaults to MYCEL_CONFIG)")
	root.PersistentFlags().StringVarP(&a.UserRef, "username", "u", a.UserRef, "username for non-REPL authentication")
	root.PersistentFlags().StringVarP(&a.Password, "password", "p", a.Password, "password for non-REPL authentication")
	root.PersistentFlags().StringVar(&a.Output, "output", app.DefaultOutput(a.Output), "output format: text or json")
	root.PersistentFlags().StringVar(&a.DaemonAddr, "daemon-addr", a.DaemonAddr, "myceld gRPC address (defaults to MYCELD_GRPC_ADDR or 127.0.0.1:9091)")
	root.PersistentFlags().BoolVar(&a.DaemonTLS, "daemon-tls", a.DaemonTLS, "use TLS for daemon gRPC (defaults to MYCELD_TLS=true when set)")
	root.PersistentFlags().StringVar(&a.DaemonTLSCAFile, "daemon-tls-ca", a.DaemonTLSCAFile, "CA certificate file for daemon TLS (defaults to MYCELD_TLS_CA_FILE)")
	root.PersistentFlags().StringVar(&a.DaemonTLSServerName, "daemon-tls-server-name", a.DaemonTLSServerName, "override daemon TLS server name (defaults to MYCELD_TLS_SERVER_NAME or host from --daemon-addr)")
	root.PersistentFlags().BoolVar(&a.DaemonTLSInsecureSkipVerify, "daemon-tls-insecure-skip-verify", a.DaemonTLSInsecureSkipVerify, "skip daemon TLS certificate verification (testing only; MYCELD_TLS_INSECURE_SKIP_VERIFY)")
	root.PersistentFlags().StringVar(&a.DaemonTLSClientCertFile, "daemon-tls-client-cert", a.DaemonTLSClientCertFile, "client certificate for daemon mTLS (defaults to MYCELD_TLS_CLIENT_CERT_FILE)")
	root.PersistentFlags().StringVar(&a.DaemonTLSClientKeyFile, "daemon-tls-client-key", a.DaemonTLSClientKeyFile, "client private key for daemon mTLS (defaults to MYCELD_TLS_CLIENT_KEY_FILE)")

	root.AddCommand(NewInitCommand(a), NewAdminCommand(a), NewUserCommand(a), NewSpaceCommand(a), NewDomainCommand(a), NewNodeCommand(a), NewGraphCommand(a), NewBlobCommand(a), NewACLCommand(a), NewAuthCommand(a), NewSessionCommand(a), NewTransactionCommand(a), NewQueryCommand(a), NewMetadataCommand(a), NewExportCommand(a), NewImportCommand(a), NewInferenceCommand(a), NewSemanticCommand(a), NewChangeStreamCommand(a), NewAccountingCommand(a), NewClusterCommand(a), NewReplCommand(a))
	if repl {
		root.Use = ""
	}
	return root
}
