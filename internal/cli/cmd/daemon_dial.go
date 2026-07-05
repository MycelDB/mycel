package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/myceldb/mycel/internal/cli/app"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func dialDaemon(ctx context.Context, a *app.App) (*grpc.ClientConn, string, error) {
	addr, err := resolveDaemonAddr(a)
	if err != nil {
		return nil, "", err
	}
	creds, err := daemonTransportCredentials(a, addr)
	if err != nil {
		return nil, "", err
	}
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, "", fmt.Errorf("dial myceld gRPC at %s: %w", addr, err)
	}
	return conn, addr, nil
}

func daemonTransportCredentials(a *app.App, addr string) (credentials.TransportCredentials, error) {
	useTLS := a.DaemonTLS || parseBoolCLIEnv(os.Getenv("MYCELD_TLS"))
	caFile := firstNonEmptyCLI(a.DaemonTLSCAFile, os.Getenv("MYCELD_TLS_CA_FILE"))
	serverName := firstNonEmptyCLI(a.DaemonTLSServerName, os.Getenv("MYCELD_TLS_SERVER_NAME"))
	skipVerify := a.DaemonTLSInsecureSkipVerify || parseBoolCLIEnv(os.Getenv("MYCELD_TLS_INSECURE_SKIP_VERIFY"))
	clientCertFile := firstNonEmptyCLI(a.DaemonTLSClientCertFile, os.Getenv("MYCELD_TLS_CLIENT_CERT_FILE"))
	clientKeyFile := firstNonEmptyCLI(a.DaemonTLSClientKeyFile, os.Getenv("MYCELD_TLS_CLIENT_KEY_FILE"))
	if caFile != "" || serverName != "" || skipVerify || clientCertFile != "" || clientKeyFile != "" {
		useTLS = true
	}
	if !useTLS {
		return insecure.NewCredentials(), nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName, InsecureSkipVerify: skipVerify} //nolint:gosec // explicit CLI testing flag
	if cfg.ServerName == "" {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			cfg.ServerName = host
		}
	}
	if caFile != "" {
		raw, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read daemon TLS CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(raw) {
			return nil, fmt.Errorf("daemon TLS CA file contains no PEM certificates")
		}
		cfg.RootCAs = pool
	}
	if (clientCertFile == "") != (clientKeyFile == "") {
		return nil, fmt.Errorf("daemon mTLS client cert and key must be set together")
	}
	if clientCertFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load daemon mTLS client key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(cfg), nil
}

func parseBoolCLIEnv(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func firstNonEmptyCLI(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
