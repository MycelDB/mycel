package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"

	adminapi "github.com/myceldb/mycel/internal/daemon/api/admin"
	clientapi "github.com/myceldb/mycel/internal/daemon/api/client"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	daemonbackup "github.com/myceldb/mycel/internal/daemon/modules/backup"
	daemonblob "github.com/myceldb/mycel/internal/daemon/modules/blob"
	daemonchange "github.com/myceldb/mycel/internal/daemon/modules/changestream"
	daegraph "github.com/myceldb/mycel/internal/daemon/modules/graph"
	daemonsemantic "github.com/myceldb/mycel/internal/daemon/modules/semantic"
	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	daemonuser "github.com/myceldb/mycel/internal/daemon/modules/user"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	Addr               string
	AdminLister        daemonadmin.AdminLister
	AdminAuthenticator daemonadmin.OperatorAuthManager
	OperatorManager    daemonadmin.OperatorManager
	BackupManager      daemonbackup.Manager
	UserManager        daemonuser.Manager
	SpaceManager       daemonspace.Manager
	SessionManager     daemonsession.Manager
	GraphManager       daegraph.Manager
	BlobManager        daemonblob.Manager
	SemanticManager    daemonsemantic.Manager
	ChangeManager      daemonchange.Manager
	TokenManager       *daemonauth.TokenManager
	Quiesce            *quiesce.Coordinator
	IngressGate        *quiesce.Gate
	QuiesceExempt      map[string]bool
	Logger             *slog.Logger
	TLSConfig          *tls.Config
}

type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *slog.Logger
}

func New(cfg Config, opts ...grpc.ServerOption) (*Server, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("grpc address is required")
	}
	if cfg.AdminLister == nil {
		return nil, fmt.Errorf("admin lister is required")
	}
	if cfg.AdminAuthenticator == nil {
		return nil, fmt.Errorf("admin authenticator is required")
	}
	if cfg.OperatorManager == nil {
		return nil, fmt.Errorf("operator manager is required")
	}
	if cfg.UserManager == nil {
		return nil, fmt.Errorf("user manager is required")
	}
	if cfg.SpaceManager == nil {
		return nil, fmt.Errorf("space manager is required")
	}
	if cfg.SessionManager == nil {
		return nil, fmt.Errorf("session manager is required")
	}
	if cfg.GraphManager == nil {
		return nil, fmt.Errorf("graph manager is required")
	}
	if cfg.BlobManager == nil {
		return nil, fmt.Errorf("blob manager is required")
	}
	if cfg.SemanticManager == nil {
		return nil, fmt.Errorf("semantic manager is required")
	}
	if cfg.ChangeManager == nil {
		return nil, fmt.Errorf("change stream manager is required")
	}
	if cfg.TokenManager == nil {
		var err error
		cfg.TokenManager, err = daemonauth.NewRandomTokenManager(daemonauth.DefaultAccessTokenTTL)
		if err != nil {
			return nil, err
		}
	}
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen grpc %s: %w", cfg.Addr, err)
	}
	publicMethods := map[string]bool{adminv1.AdminAuthService_LoginOperator_FullMethodName: true, adminv1.AdminAuthService_RefreshOperator_FullMethodName: true, clientv1.AuthService_Login_FullMethodName: true, clientv1.AuthService_Refresh_FullMethodName: true}
	quiesceExempt := defaultQuiesceExemptMethods()
	for method, exempt := range cfg.QuiesceExempt {
		quiesceExempt[method] = exempt
	}
	if cfg.IngressGate == nil {
		cfg.IngressGate = quiesce.NewGate("api-ingress")
	}
	if cfg.Quiesce != nil {
		if err := cfg.Quiesce.RegisterFirst(cfg.IngressGate); err != nil {
			return nil, err
		}
	}
	baseOptions := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(cfg.TokenManager.UnaryInterceptor(publicMethods), quiesceUnaryInterceptor(cfg.IngressGate, quiesceExempt)),
		grpc.ChainStreamInterceptor(cfg.TokenManager.StreamInterceptor(publicMethods), quiesceStreamInterceptor(cfg.IngressGate, quiesceExempt)),
	}
	if cfg.TLSConfig != nil {
		baseOptions = append(baseOptions, grpc.Creds(credentials.NewTLS(cfg.TLSConfig)))
	}
	serverOptions := append(baseOptions, opts...)
	grpcServer := grpc.NewServer(serverOptions...)
	adminv1.RegisterAdminAuthServiceServer(grpcServer, adminapi.NewAuthService(cfg.AdminAuthenticator, cfg.TokenManager))
	adminv1.RegisterAdminOperatorServiceServer(grpcServer, adminapi.NewOperatorService(cfg.OperatorManager))
	adminv1.RegisterAdminUserServiceServer(grpcServer, adminapi.NewUserService(cfg.UserManager, cfg.OperatorManager))
	adminv1.RegisterAdminSpaceServiceServer(grpcServer, adminapi.NewAdminSpaceService(cfg.SpaceManager, cfg.UserManager, cfg.OperatorManager))
	adminv1.RegisterAdminDomainServiceServer(grpcServer, adminapi.NewAdminDomainService(cfg.SpaceManager, cfg.OperatorManager))
	adminv1.RegisterAdminInferenceServiceServer(grpcServer, adminapi.NewAdminInferenceService(cfg.SemanticManager, cfg.OperatorManager))
	adminv1.RegisterAdminSemanticServiceServer(grpcServer, adminapi.NewAdminSemanticService(cfg.SemanticManager, cfg.SpaceManager, cfg.OperatorManager))
	adminv1.RegisterAdminSemanticMaintenanceServiceServer(grpcServer, adminapi.NewAdminSemanticMaintenanceService(cfg.SemanticManager, cfg.OperatorManager))
	adminv1.RegisterAdminSemanticMigrationServiceServer(grpcServer, adminapi.NewAdminSemanticMigrationService(cfg.SemanticManager, cfg.SpaceManager, cfg.OperatorManager))
	if cfg.BackupManager != nil {
		adminv1.RegisterAdminBackupServiceServer(grpcServer, adminapi.NewAdminBackupService(cfg.BackupManager, cfg.Quiesce, cfg.OperatorManager))
	}
	clientv1.RegisterAuthServiceServer(grpcServer, clientapi.NewAuthService(cfg.UserManager, cfg.TokenManager))
	clientv1.RegisterSpaceServiceServer(grpcServer, clientapi.NewSpaceService(cfg.SpaceManager))
	clientv1.RegisterDomainServiceServer(grpcServer, clientapi.NewDomainService(cfg.SpaceManager))
	clientv1.RegisterTemplateServiceServer(grpcServer, clientapi.NewTemplateService(cfg.SpaceManager))
	clientv1.RegisterSessionServiceServer(grpcServer, clientapi.NewSessionService(cfg.SessionManager, cfg.SpaceManager))
	clientv1.RegisterTransactionServiceServer(grpcServer, clientapi.NewTransactionService(cfg.SessionManager, cfg.GraphManager, cfg.ChangeManager))
	clientv1.RegisterGraphServiceServer(grpcServer, clientapi.NewGraphService(cfg.SessionManager, cfg.GraphManager, cfg.BlobManager))
	clientv1.RegisterBlobServiceServer(grpcServer, clientapi.NewBlobService(cfg.BlobManager, cfg.SpaceManager))
	clientv1.RegisterQueryServiceServer(grpcServer, clientapi.NewQueryService(cfg.SessionManager, cfg.GraphManager, cfg.SpaceManager))
	clientv1.RegisterImportExportServiceServer(grpcServer, clientapi.NewImportExportService(cfg.SessionManager, cfg.GraphManager, cfg.BlobManager, cfg.SpaceManager))
	clientv1.RegisterMetadataCatalogServiceServer(grpcServer, clientapi.NewMetadataCatalogService(cfg.SessionManager, cfg.GraphManager))
	clientv1.RegisterSemanticServiceServer(grpcServer, clientapi.NewSemanticService(cfg.SemanticManager, cfg.SpaceManager, cfg.GraphManager))
	clientv1.RegisterChangeStreamServiceServer(grpcServer, clientapi.NewChangeStreamService(cfg.ChangeManager, cfg.SpaceManager))
	return &Server{grpcServer: grpcServer, listener: listener, logger: cfg.Logger}, nil
}

func defaultQuiesceExemptMethods() map[string]bool {
	return map[string]bool{
		adminv1.AdminAuthService_LoginOperator_FullMethodName:     true,
		adminv1.AdminAuthService_RefreshOperator_FullMethodName:   true,
		adminv1.AdminAuthService_WhoAmI_FullMethodName:            true,
		adminv1.AdminBackupService_GetBackupPolicy_FullMethodName: true,
		adminv1.AdminBackupService_TriggerBackup_FullMethodName:   true,
		adminv1.AdminBackupService_GetBackupStatus_FullMethodName: true,
		adminv1.AdminBackupService_ListBackups_FullMethodName:     true,
	}
}

func (s *Server) Addr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) Serve() error {
	if s == nil {
		return fmt.Errorf("server is nil")
	}
	if s.logger != nil {
		s.logger.Info("grpc server listening", "addr", s.Addr())
	}
	if err := s.grpcServer.Serve(s.listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

func (s *Server) Stop() {
	if s == nil || s.grpcServer == nil {
		return
	}
	s.grpcServer.Stop()
}

func (s *Server) GracefulStop() {
	if s == nil || s.grpcServer == nil {
		return
	}
	s.grpcServer.GracefulStop()
}

func LoadTLSConfig(certFile string, keyFile string, clientCAFile string, requireClientCert bool) (*tls.Config, error) {
	if certFile == "" && keyFile == "" && clientCAFile == "" && !requireClientCert {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("server TLS cert and key files are required")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server TLS key pair: %w", err)
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	if clientCAFile != "" {
		raw, err := os.ReadFile(clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read client CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(raw) {
			return nil, fmt.Errorf("client CA file contains no PEM certificates")
		}
		cfg.ClientCAs = pool
		if requireClientCert {
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			cfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return cfg, nil
}

func Start(ctx context.Context, cfg Config, opts ...grpc.ServerOption) (*Server, <-chan error, error) {
	srv, err := New(cfg, opts...)
	if err != nil {
		return nil, nil, err
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve()
		close(errCh)
	}()
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	return srv, errCh, nil
}
