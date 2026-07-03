package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	adminapi "github.com/myceldb/mycel/internal/daemon/api/admin"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	"google.golang.org/grpc"
)

type Config struct {
	Addr               string
	AdminLister        daemonadmin.AdminLister
	AdminAuthenticator daemonadmin.OperatorAuthenticator
	PasswordManager    daemonadmin.OperatorPasswordManager
	TokenManager       *daemonauth.TokenManager
	Logger             *slog.Logger
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
	if cfg.PasswordManager == nil {
		return nil, fmt.Errorf("operator password manager is required")
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
	publicMethods := map[string]bool{adminv1.AdminAuthService_LoginOperator_FullMethodName: true}
	serverOptions := append([]grpc.ServerOption{grpc.ChainUnaryInterceptor(cfg.TokenManager.UnaryInterceptor(publicMethods))}, opts...)
	grpcServer := grpc.NewServer(serverOptions...)
	adminv1.RegisterAdminAuthServiceServer(grpcServer, adminapi.NewAuthService(cfg.AdminAuthenticator, cfg.TokenManager))
	adminv1.RegisterAdminOperatorServiceServer(grpcServer, adminapi.NewOperatorService(cfg.AdminLister, cfg.PasswordManager))
	return &Server{grpcServer: grpcServer, listener: listener, logger: cfg.Logger}, nil
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
