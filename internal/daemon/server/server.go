package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	adminapi "github.com/myceldb/mycel/internal/daemon/api/admin"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	"google.golang.org/grpc"
)

type Config struct {
	Addr        string
	AdminLister daemonadmin.AdminLister
	Logger      *slog.Logger
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
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen grpc %s: %w", cfg.Addr, err)
	}
	grpcServer := grpc.NewServer(opts...)
	adminv1.RegisterAdminOperatorServiceServer(grpcServer, adminapi.NewOperatorService(cfg.AdminLister))
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
