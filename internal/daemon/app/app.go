package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/logging"
	"github.com/myceldb/mycel/internal/daemon/modules/admin"
	daemonblob "github.com/myceldb/mycel/internal/daemon/modules/blob"
	daemonchange "github.com/myceldb/mycel/internal/daemon/modules/changestream"
	daegraph "github.com/myceldb/mycel/internal/daemon/modules/graph"
	daemonsemantic "github.com/myceldb/mycel/internal/daemon/modules/semantic"
	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	daemonuser "github.com/myceldb/mycel/internal/daemon/modules/user"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/daemon/server"
	"github.com/myceldb/mycel/internal/graph/change"
)

const LogFilename = "myceld.log"

func Run(ctx context.Context) int {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "myceld config error: %v\n", err)
		return 2
	}
	rt, err := Initialize(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: %v\n", err)
		return 1
	}
	defer func() { _ = rt.Close() }()

	adminModule, ok := daemonruntime.ModuleAs[*admin.Module](rt, admin.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: admin module is not registered\n")
		return 1
	}
	serverCtx, stopServer := context.WithCancel(ctx)
	userModule, ok := daemonruntime.ModuleAs[*daemonuser.Module](rt, daemonuser.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: user module is not registered\n")
		return 1
	}
	spaceModule, ok := daemonruntime.ModuleAs[*daemonspace.Module](rt, daemonspace.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: space module is not registered\n")
		return 1
	}
	sessionModule, ok := daemonruntime.ModuleAs[*daemonsession.Module](rt, daemonsession.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: session module is not registered\n")
		return 1
	}
	graphModule, ok := daemonruntime.ModuleAs[*daegraph.Module](rt, daegraph.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: graph module is not registered\n")
		return 1
	}
	blobModule, ok := daemonruntime.ModuleAs[*daemonblob.Module](rt, daemonblob.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: blob module is not registered\n")
		return 1
	}
	semanticModule, ok := daemonruntime.ModuleAs[*daemonsemantic.Module](rt, daemonsemantic.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: semantic module is not registered\n")
		return 1
	}
	changeModule, ok := daemonruntime.ModuleAs[*daemonchange.Module](rt, daemonchange.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: change stream module is not registered\n")
		return 1
	}
	tlsConfig, err := server.LoadTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile, cfg.TLSRequireClientCert)
	if err != nil {
		fmt.Fprintf(os.Stderr, "myceld TLS config error: %v\n", err)
		return 1
	}
	grpcServer, grpcErrCh, err := server.Start(serverCtx, server.Config{Addr: cfg.GRPCAddr, AdminLister: adminModule, AdminAuthenticator: adminModule, OperatorManager: adminModule, UserManager: userModule, SpaceManager: spaceModule, SessionManager: sessionModule, GraphManager: graphModule, BlobManager: blobModule, SemanticManager: semanticModule, ChangeManager: changeModule, Logger: rt.Logger, TLSConfig: tlsConfig})
	if err != nil {
		fmt.Fprintf(os.Stderr, "myceld grpc startup failed: %v\n", err)
		return 1
	}
	defer stopServer()

	rt.Logger.Info("daemon ready", "grpc_addr", grpcServer.Addr())
	logRuntimeConfiguration(rt.Logger, cfg, rt.LogPath, grpcServer.Addr())
	waitForShutdown(ctx, rt.Logger)
	stopServer()
	if err := <-grpcErrCh; err != nil {
		rt.Logger.Error("grpc server stopped with error", "error", err)
		return 1
	}
	rt.Logger.Info("daemon shutdown complete")
	return 0
}

func Initialize(ctx context.Context, cfg config.Config) (*daemonruntime.Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	dataDirCreated, err := ensureDir(cfg.DataDir, 0o700)
	if err != nil {
		return nil, fmt.Errorf("ensure data directory: %w", err)
	}
	logDir := filepath.Join(cfg.DataDir, "log")
	logDirCreated, err := ensureDir(logDir, 0o700)
	if err != nil {
		return nil, fmt.Errorf("ensure log directory: %w", err)
	}
	logPath := filepath.Join(logDir, LogFilename)
	configuredLogger, err := logging.Configure(logging.Config{Level: cfg.LogLevel, Format: cfg.LogFormat, Path: logPath})
	if err != nil {
		return nil, err
	}
	logger := configuredLogger.Logger
	logger.Info("daemon startup begins", "data_dir", cfg.DataDir, "mode", cfg.Mode)
	logger.Info("data directory ready", "path", cfg.DataDir, "created", dataDirCreated)
	logger.Info("log directory ready", "path", logDir, "created", logDirCreated)

	rt := daemonruntime.New(cfg, logger, logPath, configuredLogger.Close)
	adminModule := admin.NewModule()
	userModule := daemonuser.NewModule()
	spaceModule := daemonspace.NewModule()
	sessionModule := daemonsession.NewModule()
	graphModule := daegraph.NewModule()
	blobModule := daemonblob.NewModule(graphModule)
	semanticModule := daemonsemantic.NewModule()
	changeModule := daemonchange.NewModule()
	if err := rt.InitModules(ctx, []daemonruntime.Module{adminModule, userModule, spaceModule, sessionModule, graphModule, blobModule, semanticModule, changeModule}); err != nil {
		_ = rt.Close()
		return nil, err
	}
	graphModule.SetChangeSink(graphchange.SinkFunc(func(ctx context.Context, event graphchange.CommittedEvent) error {
		appender, err := semanticModule.DirtyEventAppender(ctx, event.SpaceID)
		if err != nil {
			return err
		}
		return appender.OnGraphCommitted(ctx, event)
	}))
	logger.Info("daemon initialization complete")
	return rt, nil
}

func logRuntimeConfiguration(logger *slog.Logger, cfg config.Config, logPath string, grpcAddr string) {
	logger.Info("daemon runtime configuration",
		"data_dir", cfg.DataDir,
		"mode", cfg.Mode,
		"grpc_addr", grpcAddr,
		"log_path", logPath,
		"log_level", cfg.LogLevel,
		"log_format", cfg.LogFormat,
		"tls_enabled", cfg.TLSCertFile != "",
		"mtls_required", cfg.TLSRequireClientCert,
	)
}

func waitForShutdown(ctx context.Context, logger *slog.Logger) {
	signalCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-signalCtx.Done()
	logger.Info("daemon shutdown begins")
}

func ensureDir(path string, perm os.FileMode) (bool, error) {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return false, fmt.Errorf("%s exists and is not a directory", path)
		}
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.MkdirAll(path, perm); err != nil {
		return false, err
	}
	return true, nil
}
