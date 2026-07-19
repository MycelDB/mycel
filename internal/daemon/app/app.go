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
	"time"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/replication"
	adminapi "github.com/myceldb/mycel/internal/daemon/api/admin"
	"github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/logging"
	"github.com/myceldb/mycel/internal/daemon/modules/admin"
	daemonbackup "github.com/myceldb/mycel/internal/daemon/modules/backup"
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
	"github.com/myceldb/mycel/internal/wal"
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

	adminService, ok := daemonruntime.ServiceAs[*admin.Module](rt, admin.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: admin service is not registered\n")
		return 1
	}
	serverCtx, stopServer := context.WithCancel(ctx)
	userService, ok := daemonruntime.ServiceAs[*daemonuser.Module](rt, daemonuser.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: user service is not registered\n")
		return 1
	}
	spaceService, ok := daemonruntime.ServiceAs[*daemonspace.Module](rt, daemonspace.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: space service is not registered\n")
		return 1
	}
	sessionService, ok := daemonruntime.ServiceAs[*daemonsession.Module](rt, daemonsession.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: session service is not registered\n")
		return 1
	}
	graphService, ok := daemonruntime.ServiceAs[*daegraph.Module](rt, daegraph.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: graph service is not registered\n")
		return 1
	}
	blobService, ok := daemonruntime.ServiceAs[*daemonblob.Module](rt, daemonblob.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: blob service is not registered\n")
		return 1
	}
	semanticService, ok := daemonruntime.ServiceAs[*daemonsemantic.Module](rt, daemonsemantic.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: semantic service is not registered\n")
		return 1
	}
	changeService, ok := daemonruntime.ServiceAs[*daemonchange.Module](rt, daemonchange.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: change stream service is not registered\n")
		return 1
	}
	backupService, ok := daemonruntime.ServiceAs[*daemonbackup.Module](rt, daemonbackup.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: backup service is not registered\n")
		return 1
	}
	tlsConfig, err := server.LoadTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile, cfg.TLSRequireClientCert)
	if err != nil {
		fmt.Fprintf(os.Stderr, "myceld TLS config error: %v\n", err)
		return 1
	}
	tokenManager, err := auth.NewRandomTokenManager(cfg.AccessTokenTTL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "myceld token manager error: %v\n", err)
		return 1
	}
	grpcServer, grpcErrCh, err := server.Start(serverCtx, server.Config{Addr: cfg.GRPCAddr, AdminLister: adminService, AdminAuthenticator: adminService, OperatorManager: adminService, BackupManager: backupService, UserManager: userService, SpaceManager: spaceService, TemplateManager: adminapi.NewAdminTemplateService(spaceService, adminService), SessionManager: sessionService, GraphManager: graphService, BlobManager: blobService, SemanticManager: semanticService, ChangeManager: changeService, TokenManager: tokenManager, Logger: rt.Logger, TLSConfig: tlsConfig, ClusterBackendAuthToken: cfg.Cluster.BackendAuthToken, Quiesce: rt.Quiesce, ClusteringManager: rt.ClusterManager, ClusteringServer: rt.ClusterManager.BackendService(), ReplicationProgress: rt.ReplicationProgress, ReplicationFollower: rt.ReplicationFollower, WALStatus: rt.WAL, WALCheckpoint: rt.WALCheckpoint, ResyncCoordinator: rt.ResyncCoordinator, SwitchoverCoordinator: rt.SwitchoverCoordinator, FailoverCoordinator: rt.FailoverCoordinator})
	if err != nil {
		fmt.Fprintf(os.Stderr, "myceld grpc startup failed: %v\n", err)
		return 1
	}
	defer stopServer()

	rt.Logger.Info("daemon ready", "grpc_addr", grpcServer.Addr())
	if rt.ClusterManager != nil {
		_ = rt.ClusterManager.Start(serverCtx)
	}
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
	clusterManager, err := clustering.NewManager(ctx, clustering.Options{DataDir: cfg.DataDir, NodeName: cfg.NodeName, ClusterName: cfg.Cluster.Name, BackendAdvertiseAddr: cfg.Cluster.BackendAdvertiseAddr, BackendAuthToken: cfg.Cluster.BackendAuthToken, SeedPeers: cfg.Cluster.SeedPeers, Bootstrap: cfg.Cluster.Bootstrap, JoinToken: cfg.Cluster.JoinToken, JoinTokenFile: cfg.Cluster.JoinTokenFile}, logger)
	if err != nil {
		_ = rt.Close()
		return nil, fmt.Errorf("initialize clustering: %w", err)
	}
	rt.ClusterManager = clusterManager
	identity := clusterManager.Identity()
	rt.NodeIdentity = &identity
	rt.NodeState = clusterManager.State()
	if reg := clusterManager.Registration(); reg != nil {
		reg.Interval = cfg.Cluster.DiscoveryInterval
	}
	logger.Info("clustering ready", "node_id", identity.NodeID, "node_name", identity.NodeName, "cluster_id", identity.ClusterID, "cluster_name", identity.ClusterName, "node_state", rt.NodeState, "backend_advertise_addr", identity.BackendAdvertiseAddr)
	if cfg.WAL.Enabled {
		walDir := cfg.WAL.Dir
		if walDir == "" {
			walDir = filepath.Join(cfg.DataDir, "wal")
		}
		walManager, err := wal.Open(ctx, wal.Options{Dir: walDir, SegmentBytes: cfg.WAL.SegmentBytes})
		if err != nil {
			_ = rt.Close()
			return nil, fmt.Errorf("open wal: %w", err)
		}
		rt.WAL = walManager
		progress := wal.NewFileProgressStore(filepath.Join(cfg.DataDir, "meta", "wal", "progress.json"))
		rt.WALProgress = progress
		rt.WALCheckpoint = wal.NewCheckpointStore(filepath.Join(cfg.DataDir, "meta", "wal", "checkpoint.json"))
		rt.WALRecovery = wal.NewRecovery(walManager, rt.WALRegistry, progress)
		rt.WALWaiter = rt.WALRecovery.Waiter()
		clusterManager.SetBackendWAL(walManager)
		clusterManager.SetBackendCheckpoint(rt.WALCheckpoint)
		logger.Info("wal ready", "path", walDir, "last_committed_lsn", walManager.LastCommittedLSN())
	}
	adminService := admin.NewModule()
	userService := daemonuser.NewModule()
	spaceService := daemonspace.NewModule()
	sessionService := daemonsession.NewModule()
	graphService := daegraph.NewModule()
	blobService := daemonblob.NewModule(graphService)
	semanticService := daemonsemantic.NewModule()
	changeService := daemonchange.NewModule()
	backupService := daemonbackup.NewModule()
	if err := rt.InitServices(ctx, []daemonruntime.Service{adminService, userService, spaceService, sessionService, graphService, blobService, semanticService, changeService, backupService}); err != nil {
		_ = rt.Close()
		return nil, err
	}
	clusterManager.SetBackendBlobPayloadProvider(daemonblob.BackendPayloadProvider{Module: blobService})
	if rt.WALRecovery != nil {
		started := time.Now()
		applied, err := rt.WALRecovery.Recover(ctx)
		if err != nil {
			_ = rt.Close()
			return nil, fmt.Errorf("recover wal: %w", err)
		}
		logger.Info("wal recovery complete", "applied_lsn", applied, "last_committed_lsn", rt.WAL.LastCommittedLSN(), "duration", time.Since(started))
	}
	if rt.WALRegistry != nil && rt.ClusterManager != nil && rt.WAL != nil {
		repDir := filepath.Join(cfg.DataDir, "meta", "clustering", "replication")
		if err := replication.CleanupStaleSnapshotStaging(ctx, cfg.DataDir, 24*time.Hour); err != nil {
			logger.Warn("failed to cleanup stale snapshot staging", "error", err)
		}
		receiveLog := replication.NewReceiveLog(filepath.Join(repDir, "receive-log"))
		progress := replication.NewProgressStore(filepath.Join(repDir, "progress.json"))
		rt.ReplicationProgress = progress
		installer := &replication.SnapshotInstaller{DataDir: cfg.DataDir, Identity: rt.ClusterManager.Identity, Progress: progress, ReceiveLog: receiveLog, ReloadAfterInstall: rt.ReloadAfterSnapshot, Authority: func() (string, int64, bool) {
			a, ok := rt.ClusterManager.Authority()
			return a.Primary.NodeID, a.AuthorityEpoch, ok
		}}
		rt.ClusterManager.SetBackendSnapshotInstaller(installer)
		rt.ClusterManager.SetBackendReplicationStatus(replication.BackendReplicationStatusProvider{Progress: progress, Cluster: rt.ClusterManager})
		rt.ClusterManager.SetBackendAuthorityInstaller(replication.BackendAuthorityInstaller{Cluster: rt.ClusterManager, Progress: progress})
		applier := &replication.Applier{Log: receiveLog, Progress: progress, Registry: rt.WALRegistry, Logger: logger, PreApplyHook: daemonblob.PayloadPreApplyHook{Module: blobService, Cluster: rt.ClusterManager, Client: backend.Client{AuthToken: cfg.Cluster.BackendAuthToken}}}
		if err := applier.Replay(ctx); err != nil {
			_ = rt.Close()
			return nil, fmt.Errorf("replay replicated wal: %w", err)
		}
		rt.ReplicationFollower = &replication.Follower{Manager: rt.ClusterManager, Streamer: backend.Client{AuthToken: cfg.Cluster.BackendAuthToken}, Applier: applier, Progress: progress, Interval: cfg.Cluster.DiscoveryInterval, Logger: logger}
		rt.ResyncCoordinator = &replication.ResyncCoordinator{Cluster: rt.ClusterManager, History: replication.NewResyncHistoryStore(filepath.Join(repDir, "resync-history.json")), Creator: &replication.SnapshotCreator{DataDir: cfg.DataDir, ClusterID: identity.ClusterID, PrimaryNodeID: identity.NodeID, AuthorityEpoch: func() int64 {
			a, ok := rt.ClusterManager.Authority()
			if !ok {
				return 0
			}
			return a.AuthorityEpoch
		}(), Quiesce: rt.Quiesce, WAL: rt.WAL, Progress: rt.WALProgress, Checkpoint: rt.WALCheckpoint, Logger: logger}, Client: backend.Client{AuthToken: cfg.Cluster.BackendAuthToken}}
		rt.SwitchoverCoordinator = &replication.SwitchoverCoordinator{Cluster: rt.ClusterManager, DataDir: cfg.DataDir, WAL: rt.WAL, Quiesce: rt.Quiesce, Client: backend.Client{AuthToken: cfg.Cluster.BackendAuthToken}, Timeout: 60 * time.Second}
		rt.FailoverCoordinator = &replication.FailoverCoordinator{Cluster: rt.ClusterManager}
	}
	graphService.SetChangeSink(graphchange.SinkFunc(func(ctx context.Context, event graphchange.CommittedEvent) error {
		appender, err := semanticService.DirtyEventAppender(ctx, event.SpaceID)
		if err != nil {
			return err
		}
		return appender.OnGraphCommitted(ctx, event)
	}))
	if err := rt.StartServices(ctx); err != nil {
		_ = rt.Close()
		return nil, err
	}
	if rt.ReplicationFollower != nil {
		if err := rt.ReplicationFollower.Start(ctx); err != nil {
			_ = rt.Close()
			return nil, fmt.Errorf("start wal replication follower: %w", err)
		}
	}
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
		"access_token_ttl", cfg.AccessTokenTTL,
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
