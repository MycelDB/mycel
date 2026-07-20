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
	"github.com/myceldb/mycel/internal/clustering/consensus"
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
	grpcServer, grpcErrCh, err := server.Start(serverCtx, server.Config{Addr: cfg.GRPCAddr, AdminLister: adminService, AdminAuthenticator: adminService, OperatorManager: adminService, BackupManager: backupService, UserManager: userService, SpaceManager: spaceService, TemplateManager: adminapi.NewAdminTemplateService(spaceService, adminService), SessionManager: sessionService, GraphManager: graphService, BlobManager: blobService, SemanticManager: semanticService, ChangeManager: changeService, TokenManager: tokenManager, Logger: rt.Logger, TLSConfig: tlsConfig, ClusterBackendAuthToken: cfg.Cluster.BackendAuthToken, Quiesce: rt.Quiesce, ClusteringManager: rt.ClusterManager, ClusteringServer: rt.ClusterManager.BackendService(), WALStatus: rt.WAL, WALCheckpoint: rt.WALCheckpoint, ClusterConfig: cfg.Cluster, RaftGroups: rt.RaftGroups})
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
	clusterManager, err := clustering.NewManager(ctx, clustering.Options{DataDir: cfg.DataDir, NodeName: cfg.NodeName, ClusterName: cfg.Cluster.Name, BackendAdvertiseAddr: cfg.Cluster.BackendAdvertiseAddr, BackendAuthToken: cfg.Cluster.BackendAuthToken}, logger)
	if err != nil {
		_ = rt.Close()
		return nil, fmt.Errorf("initialize clustering: %w", err)
	}
	rt.ClusterManager = clusterManager
	if rt.RaftRouter != nil {
		clusterManager.SetBackendRaftRouter(rt.RaftRouter)
	}
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
	if raftRuntimeConfigured(cfg) {
		if err := initializeExperimentalRaft(ctx, rt, func() consensus.StateMachine {
			return compositeSystemStateMachine{consensus.NewSystemStateMachine(), daemonuser.RaftStateMachine{Module: userService}, admin.RaftStateMachine{Module: adminService}}
		}, func(partitionID uint32) consensus.StateMachine {
			partitionCount := uint32(cfg.Cluster.RaftPartitionCount)
			return compositePartitionStateMachine{
				daemonspace.RaftStateMachine{Module: spaceService, PartitionCount: partitionCount},
				daegraph.RaftStateMachine{Module: graphService, PartitionCount: partitionCount},
				daemonblob.RaftStateMachine{Module: blobService, PartitionCount: partitionCount},
				daemonsemantic.RaftStateMachine{Module: semanticService, PartitionCount: partitionCount},
			}
		}); err != nil {
			_ = rt.Close()
			return nil, err
		}
		spaceService.EnableExperimentalRaft(rt.RaftGroups, consensus.NodeID(cfg.Cluster.RaftLocalNodeID), cfg.Cluster.RaftNodeAddrs, cfg.Cluster.BackendAuthToken)
		graphService.EnableExperimentalRaft(rt.RaftGroups, uint32(cfg.Cluster.RaftPartitionCount))
		graphService.EnableExperimentalRaftNetworking(consensus.NodeID(cfg.Cluster.RaftLocalNodeID), cfg.Cluster.RaftNodeAddrs, cfg.Cluster.BackendAuthToken)
		blobService.EnableExperimentalRaft(rt.RaftGroups, uint32(cfg.Cluster.RaftPartitionCount))
		blobService.EnableExperimentalRaftNetworking(consensus.NodeID(cfg.Cluster.RaftLocalNodeID), cfg.Cluster.RaftNodeAddrs, cfg.Cluster.BackendAuthToken, identity.ClusterID)
		semanticService.EnableExperimentalRaft(rt.RaftGroups, uint32(cfg.Cluster.RaftPartitionCount))
		semanticService.EnableExperimentalRaftNetworking(consensus.NodeID(cfg.Cluster.RaftLocalNodeID), cfg.Cluster.RaftNodeAddrs, cfg.Cluster.BackendAuthToken)
		userService.EnableExperimentalRaft(rt.RaftGroups)
		adminService.EnableExperimentalRaft(rt.RaftGroups)
	}
	clusterManager.SetBackendBlobPayloadProvider(daemonblob.BackendPayloadProvider{Module: blobService})
	clusterManager.SetBackendSpaceReader(spaceService)
	clusterManager.SetBackendGraphReader(graphService)
	clusterManager.SetBackendSemanticReader(semanticService)
	if rt.WALRecovery != nil {
		started := time.Now()
		applied, err := rt.WALRecovery.Recover(ctx)
		if err != nil {
			_ = rt.Close()
			return nil, fmt.Errorf("recover wal: %w", err)
		}
		logger.Info("wal recovery complete", "applied_lsn", applied, "last_committed_lsn", rt.WAL.LastCommittedLSN(), "duration", time.Since(started))
	}
	// Local WAL durability and recovery remain active above; clustered operation is Raft-only.
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
	logger.Info("daemon initialization complete")
	return rt, nil
}

func raftRuntimeConfigured(cfg config.Config) bool {
	// A daemon with an explicit Raft node address map participates in clustered Raft.
	// A single-node Raft configuration is also valid for local tests/dev. Default
	// standalone daemons without Raft addresses remain non-clustered.
	return len(cfg.Cluster.RaftNodeAddrs) > 0 || cfg.Cluster.RaftNodeCount == 1
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
