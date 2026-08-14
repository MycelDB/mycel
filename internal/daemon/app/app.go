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

	automationservice "github.com/myceldb/mycel/internal/automation/service"
	backupservice "github.com/myceldb/mycel/internal/backup/service"
	blobservice "github.com/myceldb/mycel/internal/blob/service"
	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/logging"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/daemon/server"
	"github.com/myceldb/mycel/internal/graph/change"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	identityservice "github.com/myceldb/mycel/internal/identity/service"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
	spaceservice "github.com/myceldb/mycel/internal/space/service"
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

	principalService, ok := daemonruntime.ServiceAs[*identityservice.PrincipalModule](rt, identityservice.PrincipalModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: identity service is not registered\n")
		return 1
	}
	serverCtx, stopServer := context.WithCancel(ctx)
	spaceService, ok := daemonruntime.ServiceAs[*spaceservice.Module](rt, spaceservice.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: space service is not registered\n")
		return 1
	}
	sessionService, ok := daemonruntime.ServiceAs[*sessionservice.Module](rt, sessionservice.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: session service is not registered\n")
		return 1
	}
	graphService, ok := daemonruntime.ServiceAs[*graphservice.Module](rt, graphservice.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: graph service is not registered\n")
		return 1
	}
	graphNotificationService, ok := daemonruntime.ServiceAs[*graphnotification.Module](rt, graphnotification.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: graph change notification service is not registered\n")
		return 1
	}
	blobService, ok := daemonruntime.ServiceAs[*blobservice.Module](rt, blobservice.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: blob service is not registered\n")
		return 1
	}
	semanticService, ok := daemonruntime.ServiceAs[*daemonsemantic.Module](rt, daemonsemantic.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: semantic service is not registered\n")
		return 1
	}
	schemaService, ok := daemonruntime.ServiceAs[*schemaservice.Module](rt, schemaservice.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: schema service is not registered\n")
		return 1
	}
	automationService, ok := daemonruntime.ServiceAs[*automationservice.Module](rt, automationservice.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: automation service is not registered\n")
		return 1
	}
	backupService, ok := daemonruntime.ServiceAs[*backupservice.Module](rt, backupservice.ModuleName)
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
	inferenceService, ok := daemonruntime.ServiceAs[*inferenceservice.Module](rt, inferenceservice.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: inference service is not registered\n")
		return 1
	}
	grpcServer, grpcErrCh, err := server.Start(serverCtx, server.Config{Addr: cfg.GRPCAddr, PrincipalManager: principalService, BackupManager: backupService, SpaceManager: spaceService, SessionManager: sessionService, GraphManager: graphService, GraphChangeManager: graphNotificationService, BlobManager: blobService, InferenceManager: inferenceService, SemanticManager: semanticService, SchemaManager: schemaService, AutomationManager: automationService, TokenManager: tokenManager, Logger: rt.Logger, TLSConfig: tlsConfig, ClusterBackendAuthToken: cfg.Cluster.BackendAuthToken, Quiesce: rt.Quiesce, ClusteringManager: rt.ClusterManager, ClusteringServer: rt.ClusterManager.BackendService(), WALStatus: rt.WAL, WALCheckpoint: rt.WALCheckpoint, ClusterConfig: cfg.Cluster, RaftGroups: rt.RaftGroups, RaftTransportDiagnostics: rt.RaftTransportDiagnostics})
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
	clusterManager, err := clustering.NewManager(ctx, clustering.Options{DataDir: cfg.DataDir, NodeName: cfg.NodeName, ClusterName: cfg.Cluster.Name, BackendAdvertiseAddr: cfg.Cluster.BackendAdvertiseAddr, BackendAuthToken: cfg.Cluster.BackendAuthToken, RaftMode: raftRuntimeConfigured(cfg), RaftLocalNodeID: uint64(cfg.Cluster.RaftLocalNodeID), RaftNodeCount: cfg.Cluster.RaftNodeCount}, logger)
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
	principalService := identityservice.NewPrincipalManager()
	spaceService := spaceservice.NewModule()
	sessionService := sessionservice.NewModule()
	schemaService := schemaservice.NewModule("")
	graphService := graphservice.NewModule()
	graphNotificationService := graphnotification.NewModule()
	automationService := automationservice.NewModule("").WithGraphRuntime(sessionService, graphService).WithSchemaManager(schemaService).WithWorkerConfig(automationservice.WorkerConfig{Enabled: cfg.Automation.WorkerEnabled, Interval: cfg.Automation.WorkerInterval, BatchSize: cfg.Automation.WorkerBatchSize, MaxTokensPerRun: cfg.Automation.MaxTokensPerRun, MaxCostPerRun: cfg.Automation.MaxCostPerRun, Concurrency: cfg.Automation.WorkerConcurrency})
	if automationProvider, err := automationProviderFromConfig(cfg); err != nil {
		_ = rt.Close()
		return nil, err
	} else if automationProvider != nil {
		automationService.WithProvider(automationProvider)
	}
	blobService := blobservice.NewModule(graphService)
	inferenceService := inferenceservice.NewModule()
	semanticService := daemonsemantic.NewModule(daemonsemantic.Config{
		SecretKeyB64:     cfg.UserStoreEncryptionKeyB64,
		SchemaManager:    schemaService,
		GraphReadManager: graphService,
		InferenceManager: inferenceService,
		MaintenanceConfig: daemonsemantic.MaintenanceConfig{
			Enabled:                    cfg.SemanticMaintenance.Enabled,
			DirtyCooldown:              cfg.SemanticMaintenance.DirtyCooldown,
			AnalyzerInterval:           cfg.SemanticMaintenance.AnalyzerInterval,
			WorkerInterval:             cfg.SemanticMaintenance.WorkerInterval,
			WorkerCount:                cfg.SemanticMaintenance.WorkerCount,
			MaxBatchSize:               cfg.SemanticMaintenance.MaxBatchSize,
			MaxConcurrentProviderCalls: cfg.SemanticMaintenance.MaxConcurrentProviderCalls,
			MaxRequestsPerMinute:       cfg.SemanticMaintenance.MaxRequestsPerMinute,
			MaxTokensPerMinute:         cfg.SemanticMaintenance.MaxTokensPerMinute,
			ProviderDefaults: daemonsemantic.ThrottleConfig{
				MaxConcurrentCalls:   cfg.SemanticMaintenance.ProviderDefaults.MaxConcurrentCalls,
				MaxRequestsPerMinute: cfg.SemanticMaintenance.ProviderDefaults.MaxRequestsPerMinute,
				MaxTokensPerMinute:   cfg.SemanticMaintenance.ProviderDefaults.MaxTokensPerMinute,
			},
			CredentialDefaults: daemonsemantic.ThrottleConfig{
				MaxConcurrentCalls:   cfg.SemanticMaintenance.CredentialDefaults.MaxConcurrentCalls,
				MaxRequestsPerMinute: cfg.SemanticMaintenance.CredentialDefaults.MaxRequestsPerMinute,
				MaxTokensPerMinute:   cfg.SemanticMaintenance.CredentialDefaults.MaxTokensPerMinute,
			},
		},
	})
	backupService := backupservice.NewModule(backupservice.Config{
		Enabled:                cfg.Backup.Enabled,
		BackupDir:              cfg.Backup.BackupDir,
		Interval:               cfg.Backup.Interval,
		RetentionCount:         cfg.Backup.RetentionCount,
		IncludeLogs:            cfg.Backup.IncludeLogs,
		Compression:            cfg.Backup.Compression,
		QuiesceDrainTimeout:    cfg.Backup.QuiesceDrainTimeout,
		BackupTimeout:          cfg.Backup.BackupTimeout,
		RetryAfter:             cfg.Backup.RetryAfter,
		StatusHistoryLimit:     cfg.Backup.StatusHistoryLimit,
		AllowReadsDuringBackup: cfg.Backup.AllowReadsDuringBackup,
	})
	if err := rt.InitServices(ctx, []daemonruntime.Service{principalService, spaceService, sessionService, schemaService, automationService, graphService, graphNotificationService, blobService, inferenceService, semanticService, backupService}); err != nil {
		_ = rt.Close()
		return nil, err
	}
	graphService.SetBlobReferenceChecker(blobService)
	graphNotificationService.WithLeaderGate(func(ctx context.Context, event graphchange.CommittedEvent) error {
		return graphService.RequireLocalGraphWriteLeader(ctx, event.SpaceID.String())
	})
	if _, err := graphNotificationService.RegisterConsumer(ctx, graphnotification.ConsumerSpec{ConsumerName: "automation", Filter: graphchange.Filter{EventTypes: []graphchange.ChangeType{graphchange.ChangeTypeNodeCreated, graphchange.ChangeTypeNodeUpdated}}, Projection: graphchange.Projection{IncludeRevision: true, IncludeNewNodeSnapshot: true, IncludeOldNodeSnapshot: true}, Lossless: true}, automationService); err != nil {
		_ = rt.Close()
		return nil, err
	}
	if raftRuntimeConfigured(cfg) {
		backupService.PrepareExperimentalRaftMode()
		systemMetadataSM := consensus.NewSystemStateMachine()
		if err := initializeExperimentalRaft(ctx, rt, func() consensus.StateMachine {
			return compositeSystemStateMachine{systemMetadataSM, identityservice.PrincipalRaftStateMachine{Module: principalService}, backupservice.RaftStateMachine{Module: backupService}, daemonsemantic.RaftStateMachine{Module: semanticService, System: true, PartitionCount: uint32(cfg.Cluster.RaftPartitionCount)}}
		}, func(partitionID uint32) consensus.StateMachine {
			partitionCount := uint32(cfg.Cluster.RaftPartitionCount)
			return compositePartitionStateMachine{
				spaceservice.RaftStateMachine{Module: spaceService, PartitionID: partitionID, PartitionCount: partitionCount},
				schemaservice.RaftStateMachine{Manager: schemaService.SchemaManager, PartitionID: partitionID, PartitionCount: partitionCount},
				graphservice.RaftStateMachine{Module: graphService, PartitionID: partitionID, PartitionCount: partitionCount},
				blobservice.RaftStateMachine{Module: blobService, PartitionID: partitionID, PartitionCount: partitionCount},
				daemonsemantic.RaftStateMachine{Module: semanticService, PartitionID: partitionID, PartitionCount: partitionCount},
			}
		}); err != nil {
			_ = rt.Close()
			return nil, err
		}
		spaceService.EnableExperimentalRaft(rt.RaftGroups, consensus.NodeID(cfg.Cluster.RaftLocalNodeID), cfg.Cluster.RaftNodeAddrs, cfg.Cluster.BackendAuthToken)
		schemaService.EnableExperimentalRaft(rt.RaftGroups, uint32(cfg.Cluster.RaftPartitionCount))
		graphService.EnableExperimentalRaft(rt.RaftGroups, uint32(cfg.Cluster.RaftPartitionCount))
		graphService.EnableExperimentalRaftNetworking(consensus.NodeID(cfg.Cluster.RaftLocalNodeID), cfg.Cluster.RaftNodeAddrs, cfg.Cluster.BackendAuthToken)
		blobService.EnableExperimentalRaft(rt.RaftGroups, uint32(cfg.Cluster.RaftPartitionCount))
		blobService.EnableExperimentalRaftNetworking(consensus.NodeID(cfg.Cluster.RaftLocalNodeID), cfg.Cluster.RaftNodeAddrs, cfg.Cluster.BackendAuthToken, identity.ClusterID)
		semanticService.EnableExperimentalRaft(rt.RaftGroups, uint32(cfg.Cluster.RaftPartitionCount))
		semanticService.EnableExperimentalRaftNetworking(consensus.NodeID(cfg.Cluster.RaftLocalNodeID), cfg.Cluster.RaftNodeAddrs, cfg.Cluster.BackendAuthToken)
		backupService.EnableExperimentalRaft(rt.RaftGroups)
		backupService.EnableClusterBackupNetworking(uint64(cfg.Cluster.RaftLocalNodeID), cfg.Cluster.RaftNodeAddrs, cfg.Cluster.BackendAuthToken)
		principalService.EnableExperimentalRaft(rt.RaftGroups)
		startSystemMetadataBootstrap(ctx, rt, systemMetadataSM)
	}
	clusterManager.SetBackendBlobPayloadProvider(blobservice.BackendPayloadProvider{Module: blobService})
	clusterManager.SetBackendClusterBackupProvider(backupService)
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
	semanticSink := graphchange.SinkFunc(func(ctx context.Context, event graphchange.CommittedEvent) error {
		appender, err := semanticService.DirtyEventAppender(ctx, event.SpaceID)
		if err != nil {
			return err
		}
		return appender.OnGraphCommitted(ctx, event)
	})
	graphService.SetChangeSink(graphchange.MultiSink{graphNotificationService, semanticSink})
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
