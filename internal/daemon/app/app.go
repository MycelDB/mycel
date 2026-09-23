package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	activitymodel "github.com/myceldb/mycel/internal/activity/model"
	activityservice "github.com/myceldb/mycel/internal/activity/service"
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
	"github.com/myceldb/mycel/internal/encryption"
	"github.com/myceldb/mycel/internal/fsperm"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/graph/writetrace"
	identityservice "github.com/myceldb/mycel/internal/identity/service"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
	spaceservice "github.com/myceldb/mycel/internal/space/service"
	"github.com/myceldb/mycel/internal/wal"
)

const LogFilename = "myceld.log"

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func ensureFreshEncryptedDeploymentMarker(cfg config.Config) error {
	dataDir := cfg.DataDir
	marker := encryptionMarkerPath(dataDir)
	if _, err := os.Stat(marker); err == nil {
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("check encryption marker: %w", err)
	}
	for _, path := range encryptionPlaintextConflictPaths(cfg) {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("encryption at rest is enabled but existing plaintext artifact path %q has no encryption marker; create a fresh encrypted deployment", path)
		} else if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("check plaintext artifact path %q: %w", path, err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(marker), fsperm.PrivateDir); err != nil {
		return fmt.Errorf("create encryption metadata directory: %w", err)
	}
	if err := os.WriteFile(marker, []byte("{\"version\":1,\"mode\":\"enabled\"}\n"), fsperm.PrivateFile); err != nil {
		return fmt.Errorf("write encryption marker: %w", err)
	}
	return nil
}

func encryptionMarkerPath(dataDir string) string {
	return filepath.Join(dataDir, "meta", "encryption", "at_rest.json")
}

func encryptionPlaintextConflictPaths(cfg config.Config) []string {
	dataDir := cfg.DataDir
	paths := []string{
		filepath.Join(dataDir, "wal"),
		filepath.Join(dataDir, "graph"),
		filepath.Join(dataDir, "blob"),
		filepath.Join(dataDir, "blobs"),
		filepath.Join(dataDir, "backup"),
		filepath.Join(dataDir, "backups"),
		filepath.Join(dataDir, "search"),
		filepath.Join(dataDir, "semantic"),
		filepath.Join(dataDir, "automation"),
		filepath.Join(dataDir, "meta", "raft"),
	}
	if walDir := strings.TrimSpace(cfg.WAL.Dir); walDir != "" {
		paths = append(paths, walDir)
	}
	return paths
}

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
	lexicalService, ok := daemonruntime.ServiceAs[*lexicalservice.Module](rt, lexicalservice.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: lexical search service is not registered\n")
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
	activityService, ok := daemonruntime.ServiceAs[*activityservice.Module](rt, activityservice.ModuleName)
	if !ok {
		fmt.Fprintf(os.Stderr, "myceld initialization failed: activity service is not registered\n")
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
	grpcServer, grpcErrCh, err := server.Start(serverCtx, server.Config{Addr: cfg.GRPCAddr, PrincipalManager: principalService, ActivityManager: activityService, BackupManager: backupService, SpaceManager: spaceService, SessionManager: sessionService, GraphManager: graphService, GraphChangeManager: graphNotificationService, BlobManager: blobService, InferenceManager: inferenceService, SemanticManager: semanticService, LexicalManager: lexicalService, SchemaManager: schemaService, AutomationManager: automationService, TokenManager: tokenManager, Logger: rt.Logger, TLSConfig: tlsConfig, ClusterBackendAuthToken: cfg.Cluster.BackendAuthToken, Quiesce: rt.Quiesce, ClusteringManager: rt.ClusterManager, ClusteringServer: rt.ClusterManager.BackendService(), WALStatus: rt.WAL, WALCheckpoint: rt.WALCheckpoint, ClusterConfig: cfg.Cluster, RaftGroups: rt.RaftGroups, RaftTransportDiagnostics: rt.RaftTransportDiagnostics})
	if err != nil {
		fmt.Fprintf(os.Stderr, "myceld grpc startup failed: %v\n", err)
		return 1
	}
	defer stopServer()

	rt.Logger.Info("daemon ready", "grpc_addr", grpcServer.Addr())
	_ = activityService.Emit(context.Background(), activitymodel.SeverityInfo, activitymodel.CategoryLifecycle, "daemon.started", "Daemon started", nil)
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
	_ = activityService.Emit(context.Background(), activitymodel.SeverityInfo, activitymodel.CategoryLifecycle, "daemon.stopped", "Daemon stopped", nil)
	rt.Logger.Info("daemon shutdown complete")
	return 0
}

func Initialize(ctx context.Context, cfg config.Config) (*daemonruntime.Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	dataDirCreated, err := ensureDir(cfg.DataDir, fsperm.PrivateDir)
	if err != nil {
		return nil, fmt.Errorf("ensure data directory: %w", err)
	}
	logDir := filepath.Join(cfg.DataDir, "log")
	logDirCreated, err := ensureDir(logDir, fsperm.PrivateDir)
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
	enc, err := encryption.NewService(ctx, cfg.Encryption)
	if err != nil {
		return nil, fmt.Errorf("initialize encryption at rest: %w", err)
	}
	if enc.Enabled() {
		if err := ensureFreshEncryptedDeploymentMarker(cfg); err != nil {
			return nil, err
		}
		keyID, _ := enc.ActiveKeyID(ctx)
		logger.Info("encryption at rest enabled", "provider", enc.ProviderID(), "active_key_id", keyID)
	} else {
		logger.Warn("encryption at rest disabled", "mode", cfg.Encryption.NormalizedMode())
	}

	rt := daemonruntime.New(cfg, logger, logPath, configuredLogger.Close)
	rt.Encryption = enc
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
		walManager, err := wal.Open(ctx, wal.Options{Dir: walDir, SegmentBytes: cfg.WAL.SegmentBytes, Encryption: enc})
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
	activityService := activityservice.NewModule()
	spaceService := spaceservice.NewModule()
	sessionService := sessionservice.NewModule()
	schemaService := schemaservice.NewModule("")
	graphService := graphservice.NewModule()
	graphNotificationService := graphnotification.NewModule()
	inferenceService := inferenceservice.NewModule().WithPrincipalStatusChecker(principalService)
	automationService := automationservice.NewModule("").WithGraphRuntime(sessionService, graphService).WithSchemaManager(schemaService).WithInferenceManager(inferenceService).WithWorkerConfig(automationservice.WorkerConfig{Enabled: cfg.Automation.WorkerEnabled, Interval: cfg.Automation.WorkerInterval, BatchSize: cfg.Automation.WorkerBatchSize, MaxInputTokens: cfg.Automation.MaxInputTokens, MaxOutputTokens: cfg.Automation.MaxOutputTokens, Concurrency: cfg.Automation.WorkerConcurrency})
	blobService := blobservice.NewModule(graphService, blobservice.Config{
		Backend:             cfg.Blob.Backend,
		ObjectStoreProvider: cfg.Blob.ObjectStoreProvider,
		S3Bucket:            cfg.Blob.S3Bucket,
		S3Prefix:            cfg.Blob.S3Prefix,
		S3Region:            cfg.Blob.S3Region,
		S3KMSKeyID:          cfg.Blob.S3KMSKeyID,
		S3EndpointURL:       cfg.Blob.S3EndpointURL,
		S3ForcePathStyle:    cfg.Blob.S3ForcePathStyle,
	})
	automationService.WithBlobManager(blobService)
	inferenceService.SetSecretResolver(inferenceservice.NewEnvelopeSecretResolver(enc))
	lexicalService := lexicalservice.NewModule()
	semanticService := daemonsemantic.NewModule(daemonsemantic.Config{
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
	if err := rt.InitServices(ctx, []daemonruntime.Service{principalService, activityService, spaceService, sessionService, schemaService, automationService, graphService, graphNotificationService, blobService, inferenceService, semanticService, lexicalService, backupService}); err != nil {
		_ = rt.Close()
		return nil, err
	}
	if rt.ClusterManager != nil {
		rt.ClusterManager.SetActivityEmitter(activityService)
	}
	graphService.SetBlobReferenceChecker(blobService)
	graphService.SetAutomationOutputFenceValidator(automationService)
	automationService.WithGraphChangeReplayer(graphNotificationService)
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
				automationservice.RaftStateMachine{Manager: automationService.AutomationManager, PartitionID: partitionID, PartitionCount: partitionCount},
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
		automationService.EnableExperimentalRaft(rt.RaftGroups, uint32(cfg.Cluster.RaftPartitionCount))
		automationService.EnableExperimentalRaftNetworking(consensus.NodeID(cfg.Cluster.RaftLocalNodeID), cfg.Cluster.RaftNodeAddrs, cfg.Cluster.BackendAuthToken)
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
	clusterManager.SetBackendAutomationRuntimeReader(automationService)
	if rt.WALRecovery != nil {
		started := time.Now()
		applied, err := rt.WALRecovery.Recover(ctx)
		if err != nil {
			_ = rt.Close()
			return nil, fmt.Errorf("recover wal: %w", err)
		}
		logger.Info("wal recovery complete", "applied_lsn", applied, "last_committed_lsn", rt.WAL.LastCommittedLSN(), "duration", time.Since(started))
	}
	if err := principalService.EnsureBootstrapPrincipals(ctx, cfg.Mode, cfg.BootstrapAdminUsername, cfg.BootstrapAdminPassword, logger); err != nil {
		_ = rt.Close()
		return nil, fmt.Errorf("ensure bootstrap principals: %w", err)
	}
	// Local WAL durability and recovery remain active above; clustered operation is Raft-only.
	writeTrace := writetrace.FromEnv()
	semanticSink := graphchange.SinkFunc(func(ctx context.Context, event graphchange.CommittedEvent) error {
		traceStart := time.Now()
		stepStart := time.Now()
		appender, err := semanticService.DirtyEventAppender(ctx, event.SpaceID)
		resolveDuration := time.Since(stepStart)
		appendDuration := time.Duration(0)
		if err == nil {
			stepStart = time.Now()
			err = appender.OnGraphCommitted(ctx, event)
			appendDuration = time.Since(stepStart)
		}
		total := time.Since(traceStart)
		if writeTrace.ShouldLog(total) {
			logger.Info("graph write semantic sink timing",
				"event", "graph_write_semantic_sink_timing",
				"space_id", event.SpaceID.String(),
				"domain_id", event.DomainID.String(),
				"transaction_id", event.TxnID.String(),
				"graph_revision", event.GraphRevision,
				"total_ms", writetrace.MS(total),
				"appender_resolve_ms", writetrace.MS(resolveDuration),
				"dirty_event_append_ms", writetrace.MS(appendDuration),
				"changes", len(event.Changes),
				"created_nodes", len(event.CreatedNodeIDs),
				"updated_nodes", len(event.UpdatedNodeIDs),
				"deleted_nodes", len(event.DeletedNodeIDs),
				"changed_edges", len(event.ChangedEdges),
				"affected_nodes", len(event.AffectedNodeIDs),
				"affected_edges", len(event.AffectedEdgeIDs),
				"error", errorString(err),
			)
		}
		return err
	})
	lexicalSink := graphchange.SinkFunc(func(ctx context.Context, event graphchange.CommittedEvent) error {
		return lexicalService.OnGraphCommitted(ctx, event)
	})
	if raftRuntimeConfigured(cfg) {
		graphService.SetChangeSink(graphchange.MultiSink{graphchange.Named("semantic", semanticSink), graphchange.Named("lexical", lexicalSink)})
		graphService.SetRaftApplyChangeSink(graphchange.Named("graph_change_notification", graphNotificationService))
	} else {
		graphService.SetChangeSink(graphchange.MultiSink{graphchange.Named("graph_change_notification", graphNotificationService), graphchange.Named("semantic", semanticSink), graphchange.Named("lexical", lexicalSink)})
	}
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
