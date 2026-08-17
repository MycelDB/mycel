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

	automationservice "github.com/myceldb/mycel/internal/automation/service"
	daemonbackup "github.com/myceldb/mycel/internal/backup/service"
	daemonblob "github.com/myceldb/mycel/internal/blob/service"
	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	adminapi "github.com/myceldb/mycel/internal/daemon/api/admin"
	clientapi "github.com/myceldb/mycel/internal/daemon/api/client"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	daemoninference "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	Addr                     string
	PrincipalManager         adminapi.PrincipalManager
	BackupManager            daemonbackup.Manager
	SpaceManager             daemonspace.Manager
	SessionManager           daemonsession.Manager
	GraphManager             daegraph.Manager
	GraphChangeManager       graphnotification.Manager
	BlobManager              daemonblob.Manager
	InferenceManager         daemoninference.Manager
	SemanticManager          daemonsemantic.Manager
	SchemaManager            schemaservice.Manager
	AutomationManager        automationservice.Manager
	TokenManager             *daemonauth.TokenManager
	Quiesce                  *quiesce.Coordinator
	IngressGate              *quiesce.Gate
	QuiesceExempt            map[string]bool
	Logger                   *slog.Logger
	TLSConfig                *tls.Config
	ClusterBackendAuthToken  string
	ClusteringManager        *clustering.Manager
	ClusteringServer         clusterpb.ClusterBackendServiceServer
	WALStatus                adminapi.WALStatusProvider
	WALCheckpoint            *wal.CheckpointStore
	ClusterConfig            daemonconfig.ClusterConfig
	RaftGroups               *consensus.MultiGroup
	RaftTransportDiagnostics *consensus.TransportDiagnostics
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
	if cfg.PrincipalManager == nil {
		return nil, fmt.Errorf("principal manager is required")
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
	if cfg.GraphChangeManager == nil {
		return nil, fmt.Errorf("graph change manager is required")
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
	publicMethods := defaultPublicMethods()
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
		grpc.ChainUnaryInterceptor(clusterBackendUnaryAuthInterceptor(cfg.ClusterBackendAuthToken), cfg.TokenManager.UnaryInterceptor(publicMethods), quiesceUnaryInterceptor(cfg.IngressGate, quiesceExempt)),
		grpc.ChainStreamInterceptor(clusterBackendStreamAuthInterceptor(cfg.ClusterBackendAuthToken), cfg.TokenManager.StreamInterceptor(publicMethods), quiesceStreamInterceptor(cfg.IngressGate, quiesceExempt)),
	}
	if cfg.TLSConfig != nil {
		baseOptions = append(baseOptions, grpc.Creds(credentials.NewTLS(cfg.TLSConfig)))
	}
	serverOptions := append(baseOptions, opts...)
	grpcServer := grpc.NewServer(serverOptions...)
	if cfg.ClusteringServer != nil {
		clusterpb.RegisterClusterBackendServiceServer(grpcServer, cfg.ClusteringServer)
	}
	commonv1.RegisterAuthServiceServer(grpcServer, clientapi.NewAuthService(cfg.PrincipalManager, cfg.TokenManager))
	adminv1.RegisterAdminPrincipalServiceServer(grpcServer, adminapi.NewPrincipalService(cfg.PrincipalManager, cfg.TokenManager))
	adminv1.RegisterAdminSpaceServiceServer(grpcServer, adminapi.NewAdminSpaceService(cfg.SpaceManager, cfg.PrincipalManager, cfg.PrincipalManager))
	adminv1.RegisterAdminDomainServiceServer(grpcServer, adminapi.NewAdminDomainService(cfg.SpaceManager, cfg.PrincipalManager))
	adminInference := adminapi.NewAdminInferenceService(cfg.SemanticManager, cfg.InferenceManager, cfg.PrincipalManager)
	adminv1.RegisterAdminInferenceCatalogServiceServer(grpcServer, adminInference)
	adminv1.RegisterAdminInferenceProfileServiceServer(grpcServer, adminInference)
	adminv1.RegisterAdminInferenceCredentialServiceServer(grpcServer, adminInference)
	adminv1.RegisterAdminInferenceGrantServiceServer(grpcServer, adminInference)
	adminv1.RegisterAdminInferencePolicyServiceServer(grpcServer, adminInference)
	adminv1.RegisterAdminInferenceUsageServiceServer(grpcServer, adminInference)
	adminv1.RegisterAdminSemanticServiceServer(grpcServer, adminapi.NewAdminSemanticService(cfg.SemanticManager, cfg.SpaceManager, cfg.PrincipalManager))
	adminv1.RegisterAdminSemanticMaintenanceServiceServer(grpcServer, adminapi.NewAdminSemanticMaintenanceService(cfg.SemanticManager, cfg.PrincipalManager))
	adminv1.RegisterAdminSemanticMigrationServiceServer(grpcServer, adminapi.NewAdminSemanticMigrationService(cfg.SemanticManager, cfg.SpaceManager, cfg.PrincipalManager))
	if cfg.SchemaManager != nil {
		adminv1.RegisterAdminSchemaServiceServer(grpcServer, adminapi.NewAdminSchemaService(cfg.SchemaManager))
	}
	if cfg.AutomationManager != nil {
		adminv1.RegisterAdminAutomationServiceServer(grpcServer, adminapi.NewAdminAutomationService(cfg.AutomationManager, cfg.PrincipalManager))
	}
	if cfg.ClusteringManager != nil {
		clusterAdmin := adminapi.NewAdminClusterService(cfg.ClusteringManager, cfg.PrincipalManager).WithWALStatus(cfg.WALStatus, cfg.WALCheckpoint).WithClusterRuntime(cfg.ClusterConfig, cfg.RaftGroups, cfg.RaftTransportDiagnostics)
		if provider, ok := cfg.GraphManager.(adminapi.LocalGraphConsistencyProvider); ok {
			clusterAdmin.WithGraphConsistency(provider)
		}
		if provider, ok := cfg.GraphManager.(adminapi.LocalGraphForensicExportProvider); ok {
			clusterAdmin.WithGraphForensicExport(provider)
		}
		adminv1.RegisterAdminClusterServiceServer(grpcServer, clusterAdmin)
	}
	if cfg.BackupManager != nil {
		adminv1.RegisterAdminBackupServiceServer(grpcServer, adminapi.NewAdminBackupService(cfg.BackupManager, cfg.Quiesce, cfg.PrincipalManager).WithClusterRuntime(cfg.ClusteringManager, cfg.ClusterConfig))
	}
	var clientRouter clientapi.ClientRequestRouter
	localNode := consensus.NodeID(cfg.ClusterConfig.RaftLocalNodeID)
	if cfg.ClusteringManager != nil {
		identity := cfg.ClusteringManager.Identity()
		clientRouter = clientapi.NewBackendClientRequestRouter(clientRoutingEnabled(cfg.ClusterConfig, identity.ClusterID), identity.ClusterID, localNode, cfg.ClusterConfig.RaftNodeAddrs, cfg.ClusterBackendAuthToken)
	}
	sessionAPI := clientapi.NewSessionService(cfg.SessionManager, cfg.SpaceManager).WithClientRequestRouter(clientRouter)
	if provider, ok := cfg.GraphManager.(clientapi.GraphWriteRouteProvider); ok {
		sessionAPI.WithGraphWriteRouteProvider(provider)
	}
	transactionAPI := clientapi.NewTransactionService(cfg.SessionManager, cfg.GraphManager, cfg.SpaceManager).WithClientRequestRouter(clientRouter)
	graphAPI := clientapi.NewGraphService(cfg.SessionManager, cfg.GraphManager, cfg.BlobManager).WithClientRequestRouter(clientRouter)
	queryAPI := clientapi.NewQueryService(cfg.SessionManager, cfg.GraphManager, cfg.SpaceManager).WithSchemaManager(cfg.SchemaManager).WithSemanticManager(cfg.SemanticManager).WithClientRequestRouter(clientRouter)
	importExportAPI := clientapi.NewImportExportService(cfg.SessionManager, cfg.GraphManager, cfg.BlobManager, cfg.SpaceManager).WithClientRequestRouter(clientRouter)
	metadataCatalogAPI := clientapi.NewMetadataCatalogService(cfg.SessionManager, cfg.GraphManager).WithClientRequestRouter(clientRouter)
	if cfg.ClusteringManager != nil {
		cfg.ClusteringManager.SetBackendClientRequestForwarder(clientapi.ForwardedClientHandler{LocalNode: localNode, Sessions: sessionAPI, Transactions: transactionAPI, Graphs: graphAPI, Queries: queryAPI, Metadata: metadataCatalogAPI})
	}
	clientv1.RegisterSpaceServiceServer(grpcServer, clientapi.NewSpaceService(cfg.SpaceManager))
	clientv1.RegisterDomainServiceServer(grpcServer, clientapi.NewDomainService(cfg.SpaceManager))
	clientv1.RegisterSessionServiceServer(grpcServer, sessionAPI)
	clientv1.RegisterTransactionServiceServer(grpcServer, transactionAPI)
	clientv1.RegisterGraphServiceServer(grpcServer, graphAPI)
	clientv1.RegisterBlobServiceServer(grpcServer, clientapi.NewBlobService(cfg.BlobManager, cfg.SpaceManager))
	clientv1.RegisterQueryServiceServer(grpcServer, queryAPI)
	if cfg.SchemaManager != nil {
		clientv1.RegisterSchemaServiceServer(grpcServer, clientapi.NewSchemaService(cfg.SchemaManager))
	}
	if cfg.AutomationManager != nil {
		clientv1.RegisterAutomationServiceServer(grpcServer, clientapi.NewAutomationService(cfg.AutomationManager))
	}
	clientv1.RegisterImportExportServiceServer(grpcServer, importExportAPI)
	clientv1.RegisterMetadataCatalogServiceServer(grpcServer, metadataCatalogAPI)
	clientv1.RegisterSemanticServiceServer(grpcServer, clientapi.NewSemanticService(cfg.SemanticManager, cfg.SpaceManager, cfg.GraphManager))
	graphChangeAPI := clientapi.NewGraphChangeService(cfg.GraphChangeManager, cfg.SpaceManager)
	if checker, ok := cfg.GraphManager.(clientapi.TransactionGraphWriteLeaderChecker); ok {
		graphChangeAPI.WithGraphWriteLeaderChecker(checker)
	}
	clientv1.RegisterGraphChangeServiceServer(grpcServer, graphChangeAPI)
	return &Server{grpcServer: grpcServer, listener: listener, logger: cfg.Logger}, nil
}

func clientRoutingEnabled(cfg daemonconfig.ClusterConfig, clusterID string) bool {
	return clusterID != "" && cfg.RaftLocalNodeID > 0 && (len(cfg.RaftNodeAddrs) > 0 || cfg.RaftNodeCount == 1)
}

func defaultPublicMethods() map[string]bool {
	return map[string]bool{
		commonv1.AuthService_Login_FullMethodName:                                   true,
		commonv1.AuthService_Refresh_FullMethodName:                                 true,
		clusterpb.ClusterBackendService_RegisterNode_FullMethodName:                 true,
		clusterpb.ClusterBackendService_GetClusterView_FullMethodName:               true,
		clusterpb.ClusterBackendService_UpdateNodeStatus_FullMethodName:             true,
		clusterpb.ClusterBackendService_WatchClusterUpdates_FullMethodName:          true,
		clusterpb.ClusterBackendService_GetBlobPayload_FullMethodName:               true,
		clusterpb.ClusterBackendService_DeliverRaftMessages_FullMethodName:          true,
		clusterpb.ClusterBackendService_GetRaftSpace_FullMethodName:                 true,
		clusterpb.ClusterBackendService_ListRaftSpaces_FullMethodName:               true,
		clusterpb.ClusterBackendService_ExecuteRaftGraphRead_FullMethodName:         true,
		clusterpb.ClusterBackendService_ExecuteRaftSemanticRead_FullMethodName:      true,
		clusterpb.ClusterBackendService_ForwardClientRequest_FullMethodName:         true,
		clusterpb.ClusterBackendService_GetLocalGraphConsistency_FullMethodName:     true,
		clusterpb.ClusterBackendService_CheckLocalBackupReadiness_FullMethodName:    true,
		clusterpb.ClusterBackendService_AcquireLocalBackupQuiesce_FullMethodName:    true,
		clusterpb.ClusterBackendService_ReleaseLocalBackupQuiesce_FullMethodName:    true,
		clusterpb.ClusterBackendService_AcquireLocalRaftBackupFreeze_FullMethodName: true,
		clusterpb.ClusterBackendService_ReleaseLocalRaftBackupFreeze_FullMethodName: true,
		clusterpb.ClusterBackendService_CreateLocalBackupArchive_FullMethodName:     true,
	}
}

func defaultQuiesceExemptMethods() map[string]bool {
	return map[string]bool{
		commonv1.AuthService_Login_FullMethodName:                                   true,
		commonv1.AuthService_Refresh_FullMethodName:                                 true,
		commonv1.AuthService_WhoAmI_FullMethodName:                                  true,
		adminv1.AdminBackupService_GetBackupPolicy_FullMethodName:                   true,
		adminv1.AdminBackupService_TriggerBackup_FullMethodName:                     true,
		adminv1.AdminBackupService_GetBackupStatus_FullMethodName:                   true,
		adminv1.AdminBackupService_ListBackups_FullMethodName:                       true,
		adminv1.AdminBackupService_TriggerClusterBackup_FullMethodName:              true,
		adminv1.AdminBackupService_GetClusterBackupStatus_FullMethodName:            true,
		adminv1.AdminBackupService_ListClusterBackups_FullMethodName:                true,
		adminv1.AdminBackupService_ValidateClusterBackupSet_FullMethodName:          true,
		adminv1.AdminClusterService_GetClusterHealth_FullMethodName:                 true,
		clusterpb.ClusterBackendService_RegisterNode_FullMethodName:                 true,
		clusterpb.ClusterBackendService_GetClusterView_FullMethodName:               true,
		clusterpb.ClusterBackendService_UpdateNodeStatus_FullMethodName:             true,
		clusterpb.ClusterBackendService_WatchClusterUpdates_FullMethodName:          true,
		clusterpb.ClusterBackendService_GetBlobPayload_FullMethodName:               true,
		clusterpb.ClusterBackendService_DeliverRaftMessages_FullMethodName:          true,
		clusterpb.ClusterBackendService_GetRaftSpace_FullMethodName:                 true,
		clusterpb.ClusterBackendService_ListRaftSpaces_FullMethodName:               true,
		clusterpb.ClusterBackendService_ExecuteRaftGraphRead_FullMethodName:         true,
		clusterpb.ClusterBackendService_ExecuteRaftSemanticRead_FullMethodName:      true,
		clusterpb.ClusterBackendService_ForwardClientRequest_FullMethodName:         true,
		clusterpb.ClusterBackendService_GetLocalGraphConsistency_FullMethodName:     true,
		clusterpb.ClusterBackendService_CheckLocalBackupReadiness_FullMethodName:    true,
		clusterpb.ClusterBackendService_AcquireLocalBackupQuiesce_FullMethodName:    true,
		clusterpb.ClusterBackendService_ReleaseLocalBackupQuiesce_FullMethodName:    true,
		clusterpb.ClusterBackendService_AcquireLocalRaftBackupFreeze_FullMethodName: true,
		clusterpb.ClusterBackendService_ReleaseLocalRaftBackupFreeze_FullMethodName: true,
		clusterpb.ClusterBackendService_CreateLocalBackupArchive_FullMethodName:     true,
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
