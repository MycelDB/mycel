package disrupttest

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type TestScope struct {
	RunID    string `json:"runId"`
	SpaceID  string `json:"spaceId"`
	DomainID string `json:"domainId"`
}

type MycelClient struct {
	conn  *grpc.ClientConn
	token string
	login *commonv1.LoginResponse
}

func DialMycel(ctx context.Context, endpoint Endpoint, username, password string) (*MycelClient, error) {
	conn, err := grpc.NewClient(endpoint.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	loginCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	login, err := commonv1.NewAuthServiceClient(conn).Login(loginCtx, &commonv1.LoginRequest{Username: username, Password: password, Client: &commonv1.ClientInfo{Name: "mycel-raft-disrupttest", Platform: "cli"}})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &MycelClient{conn: conn, token: login.GetAccessToken(), login: login}, nil
}

func (c *MycelClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *MycelClient) authContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}

func rpcContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (c *MycelClient) CreateScope(ctx context.Context, runID string) (TestScope, error) {
	name := "raft-disrupt-" + runID
	rpcCtx, cancel := rpcContext(ctx, 30*time.Second)
	defer cancel()
	authCtx := c.authContext(rpcCtx)
	res, err := adminv1.NewAdminSpaceServiceClient(c.conn).CreateSpace(authCtx, &adminv1.CreateSpaceRequest{Name: name, OwnerUsername: c.login.GetPrincipal().GetUsername(), DefaultDomainKey: "default", DefaultDomainName: "Default"})
	if err != nil {
		return TestScope{}, err
	}
	return TestScope{RunID: runID, SpaceID: res.GetSpace().GetSpaceId(), DomainID: res.GetDefaultDomainId()}, nil
}

func (c *MycelClient) ExecuteGQL(ctx context.Context, scope TestScope, gql string, readOnly bool) (*clientv1.QueryResult, error) {
	return c.withTransaction(ctx, scope, readOnly, func(authCtx context.Context, txID string) (*clientv1.QueryResult, error) {
		res, err := clientv1.NewQueryServiceClient(c.conn).ExecuteGQL(authCtx, &clientv1.ExecuteGQLRequest{TransactionId: txID, Query: gql})
		if err != nil {
			return nil, err
		}
		return res.GetResult(), nil
	})
}

func (c *MycelClient) ExecuteGQLScript(ctx context.Context, scope TestScope, script string) error {
	_, err := c.withTransaction(ctx, scope, false, func(authCtx context.Context, txID string) (*clientv1.QueryResult, error) {
		res, err := clientv1.NewQueryServiceClient(c.conn).ExecuteGQLScript(authCtx, &clientv1.ExecuteGQLScriptRequest{TransactionId: txID, Script: script, StopOnError: true})
		if err != nil {
			return nil, err
		}
		for _, stmt := range res.GetStatements() {
			if !stmt.GetSuccess() {
				return nil, fmt.Errorf("gql script statement %d failed: %s", stmt.GetIndex(), stmt.GetError())
			}
		}
		return res.GetResult(), nil
	})
	return err
}

func (c *MycelClient) CountGQL(ctx context.Context, scope TestScope, gql string) (int64, error) {
	res, err := c.ExecuteGQL(ctx, scope, gql, true)
	if err != nil {
		return 0, err
	}
	return CountFromResult(res)
}

func (c *MycelClient) withTransaction(ctx context.Context, scope TestScope, readOnly bool, execute func(authCtx context.Context, txID string) (*clientv1.QueryResult, error)) (*clientv1.QueryResult, error) {
	rpcCtx, cancel := rpcContext(ctx, 20*time.Second)
	defer cancel()
	authCtx := c.authContext(rpcCtx)
	sessionClient := clientv1.NewSessionServiceClient(c.conn)
	txClient := clientv1.NewTransactionServiceClient(c.conn)
	sessionRes, err := sessionClient.OpenSession(authCtx, &clientv1.OpenSessionRequest{SpaceId: scope.SpaceID, DomainId: scope.DomainID})
	if err != nil {
		return nil, err
	}
	sessionID := sessionRes.GetSession().GetSessionId()
	defer func() {
		_, _ = sessionClient.CloseSession(authCtx, &clientv1.CloseSessionRequest{SessionId: sessionID})
	}()
	mode := clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE
	if readOnly {
		mode = clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY
	}
	txRes, err := txClient.BeginTransaction(authCtx, &clientv1.BeginTransactionRequest{SessionId: sessionID, Mode: mode})
	if err != nil {
		return nil, err
	}
	txID := txRes.GetTransaction().GetTransactionId()
	closed := false
	defer func() {
		if !closed {
			_, _ = txClient.RollbackTransaction(authCtx, &clientv1.RollbackTransactionRequest{TransactionId: txID})
		}
	}()
	result, err := execute(authCtx, txID)
	if err != nil {
		return nil, err
	}
	if readOnly {
		_, err = txClient.CloseTransaction(authCtx, &clientv1.CloseTransactionRequest{TransactionId: txID})
	} else {
		_, err = txClient.CommitTransaction(authCtx, &clientv1.CommitTransactionRequest{TransactionId: txID})
	}
	if err != nil {
		return nil, err
	}
	closed = true
	return result, nil
}

func (c *MycelClient) WriteChaos(ctx context.Context, scope TestScope, workerID string, seq int64) error {
	gql := fmt.Sprintf("INSERT (:ChaosWrite {run: %q, seq: %d, worker: %q})", scope.RunID, seq, workerID)
	_, err := c.ExecuteGQL(ctx, scope, gql, false)
	return err
}

func (c *MycelClient) CountChaos(ctx context.Context, scope TestScope) (int64, error) {
	gql := fmt.Sprintf("MATCH (n:ChaosWrite {run: %q}) RETURN count(n) FETCH FIRST 1 ROW ONLY", scope.RunID)
	return c.CountGQL(ctx, scope, gql)
}

func (c *MycelClient) TriggerClusterBackup(ctx context.Context, reason, outputDir, archiveFormat string) (*adminv1.TriggerClusterBackupResponse, error) {
	rpcCtx, cancel := rpcContext(ctx, 10*time.Minute)
	defer cancel()
	format := adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR
	if strings.EqualFold(strings.TrimSpace(archiveFormat), "tar.gz") || strings.EqualFold(strings.TrimSpace(archiveFormat), "tgz") {
		format = adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_GZ
	} else if strings.EqualFold(strings.TrimSpace(archiveFormat), "tar.zst") || strings.EqualFold(strings.TrimSpace(archiveFormat), "tzst") {
		format = adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_TAR_ZST
	} else if strings.EqualFold(strings.TrimSpace(archiveFormat), "zip") {
		format = adminv1.BackupArchiveFormat_BACKUP_ARCHIVE_FORMAT_ZIP
	}
	return adminv1.NewAdminBackupServiceClient(c.conn).TriggerClusterBackup(c.authContext(rpcCtx), &adminv1.TriggerClusterBackupRequest{Reason: reason, OutputDir: outputDir, ArchiveFormat: format})
}

func (c *MycelClient) ValidateClusterBackupSet(ctx context.Context, backupSetPath string) (*adminv1.ValidateClusterBackupSetResponse, error) {
	rpcCtx, cancel := rpcContext(ctx, time.Minute)
	defer cancel()
	return adminv1.NewAdminBackupServiceClient(c.conn).ValidateClusterBackupSet(c.authContext(rpcCtx), &adminv1.ValidateClusterBackupSetRequest{BackupSetPath: backupSetPath})
}

func (c *MycelClient) LocalConsistencyCounts(ctx context.Context, scopes []TestScope) (WorkloadCounts, error) {
	rpcCtx, cancel := rpcContext(ctx, 20*time.Second)
	defer cancel()
	authCtx := c.authContext(rpcCtx)
	client := adminv1.NewAdminClusterServiceClient(c.conn)
	total := WorkloadCounts{Scopes: map[string]WorkloadCounts{}}
	for _, scope := range scopes {
		res, err := client.GetLocalGraphConsistency(authCtx, &adminv1.GetLocalGraphConsistencyRequest{SpaceId: scope.SpaceID, DomainId: scope.DomainID})
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "graph store manifest does not exist") {
				total.Scopes[scope.RunID] = WorkloadCounts{}
				continue
			}
			return WorkloadCounts{}, err
		}
		stats := res.GetStats()
		scopeCounts := WorkloadCounts{Nodes: int64(stats.GetNodeCount()), Edges: int64(stats.GetEdgeCount())}
		total = total.Add(scopeCounts)
		total.Scopes[scope.RunID] = scopeCounts
	}
	return total, nil
}

func CountFromResult(res *clientv1.QueryResult) (int64, error) {
	rows := res.GetRows()
	if len(rows) != 1 {
		return 0, fmt.Errorf("count query returned %d rows", len(rows))
	}
	field := rows[0].GetFields()["count"]
	if field == nil || field.GetScalar() == nil {
		for _, v := range rows[0].GetFields() {
			if v.GetScalar() != nil {
				field = v
				break
			}
		}
	}
	if field == nil || field.GetScalar() == nil {
		return 0, fmt.Errorf("count query did not return a scalar count")
	}
	n := field.GetScalar().GetNumberValue()
	if math.Trunc(n) != n {
		return 0, fmt.Errorf("count query returned non-integer %v", n)
	}
	return int64(n), nil
}

type Diagnostics struct {
	Endpoint      string `json:"endpoint"`
	ClusterID     string `json:"clusterId,omitempty"`
	NodeID        string `json:"nodeId,omitempty"`
	NodeName      string `json:"nodeName,omitempty"`
	ClusterStatus any    `json:"clusterStatus,omitempty"`
	Health        any    `json:"health,omitempty"`
	Runtime       any    `json:"runtime,omitempty"`
	RaftGroups    any    `json:"raftGroups,omitempty"`
	Warning       string `json:"warning,omitempty"`
}

func (c *MycelClient) Diagnostics(ctx context.Context, endpoint string) Diagnostics {
	rpcCtx, cancel := rpcContext(ctx, 10*time.Second)
	defer cancel()
	authCtx := c.authContext(rpcCtx)
	client := adminv1.NewAdminClusterServiceClient(c.conn)
	d := Diagnostics{Endpoint: endpoint}
	if res, err := client.GetClusterStatus(authCtx, &adminv1.GetClusterStatusRequest{}); err == nil {
		d.ClusterStatus = res
		d.ClusterID = res.GetCluster().GetClusterId()
		d.NodeID = res.GetNode().GetNodeId()
		d.NodeName = res.GetNode().GetNodeName()
	} else {
		d.Warning = appendWarning(d.Warning, "cluster status: "+err.Error())
	}
	if res, err := client.GetClusterHealth(authCtx, &adminv1.GetClusterHealthRequest{}); err == nil {
		d.Health = res
	} else {
		d.Warning = appendWarning(d.Warning, "cluster health: "+err.Error())
	}
	if res, err := client.GetClusterRuntimeStatus(authCtx, &adminv1.GetClusterRuntimeStatusRequest{}); err == nil {
		d.Runtime = res
	} else {
		d.Warning = appendWarning(d.Warning, "runtime status: "+err.Error())
	}
	if res, err := client.ListRaftGroups(authCtx, &adminv1.ListRaftGroupsRequest{}); err == nil {
		d.RaftGroups = res
	} else {
		d.Warning = appendWarning(d.Warning, "raft groups: "+err.Error())
	}
	return d
}

func appendWarning(existing, next string) string {
	if existing == "" {
		return next
	}
	return existing + "; " + next
}

func IsTransientError(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	switch code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Canceled:
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "context deadline exceeded") || strings.Contains(msg, "deadline exceeded") {
		return true
	}
	if code == codes.NotFound && strings.Contains(msg, "session, transaction, space, or domain not found") {
		return true
	}
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "transport is closing") ||
		strings.Contains(msg, "client connection is closing") ||
		strings.Contains(msg, "error reading server preface") ||
		strings.Contains(msg, "temporary") ||
		strings.Contains(msg, "not connected") ||
		strings.Contains(msg, "lost connection to pod") ||
		strings.Contains(msg, "portforward") ||
		strings.Contains(msg, "port-forward") ||
		strings.Contains(msg, "has no leader") ||
		strings.Contains(msg, "raft group has no leader") ||
		strings.Contains(msg, "invalid session state")
}
