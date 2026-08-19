package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	daemonblob "github.com/myceldb/mycel/internal/blob/service"
	"github.com/myceldb/mycel/internal/clustering"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	"github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"github.com/myceldb/mycel/internal/identity/model"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakePrincipalManager struct{}

func (fakePrincipalManager) ListPrincipals(context.Context) ([]principalservice.PrincipalSummary, error) {
	createdAt := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	return []principalservice.PrincipalSummary{{ID: "admin-1", Username: "admin", Kind: principalservice.PrincipalKindHuman, State: principalservice.PrincipalStateActive, LoginEnabled: true, CreatedAt: createdAt, UpdatedAt: createdAt}}, nil
}
func (fakePrincipalManager) GetPrincipal(context.Context, string) (principalservice.PrincipalSummary, error) {
	return principalservice.PrincipalSummary{}, principalservice.ErrPrincipalNotFound
}
func (fakePrincipalManager) FindPrincipal(context.Context, string, string) (principalservice.PrincipalSummary, error) {
	return principalservice.PrincipalSummary{}, principalservice.ErrPrincipalNotFound
}
func (fakePrincipalManager) CreatePrincipal(context.Context, principalservice.CreatePrincipalInput) (principalservice.PrincipalSummary, error) {
	return principalservice.PrincipalSummary{}, nil
}
func (fakePrincipalManager) UpdatePrincipal(context.Context, principalservice.UpdatePrincipalInput) (principalservice.PrincipalSummary, error) {
	return principalservice.PrincipalSummary{}, nil
}
func (fakePrincipalManager) DisablePrincipal(context.Context, string) (principalservice.PrincipalSummary, error) {
	return principalservice.PrincipalSummary{}, nil
}
func (fakePrincipalManager) EnablePrincipal(context.Context, string) (principalservice.PrincipalSummary, error) {
	return principalservice.PrincipalSummary{}, nil
}
func (fakePrincipalManager) DeletePrincipal(context.Context, string) (principalservice.PrincipalSummary, error) {
	return principalservice.PrincipalSummary{}, nil
}
func (fakePrincipalManager) SetPrincipalPassword(context.Context, string, string) (principalservice.PrincipalSummary, error) {
	return principalservice.PrincipalSummary{}, nil
}
func (fakePrincipalManager) AuthenticatePrincipal(context.Context, string, string) (principalservice.PrincipalSummary, error) {
	createdAt := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	return principalservice.PrincipalSummary{ID: "00000000-0000-0000-0000-000000000001", Username: "alice", Kind: principalservice.PrincipalKindHuman, State: principalservice.PrincipalStateActive, LoginEnabled: true, CreatedAt: createdAt, UpdatedAt: createdAt}, nil
}
func (fakePrincipalManager) CreateAuthSession(context.Context, principalservice.PrincipalSummary, domainauth.RefreshSessionMetadata, int, time.Duration, time.Duration) (domainauth.RefreshToken, domainauth.RefreshSession, error) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	return "refresh", domainauth.RefreshSession{ID: id, PrincipalID: identity.PrincipalID("00000000-0000-0000-0000-000000000001"), Status: domainauth.RefreshSessionStatusActive, CreatedAt: time.Now(), LastUsedAt: time.Now(), AbsoluteExpiresAt: time.Now().Add(time.Hour)}, nil
}
func (fakePrincipalManager) RefreshAuthSession(context.Context, domainauth.RefreshToken, domainauth.RefreshSessionMetadata, int, time.Duration) (principalservice.PrincipalSummary, domainauth.RefreshToken, domainauth.RefreshSession, error) {
	return principalservice.PrincipalSummary{}, "refresh", domainauth.RefreshSession{}, nil
}
func (fakePrincipalManager) ListPrincipalSessions(context.Context, string) ([]domainauth.RefreshSession, error) {
	return nil, nil
}
func (fakePrincipalManager) RevokePrincipalSession(context.Context, string, string) error { return nil }
func (fakePrincipalManager) RevokePrincipalSessions(context.Context, string) (int, error) {
	return 0, nil
}
func (fakePrincipalManager) ListRoleBindings(context.Context, string) ([]principalservice.RoleBinding, error) {
	return nil, nil
}
func (fakePrincipalManager) GrantRole(context.Context, string, string, principalservice.AccessScope, string, string) (principalservice.RoleBinding, principalservice.PrincipalSummary, error) {
	return principalservice.RoleBinding{}, principalservice.PrincipalSummary{}, nil
}
func (fakePrincipalManager) RevokeRole(context.Context, string, string, string) (principalservice.PrincipalSummary, error) {
	return principalservice.PrincipalSummary{}, nil
}
func (fakePrincipalManager) ListCapabilityGrants(context.Context, string) ([]principalservice.CapabilityGrant, error) {
	return nil, nil
}
func (fakePrincipalManager) GrantCapability(context.Context, string, string, principalservice.AccessScope, string, string) (principalservice.CapabilityGrant, principalservice.PrincipalSummary, error) {
	return principalservice.CapabilityGrant{}, principalservice.PrincipalSummary{}, nil
}
func (fakePrincipalManager) RevokeCapability(context.Context, string, string, string) (principalservice.PrincipalSummary, error) {
	return principalservice.PrincipalSummary{}, nil
}
func (fakePrincipalManager) Authorize(context.Context, string, string, principalservice.AccessScope) error {
	return nil
}
func (fakePrincipalManager) HasCapability(context.Context, string, string) (bool, error) {
	return true, nil
}
func (fakePrincipalManager) EffectiveAccess(context.Context, string, principalservice.AccessScope) (principalservice.EffectiveAccess, error) {
	return principalservice.EffectiveAccess{}, nil
}

type fakeSpaceManager struct{}

func (fakeSpaceManager) ListVisibleSpaces(context.Context, string, bool) ([]domainspace.Space, error) {
	return []domainspace.Space{{SpaceID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), OwnerID: identity.PrincipalID("00000000-0000-0000-0000-000000000001"), Name: "Personal", Status: "active", CreatedAt: time.Now(), UpdatedAt: time.Now()}}, nil
}
func (fakeSpaceManager) GetVisibleSpace(context.Context, string, string) (domainspace.Space, error) {
	return domainspace.Space{SpaceID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), OwnerID: identity.PrincipalID("00000000-0000-0000-0000-000000000001"), Name: "Personal", Status: "active", CreatedAt: time.Now(), UpdatedAt: time.Now()}, nil
}
func (fakeSpaceManager) ListSpaces(context.Context, bool) ([]domainspace.Space, error) {
	return nil, nil
}
func (fakeSpaceManager) GetSpace(context.Context, string) (domainspace.Space, error) {
	return domainspace.Space{}, daemonspace.ErrSpaceNotFound
}
func (fakeSpaceManager) CreateSpace(context.Context, daemonspace.CreateSpaceInput) (domainspace.Space, graph.Domain, error) {
	return domainspace.Space{}, graph.Domain{}, nil
}
func (fakeSpaceManager) DeleteSpace(context.Context, string) error { return nil }
func (fakeSpaceManager) GrantSpacePrincipal(context.Context, string, string, string) (daemonspace.SpaceGrant, error) {
	return daemonspace.SpaceGrant{}, nil
}
func (fakeSpaceManager) EffectiveAccess(context.Context, string, domainspace.Space) (daemonspace.EffectiveAccess, error) {
	return daemonspace.EffectiveAccess{Roles: []string{"owner"}, Capabilities: []string{"CAPABILITY_SPACE_READ"}}, nil
}
func (fakeSpaceManager) DomainEffectiveAccess(context.Context, string, string) (daemonspace.EffectiveAccess, error) {
	return daemonspace.EffectiveAccess{Roles: []string{"owner"}, Capabilities: []string{"CAPABILITY_DOMAIN_READ"}}, nil
}
func (fakeSpaceManager) ListDomains(context.Context, string, bool) ([]graph.Domain, error) {
	return []graph.Domain{{ID: uuid.MustParse("00000000-0000-0000-0000-000000000004"), SpaceID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Key: "default", Name: "Default", Default: true, CreatedAt: time.Now(), UpdatedAt: time.Now()}}, nil
}
func (fakeSpaceManager) GetDomain(context.Context, string) (graph.Domain, error) {
	return graph.Domain{ID: uuid.MustParse("00000000-0000-0000-0000-000000000004"), SpaceID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Key: "default", Name: "Default", Default: true, CreatedAt: time.Now(), UpdatedAt: time.Now()}, nil
}
func (fakeSpaceManager) GetDomainByRef(context.Context, string, string) (graph.Domain, error) {
	return graph.Domain{ID: uuid.MustParse("00000000-0000-0000-0000-000000000004"), SpaceID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Key: "default", Name: "Default", Default: true, CreatedAt: time.Now(), UpdatedAt: time.Now()}, nil
}
func (fakeSpaceManager) ListVisibleDomains(context.Context, string, string, bool) ([]graph.Domain, error) {
	return []graph.Domain{{ID: uuid.MustParse("00000000-0000-0000-0000-000000000004"), SpaceID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Key: "default", Name: "Default", Default: true, CreatedAt: time.Now(), UpdatedAt: time.Now()}}, nil
}
func (fakeSpaceManager) GetVisibleDomain(context.Context, string, string, string, string) (graph.Domain, error) {
	return graph.Domain{ID: uuid.MustParse("00000000-0000-0000-0000-000000000004"), SpaceID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Key: "default", Name: "Default", Default: true, CreatedAt: time.Now(), UpdatedAt: time.Now()}, nil
}
func (fakeSpaceManager) CreateDomain(context.Context, string, daemonspace.CreateDomainInput) (graph.Domain, error) {
	return graph.Domain{}, nil
}
func (fakeSpaceManager) UpdateDomain(context.Context, string, daemonspace.UpdateDomainInput) (graph.Domain, error) {
	return graph.Domain{}, nil
}
func (fakeSpaceManager) DeleteDomain(context.Context, string, string, string) error { return nil }
func TestServerRegistersProtectedAdminPrincipalService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tokens := daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	graphModule := daegraph.NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}
	graphModule.Init(ctx, rt)
	blobModule := daemonblob.NewModule(graphModule)
	blobModule.Init(ctx, rt)
	semanticModule := daemonsemantic.NewModule()
	semanticModule.Init(ctx, rt)
	graphNotificationModule := graphnotification.NewModule()
	graphNotificationModule.Init(ctx, rt)
	srv, errCh, err := Start(ctx, Config{Addr: "127.0.0.1:0", PrincipalManager: fakePrincipalManager{}, SpaceManager: fakeSpaceManager{}, SessionManager: daemonsession.NewModule(), GraphManager: graphModule, GraphChangeManager: graphNotificationModule, BlobManager: blobModule, SemanticManager: semanticModule, TokenManager: tokens})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer srv.Stop()

	conn, err := grpc.DialContext(ctx, srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial grpc server: %v", err)
	}
	defer conn.Close()
	principalClient := adminv1.NewAdminPrincipalServiceClient(conn)
	if _, err := principalClient.ListPrincipals(ctx, &adminv1.ListPrincipalsRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated list to fail, got %v", err)
	}
	authClient := commonv1.NewAuthServiceClient(conn)
	login, err := authClient.Login(ctx, &commonv1.LoginRequest{Username: "admin", Password: "pass"})
	if err != nil {
		t.Fatalf("AuthService Login() error = %v", err)
	}
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+login.GetAccessToken())
	clientAuth := commonv1.NewAuthServiceClient(conn)
	clientLogin, err := clientAuth.Login(ctx, &commonv1.LoginRequest{Username: "alice", Password: "pass"})
	if err != nil {
		t.Fatalf("client AuthService Login() error = %v", err)
	}
	if clientLogin.GetAccessToken() == "" || clientLogin.GetPrincipal().GetUsername() != "alice" {
		t.Fatalf("unexpected client login response: %#v", clientLogin)
	}
	if _, err := clientAuth.WhoAmI(ctx, &commonv1.WhoAmIRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated client WhoAmI to fail, got %v", err)
	}
	clientAuthCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+clientLogin.GetAccessToken())
	spaceRes, err := clientv1.NewSpaceServiceClient(conn).ListSpaces(clientAuthCtx, &clientv1.ListSpacesRequest{})
	if err != nil || len(spaceRes.GetSpaces()) != 1 || spaceRes.GetSpaces()[0].GetName() != "Personal" {
		t.Fatalf("client SpaceService ListSpaces() = %#v, %v", spaceRes, err)
	}

	res, err := principalClient.ListPrincipals(authCtx, &adminv1.ListPrincipalsRequest{})
	if err != nil {
		t.Fatalf("ListPrincipals() error = %v", err)
	}
	if len(res.GetPrincipals()) != 1 || res.GetPrincipals()[0].GetUsername() != "admin" {
		t.Fatalf("unexpected principals response: %#v", res)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}

func TestServerRegistersProtectedAdminClusterService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testServerConfig(t, ctx)
	mgr, err := clustering.NewManager(ctx, clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093"}, slog.Default())
	if err != nil {
		t.Fatalf("new cluster manager: %v", err)
	}
	cfg.ClusteringManager = mgr
	cfg.ClusteringServer = mgr.BackendService()
	srv, errCh, err := Start(ctx, cfg)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer srv.Stop()

	conn, err := grpc.DialContext(ctx, srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial grpc server: %v", err)
	}
	defer conn.Close()
	client := adminv1.NewAdminClusterServiceClient(conn)
	if _, err := client.GetClusterStatus(ctx, &adminv1.GetClusterStatusRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated cluster status to fail, got %v", err)
	}
	authClient := commonv1.NewAuthServiceClient(conn)
	login, err := authClient.Login(ctx, &commonv1.LoginRequest{Username: "admin", Password: "pass"})
	if err != nil {
		t.Fatalf("AuthService Login() error = %v", err)
	}
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+login.GetAccessToken())
	res, err := client.GetClusterStatus(authCtx, &adminv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatalf("GetClusterStatus() error = %v", err)
	}
	if res.GetNode().GetNodeName() != "node-a" || res.GetCluster().GetClusterName() != "dev" {
		t.Fatalf("unexpected cluster status: %#v", res)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}

func TestServerTLSAndMTLS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	caCert, caKey, caPath := writeTestCA(t, dir)
	serverCert, serverKey := writeTestCert(t, dir, "server", caCert, caKey, true)
	clientCert, clientKey := writeTestCert(t, dir, "client", caCert, caKey, false)
	tlsCfg, err := LoadTLSConfig(serverCert, serverKey, caPath, true)
	if err != nil {
		t.Fatalf("LoadTLSConfig() error = %v", err)
	}
	cfg := testServerConfig(t, ctx)
	cfg.TLSConfig = tlsCfg
	srv, errCh, err := Start(ctx, cfg)
	if err != nil {
		t.Fatalf("Start() TLS error = %v", err)
	}
	defer srv.Stop()
	clientPair, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		t.Fatalf("load client pair: %v", err)
	}
	roots := x509.NewCertPool()
	rawCA, _ := os.ReadFile(caPath)
	if !roots.AppendCertsFromPEM(rawCA) {
		t.Fatal("append test CA")
	}
	conn, err := grpc.DialContext(ctx, srv.Addr(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "localhost", Certificates: []tls.Certificate{clientPair}})), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial TLS server with client cert: %v", err)
	}
	defer conn.Close()
	if _, err := commonv1.NewAuthServiceClient(conn).Login(ctx, &commonv1.LoginRequest{Username: "admin", Password: "pass"}); err != nil {
		t.Fatalf("AuthService Login over mTLS failed: %v", err)
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for TLS server shutdown")
	}
}

func testServerConfig(t *testing.T, ctx context.Context) Config {
	t.Helper()
	tokens := daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	graphModule := daegraph.NewModule()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}
	graphModule.Init(ctx, rt)
	blobModule := daemonblob.NewModule(graphModule)
	blobModule.Init(ctx, rt)
	semanticModule := daemonsemantic.NewModule()
	semanticModule.Init(ctx, rt)
	graphNotificationModule := graphnotification.NewModule()
	graphNotificationModule.Init(ctx, rt)
	return Config{Addr: "127.0.0.1:0", PrincipalManager: fakePrincipalManager{}, SpaceManager: fakeSpaceManager{}, SessionManager: daemonsession.NewModule(), GraphManager: graphModule, GraphChangeManager: graphNotificationModule, BlobManager: blobModule, SemanticManager: semanticModule, TokenManager: tokens}
}

func writeTestCA(t *testing.T, dir string) (*x509.Certificate, *rsa.PrivateKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "ca.pem")
	writePEM(t, path, "CERTIFICATE", der)
	return cert, key, path
}

func writeTestCert(t *testing.T, dir string, name string, ca *x509.Certificate, caKey *rsa.PrivateKey, server bool) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	usage := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	dns := []string(nil)
	ips := []net.IP(nil)
	if server {
		usage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		dns = []string{"localhost"}
		ips = []net.IP{net.ParseIP("127.0.0.1")}
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: usage, DNSNames: dns, IPAddresses: ips, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(dir, name+".pem")
	keyPath := filepath.Join(dir, name+"-key.pem")
	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
	return certPath, keyPath
}

func writePEM(t *testing.T, path string, typ string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
