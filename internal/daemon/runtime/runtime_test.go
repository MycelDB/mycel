package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/daemon/config"
	coreruntime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
)

type testModule struct {
	name string
	init func(context.Context, coreruntime.Host) InitResult
}

type lifecycleService struct {
	testModule
	start func(context.Context) error
	stop  func(context.Context) error
}

type initParticipantService struct {
	testModule
	participant quiesce.Participant
}

type statusService struct {
	testModule
	status ServiceStatus
	health HealthStatus
}

func (m testModule) Name() string { return m.name }
func (m testModule) Init(ctx context.Context, host coreruntime.Host) InitResult {
	if m.init != nil {
		return m.init(ctx, host)
	}
	return OK(m.name)
}

func (s lifecycleService) Start(ctx context.Context) error {
	if s.start != nil {
		return s.start(ctx)
	}
	return nil
}

func (s lifecycleService) Stop(ctx context.Context) error {
	if s.stop != nil {
		return s.stop(ctx)
	}
	return nil
}

func (s initParticipantService) Init(ctx context.Context, host coreruntime.Host) InitResult {
	registrar, ok := host.(coreruntime.QuiesceRegistrar)
	if !ok {
		return Abort(s.name, "quiesce", "host does not support quiesce registration", nil)
	}
	if err := registrar.RegisterQuiesceParticipant(s.participant); err != nil {
		return Abort(s.name, "quiesce", "register quiesce participant", err)
	}
	return s.testModule.Init(ctx, host)
}

func (s statusService) Status(context.Context) ServiceStatus { return s.status }
func (s statusService) Health(context.Context) HealthStatus  { return s.health }

type runtimeTestParticipant struct{ name string }

func (p runtimeTestParticipant) Name() string { return p.name }
func (p runtimeTestParticipant) Quiesce(context.Context, quiesce.Request) (quiesce.Lease, error) {
	return quiesce.LeaseFunc(func(context.Context) error { return nil }), nil
}
func (p runtimeTestParticipant) Status() quiesce.ParticipantStatus {
	return quiesce.ParticipantStatus{Name: p.name}
}

var _ Service = testModule{}
var _ Starter = lifecycleService{}
var _ coreruntime.Host = (*Runtime)(nil)
var _ coreruntime.QuiesceRegistrar = (*Runtime)(nil)
var _ coreruntime.LocalRouteIdentityProvider = (*Runtime)(nil)
var _ coreruntime.WALProvider = (*Runtime)(nil)
var _ Stopper = lifecycleService{}

func TestRuntimeLocalRouteIdentity(t *testing.T) {
	rt := New(config.Config{Cluster: config.ClusterConfig{RaftLocalNodeID: 1, BackendAdvertiseAddr: "127.0.0.1:9093", RaftNodeAddrs: []string{"a", "b"}}}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	rt.NodeIdentity = &model.NodeIdentity{NodeID: "node_2", NodeName: "node-b", ClusterID: "cluster-1", BackendAdvertiseAddr: "127.0.0.1:19093"}
	got := rt.LocalRouteIdentity()
	if got.NodeID != "node_2" || got.NodeName != "node-b" || got.ClusterID != "cluster-1" || !got.RaftMode || got.RaftNodeID != 1 || got.BackendAdvertiseAddr != "127.0.0.1:19093" {
		t.Fatalf("LocalRouteIdentity() = %#v", got)
	}
	got.RaftNodeAddrs[0] = "mutated"
	if rt.Config.Cluster.RaftNodeAddrs[0] == "mutated" {
		t.Fatal("LocalRouteIdentity leaked raft node address slice")
	}
}

func TestRuntimeLocalRouteIdentityStandaloneDoesNotAdvertiseRaftNode(t *testing.T) {
	rt := New(config.Config{Cluster: config.ClusterConfig{RaftLocalNodeID: 1, RaftNodeCount: 3}}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	got := rt.LocalRouteIdentity()
	if got.RaftMode || got.RaftNodeID != 0 {
		t.Fatalf("LocalRouteIdentity()=%#v, want non-raft identity", got)
	}
}

func TestRuntimeInitServicesRegistersByName(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "/tmp/myceld.log", nil)
	service := testModule{name: "admin"}
	if err := rt.InitServices(context.Background(), []Service{service}); err != nil {
		t.Fatalf("InitServices() error = %v", err)
	}
	got, ok := rt.Service("admin")
	if !ok {
		t.Fatal("expected admin service to be registered")
	}
	if got.Name() != "admin" {
		t.Fatalf("unexpected service: %s", got.Name())
	}
	typed, ok := ServiceAs[testModule](rt, "admin")
	if !ok || typed.Name() != "admin" {
		t.Fatalf("expected typed admin service, got ok=%v service=%#v", ok, typed)
	}
}

func TestRuntimeInitServicesRejectsDuplicateNames(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	err := rt.InitServices(context.Background(), []Service{testModule{name: "admin"}, testModule{name: "admin"}})
	if err == nil {
		t.Fatal("expected duplicate service name error")
	}
}

func TestRuntimeHasQuiesceCoordinator(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	if rt.Quiesce == nil {
		t.Fatal("expected runtime quiesce coordinator")
	}
}

func TestServiceCanRegisterQuiesceParticipantDuringInit(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	service := initParticipantService{testModule: testModule{name: "graph"}, participant: runtimeTestParticipant{name: "graph"}}
	if err := rt.InitServices(context.Background(), []Service{service}); err != nil {
		t.Fatalf("InitServices() error = %v", err)
	}
	participants := rt.Quiesce.Participants()
	if len(participants) != 1 {
		t.Fatalf("len(participants) = %d, want 1", len(participants))
	}
	if participants[0].Name() != "graph" {
		t.Fatalf("participant name = %q, want graph", participants[0].Name())
	}
}

func TestRuntimeServiceRegistryPreservesOrder(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	services := []Service{testModule{name: "graph"}, testModule{name: "blob"}}
	if err := rt.InitServices(context.Background(), services); err != nil {
		t.Fatalf("InitServices() error = %v", err)
	}

	ordered := rt.Services()
	if got, want := len(ordered), 2; got != want {
		t.Fatalf("len(Services()) = %d, want %d", got, want)
	}
	if ordered[0].Name() != "graph" || ordered[1].Name() != "blob" {
		t.Fatalf("unexpected service order: %s, %s", ordered[0].Name(), ordered[1].Name())
	}

	service, ok := rt.Service("graph")
	if !ok || service.Name() != "graph" {
		t.Fatalf("expected graph service, got ok=%v service=%#v", ok, service)
	}
	ordered[0] = testModule{name: "mutated"}
	if rt.Services()[0].Name() != "graph" {
		t.Fatal("Services() returned slice should not mutate runtime order")
	}

	typed, ok := ServiceAs[testModule](rt, "blob")
	if !ok || typed.Name() != "blob" {
		t.Fatalf("expected typed blob service, got ok=%v service=%#v", ok, typed)
	}
}

func TestRuntimeRegisterServiceRejectsInvalidServices(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	if err := rt.RegisterService(nil); err == nil {
		t.Fatal("expected nil service error")
	}
	if err := rt.RegisterService(testModule{name: "   "}); err == nil {
		t.Fatal("expected empty service name error")
	}
	if err := rt.RegisterService(testModule{name: "graph"}); err != nil {
		t.Fatalf("RegisterService() error = %v", err)
	}
	if err := rt.RegisterService(testModule{name: "graph"}); err == nil {
		t.Fatal("expected duplicate service name error")
	}
}

func TestRuntimeCollectsServiceStatuses(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	services := []Service{
		testModule{name: "passive"},
		statusService{testModule: testModule{name: "graph"}, status: ServiceStatus{Name: "graph", State: "running", Started: true}, health: HealthStatus{Name: "graph", Healthy: true}},
		statusService{testModule: testModule{name: "semantic"}, status: ServiceStatus{Name: "semantic", State: "disabled"}, health: HealthStatus{Name: "semantic", Healthy: true}},
	}
	if err := rt.InitServices(context.Background(), services); err != nil {
		t.Fatalf("InitServices() error = %v", err)
	}
	statuses := rt.ServiceStatuses(context.Background())
	if len(statuses) != 2 {
		t.Fatalf("len(ServiceStatuses()) = %d, want 2", len(statuses))
	}
	if statuses[0].Name != "graph" || statuses[0].State != "running" || !statuses[0].Started {
		t.Fatalf("unexpected first status: %#v", statuses[0])
	}
	if statuses[1].Name != "semantic" || statuses[1].State != "disabled" {
		t.Fatalf("unexpected second status: %#v", statuses[1])
	}

	health := rt.HealthStatuses(context.Background())
	if len(health) != 2 || health[0].Name != "graph" || !health[0].Healthy || health[1].Name != "semantic" || !health[1].Healthy {
		t.Fatalf("unexpected health statuses: %#v", health)
	}
}

func TestRuntimeStartStopServicesOrdering(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	var events []string
	services := []Service{
		lifecycleService{testModule: testModule{name: "graph"}, start: func(context.Context) error {
			events = append(events, "start graph")
			return nil
		}, stop: func(context.Context) error {
			events = append(events, "stop graph")
			return nil
		}},
		testModule{name: "passive"},
		lifecycleService{testModule: testModule{name: "semantic"}, start: func(context.Context) error {
			events = append(events, "start semantic")
			return nil
		}, stop: func(context.Context) error {
			events = append(events, "stop semantic")
			return nil
		}},
	}
	if err := rt.InitServices(context.Background(), services); err != nil {
		t.Fatalf("InitServices() error = %v", err)
	}
	if err := rt.StartServices(context.Background()); err != nil {
		t.Fatalf("StartServices() error = %v", err)
	}
	if err := rt.StopServices(context.Background()); err != nil {
		t.Fatalf("StopServices() error = %v", err)
	}

	want := []string{"start graph", "start semantic", "stop semantic", "stop graph"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestRuntimeStartServicesFailureStopsAlreadyStarted(t *testing.T) {
	startErr := errors.New("start failed")
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	var events []string
	services := []Service{
		lifecycleService{testModule: testModule{name: "graph"}, start: func(context.Context) error {
			events = append(events, "start graph")
			return nil
		}, stop: func(context.Context) error {
			events = append(events, "stop graph")
			return nil
		}},
		lifecycleService{testModule: testModule{name: "semantic"}, start: func(context.Context) error {
			events = append(events, "start semantic")
			return startErr
		}, stop: func(context.Context) error {
			events = append(events, "stop semantic")
			return nil
		}},
	}
	if err := rt.InitServices(context.Background(), services); err != nil {
		t.Fatalf("InitServices() error = %v", err)
	}
	err := rt.StartServices(context.Background())
	if !errors.Is(err, startErr) {
		t.Fatalf("StartServices() error = %v, want %v", err, startErr)
	}
	want := []string{"start graph", "start semantic", "stop graph"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestRuntimeStopServicesReturnsStopErrors(t *testing.T) {
	stopErr := errors.New("stop failed")
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	service := lifecycleService{testModule: testModule{name: "graph"}, stop: func(context.Context) error {
		return stopErr
	}}
	if err := rt.InitServices(context.Background(), []Service{service}); err != nil {
		t.Fatalf("InitServices() error = %v", err)
	}
	if err := rt.StartServices(context.Background()); err != nil {
		t.Fatalf("StartServices() error = %v", err)
	}
	if err := rt.StopServices(context.Background()); !errors.Is(err, stopErr) {
		t.Fatalf("StopServices() error = %v, want %v", err, stopErr)
	}
	if err := rt.StopServices(context.Background()); err != nil {
		t.Fatalf("second StopServices() error = %v", err)
	}
}

func TestServiceStatusZeroValueIsNonSensitive(t *testing.T) {
	var status ServiceStatus
	if status.Name != "" || status.State != "" || status.Started || !status.StartedAt.IsZero() || status.LastError != "" {
		t.Fatalf("unexpected zero-value service status: %#v", status)
	}

	var health HealthStatus
	if health.Name != "" || health.Healthy || health.Reason != "" {
		t.Fatalf("unexpected zero-value health status: %#v", health)
	}
}

func TestRuntimeCloseStopsStartedServices(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	stopped := false
	service := lifecycleService{testModule: testModule{name: "graph"}, stop: func(context.Context) error {
		stopped = true
		return nil
	}}
	if err := rt.InitServices(context.Background(), []Service{service}); err != nil {
		t.Fatalf("InitServices() error = %v", err)
	}
	if err := rt.StartServices(context.Background()); err != nil {
		t.Fatalf("StartServices() error = %v", err)
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !stopped {
		t.Fatal("expected Close to stop started services")
	}
}

func TestRuntimeCloseUsesCloseFunc(t *testing.T) {
	closed := false
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", func() error {
		closed = true
		return nil
	})
	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !closed {
		t.Fatal("expected close func to run")
	}
}
