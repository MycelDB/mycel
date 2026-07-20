package admin

import (
	"context"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"log/slog"
	"testing"
	"time"
)

func TestAdminRaftStateMachineAppliesAdminPut(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if r := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !r.OK {
		t.Fatal(r.Error)
	}
	a := Admin{ID: "admin-1", Username: "alice", State: AdminStateActive, PasswordHash: "hash", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	cmd, err := m.buildAdminPutRaftCommand(a, "admin-put-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatal(err)
	}
	got, err := m.FindOperator(ctx, "alice", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "admin-1" {
		t.Fatalf("ID=%q", got.ID)
	}
}

func TestCreateOperatorUsesSystemRaftWhenEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m := NewModule()
	if r := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !r.OK {
		t.Fatal(r.Error)
	}
	router := consensus.NewLocalMessageRouter()
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 1, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return RaftStateMachine{Module: m} }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	m.EnableExperimentalRaft(groups)
	g, _ := groups.Group(consensus.SystemGroupID)
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return g.Leader() == 1 }); err != nil {
		t.Fatal(err)
	}
	created, err := m.CreateOperator(ctx, CreateOperatorInput{Username: "bob", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.AuthenticateOperator(ctx, "bob", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != created.ID {
		t.Fatalf("ID=%q want %q", got.ID, created.ID)
	}
}

func TestAdminSystemRaftReplicatesAcrossThreeNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	modules := map[consensus.NodeID]*Module{}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := NewModule()
		if r := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !r.OK {
			t.Fatal(r.Error)
		}
		modules[nodeID] = m
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: 1, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return RaftStateMachine{Module: m} }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatal(err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg)
		for _, g := range mg.Groups() {
			for _, r := range routers {
				r.Register(g)
			}
		}
	}
	defer func() {
		for _, mg := range groupsByNode {
			mg.Stop()
		}
	}()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for _, mg := range groupsByNode {
					mg.Tick()
				}
			}
		}
	}()
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, func() {
		for _, mg := range groupsByNode {
			mg.Tick()
		}
	}, func() bool {
		counts := map[consensus.NodeID]int{}
		for _, mg := range groupsByNode {
			if g, ok := mg.Group(consensus.SystemGroupID); ok && g.Leader() != 0 {
				counts[g.Leader()]++
			}
		}
		for _, c := range counts {
			if c >= 2 {
				return true
			}
		}
		return false
	}); err != nil {
		t.Fatal(err)
	}
	created, err := modules[1].CreateOperator(ctx, CreateOperatorInput{Username: "zoe", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := modules[1].SetOperatorPassword(ctx, created.ID, "new-secret"); err != nil {
		t.Fatal(err)
	}
	for id, m := range modules {
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			got, err := m.AuthenticateOperator(ctx, "zoe", "new-secret")
			return err == nil && got.ID == created.ID
		}); err != nil {
			t.Fatalf("node %d missing operator: %v", id, err)
		}
	}
}

func TestAdminSystemRaftSurvivesLeaderFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	modules := map[consensus.NodeID]*Module{}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := NewModule()
		if r := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !r.OK {
			t.Fatal(r.Error)
		}
		modules[nodeID] = m
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: 1, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return RaftStateMachine{Module: m} }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatal(err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg)
		for _, g := range mg.Groups() {
			for _, r := range routers {
				r.Register(g)
			}
		}
	}
	defer func() {
		for _, mg := range groupsByNode {
			mg.Stop()
		}
	}()
	active := map[consensus.NodeID]bool{1: true, 2: true, 3: true}
	tick := func() {
		for id, mg := range groupsByNode {
			if active[id] {
				mg.Tick()
			}
		}
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				tick()
			}
		}
	}()
	leader := func() consensus.NodeID {
		counts := map[consensus.NodeID]int{}
		for id, mg := range groupsByNode {
			if !active[id] {
				continue
			}
			if g, ok := mg.Group(consensus.SystemGroupID); ok && g.Leader() != 0 {
				counts[g.Leader()]++
			}
		}
		for l, c := range counts {
			if active[l] && c >= 2 {
				return l
			}
		}
		return 0
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, tick, func() bool { return leader() != 0 }); err != nil {
		t.Fatal(err)
	}
	old := leader()
	active[old] = false
	groupsByNode[old].Stop()
	for _, r := range routers {
		r.UnregisterNode(old)
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, tick, func() bool { l := leader(); return l != 0 && l != old }); err != nil {
		t.Fatal(err)
	}
	writer := consensus.NodeID(1)
	if writer == old {
		writer = 2
	}
	created, err := modules[writer].CreateOperator(ctx, CreateOperatorInput{Username: "yuki", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	for id, m := range modules {
		if !active[id] {
			continue
		}
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			got, err := m.AuthenticateOperator(ctx, "yuki", "secret")
			return err == nil && got.ID == created.ID
		}); err != nil {
			t.Fatalf("node %d missing operator: %v", id, err)
		}
	}
}

func TestAdminSessionSystemRaftReplicatesAcrossThreeNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	modules := map[consensus.NodeID]*Module{}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := NewModule()
		if r := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !r.OK {
			t.Fatal(r.Error)
		}
		modules[nodeID] = m
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: 1, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return RaftStateMachine{Module: m} }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatal(err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg)
		for _, g := range mg.Groups() {
			for _, r := range routers {
				r.Register(g)
			}
		}
	}
	defer func() {
		for _, mg := range groupsByNode {
			mg.Stop()
		}
	}()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for _, mg := range groupsByNode {
					mg.Tick()
				}
			}
		}
	}()
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, func() {
		for _, mg := range groupsByNode {
			mg.Tick()
		}
	}, func() bool {
		counts := map[consensus.NodeID]int{}
		for _, mg := range groupsByNode {
			if g, ok := mg.Group(consensus.SystemGroupID); ok && g.Leader() != 0 {
				counts[g.Leader()]++
			}
		}
		for _, c := range counts {
			if c >= 2 {
				return true
			}
		}
		return false
	}); err != nil {
		t.Fatal(err)
	}
	admin, err := modules[1].CreateOperator(ctx, CreateOperatorInput{Username: "session-admin", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	_, session, err := modules[1].CreateOperatorAuthSession(ctx, admin, domainauth.RefreshSessionMetadata{ClientName: "test"}, 32, time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for id, m := range modules {
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			sessions, err := m.ListOperatorSessions(ctx, admin.ID)
			return err == nil && len(sessions) == 1 && sessions[0].ID == session.ID
		}); err != nil {
			t.Fatalf("node %d missing session: %v", id, err)
		}
	}
	if err := modules[2].RevokeOperatorSession(ctx, admin.ID, session.ID.String()); err != nil {
		t.Fatal(err)
	}
	for id, m := range modules {
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			sessions, err := m.ListOperatorSessions(ctx, admin.ID)
			return err == nil && len(sessions) == 1 && sessions[0].Status == domainauth.RefreshSessionStatusRevoked
		}); err != nil {
			t.Fatalf("node %d missing revoked session: %v", id, err)
		}
	}
}
