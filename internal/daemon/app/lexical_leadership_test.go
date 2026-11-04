package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestAsyncLexicalLeadershipSchedulesOnlyNewLocalTerms(t *testing.T) {
	ctx := context.Background()
	notifications := newTestGraphNotifications(t)
	partitionCount := uint32(2)
	scopes := make([]graphchange.Scope, partitionCount)
	for partitionID := range partitionCount {
		var spaceID domainspace.SpaceID
		for {
			spaceID = domainspace.SpaceID(uuid.New())
			got, err := partitioning.PartitionForSpaceID(spaceID, partitionCount)
			if err != nil {
				t.Fatal(err)
			}
			if uint32(got) == partitionID {
				break
			}
		}
		domainID := graph.DomainID(uuid.New())
		scopes[partitionID] = graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()}
		if err := notifications.OnGraphCommitted(ctx, lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 1, "quasar")); err != nil {
			t.Fatal(err)
		}
	}
	zero, one := uint32(0), uint32(1)
	statuses := []consensus.GroupStatus{
		{NodeID: 1, Leader: 1, Term: 2}, // System leadership must not wake a space partition.
		{NodeID: 1, Leader: 1, Term: 2, PartitionID: &zero},
		{NodeID: 1, Leader: 2, Term: 2, PartitionID: &one},
	}
	seen := map[uint32]uint64{}
	var scheduled []graphchange.Scope
	schedule := func(scope graphchange.Scope) { scheduled = append(scheduled, scope) }
	refresh := func(want []graphchange.Scope, wantSeen map[uint32]uint64) {
		t.Helper()
		scheduled = nil
		next, err := scheduleAsyncLexicalLeadershipScopes(ctx, notifications, schedule, statuses, partitionCount, seen)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(scheduled, want) || !reflect.DeepEqual(next, wantSeen) {
			t.Fatalf("scheduled=%+v terms=%v, want %+v / %v", scheduled, next, want, wantSeen)
		}
		seen = next
	}
	refresh([]graphchange.Scope{scopes[0]}, map[uint32]uint64{0: 2})
	refresh(nil, map[uint32]uint64{0: 2})
	statuses[1].Term = 3
	refresh([]graphchange.Scope{scopes[0]}, map[uint32]uint64{0: 3})
	statuses[1].Leader = 2
	refresh(nil, map[uint32]uint64{})
	statuses[2].Leader = 1
	refresh([]graphchange.Scope{scopes[1]}, map[uint32]uint64{1: 2})
	statuses[1].Leader = 1
	statuses[1].Term = 4
	refresh([]graphchange.Scope{scopes[0]}, map[uint32]uint64{0: 4, 1: 2})
}

func TestAsyncLexicalLeadershipRetriesFailedScopeEnumeration(t *testing.T) {
	ctx := context.Background()
	zero := uint32(0)
	statuses := []consensus.GroupStatus{{NodeID: 1, Leader: 1, Term: 2, PartitionID: &zero}}
	seen := map[uint32]uint64{}
	next, err := scheduleAsyncLexicalLeadershipScopes(ctx, graphnotification.NewModule(), func(graphchange.Scope) {
		t.Fatal("scope scheduled after failed enumeration")
	}, statuses, 1, seen)
	if !errors.Is(err, graphnotification.ErrNotInitialized) || next != nil || len(seen) != 0 {
		t.Fatalf("failed enumeration consumed leadership term: next=%v seen=%v err=%v", next, seen, err)
	}
	next, err = scheduleAsyncLexicalLeadershipScopes(ctx, newTestGraphNotifications(t), func(graphchange.Scope) {
		t.Fatal("empty history scheduled a scope")
	}, statuses, 1, seen)
	if err != nil || next[0] != 2 {
		t.Fatalf("retry did not accept current term: next=%v err=%v", next, err)
	}
}

func TestAsyncLexicalLeadershipIgnoresUnknownOwnership(t *testing.T) {
	zero := uint32(0)
	for _, state := range []consensus.GroupStatus{
		{NodeID: 1, Leader: 0, Term: 2, PartitionID: &zero},
		{NodeID: 0, Leader: 0, Term: 2, PartitionID: &zero},
		{NodeID: 1, Leader: 1, Term: 0, PartitionID: &zero},
		{NodeID: 1, Leader: 2, Term: 2, PartitionID: &zero},
	} {
		// An uninitialized notification module makes unintended enumeration fail.
		next, err := scheduleAsyncLexicalLeadershipScopes(context.Background(), graphnotification.NewModule(), func(graphchange.Scope) {
			t.Fatal("scope scheduled without local leadership")
		}, []consensus.GroupStatus{state}, 1, map[uint32]uint64{})
		if err != nil || len(next) != 0 {
			t.Fatalf("unknown/follower ownership scheduled replay: next=%v err=%v", next, err)
		}
	}
}

func TestAsyncLexicalLeadershipReplayPreservesRetentionGap(t *testing.T) {
	ctx := context.Background()
	notifications := newTestGraphNotifications(t)
	notifications.SetRetentionForTest(1, time.Hour)
	lexical := newTestLexicalModule(t)
	spaceID, domainID := domainspace.SpaceID(uuid.New()), graph.DomainID(uuid.New())
	for revision := uint64(1); revision <= 2; revision++ {
		if err := notifications.OnGraphCommitted(ctx, lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), revision, "quasar")); err != nil {
			t.Fatal(err)
		}
	}
	consumer := &asyncLexicalConsumer{lexical: lexical}
	var replayErr error
	zero := uint32(0)
	_, err := scheduleAsyncLexicalLeadershipScopes(ctx, notifications, func(scope graphchange.Scope) {
		replayErr = replayAsyncLexicalScope(ctx, nil, notifications, lexical, nil, consumer, scope)
	}, []consensus.GroupStatus{{NodeID: 1, Leader: 1, Term: 2, PartitionID: &zero}}, 1, map[uint32]uint64{})
	if err != nil || !errors.Is(replayErr, graphnotification.ErrOutOfRange) {
		t.Fatalf("leadership replay skipped missing history: schedule=%v replay=%v", err, replayErr)
	}
	if got := lexical.Status(ctx, spaceID.String(), domainID.String(), 2); got.State != lexicalservice.StateUnavailable || got.IndexedGraphRevision != 0 {
		t.Fatalf("leadership replay certified an incomplete index: %+v", got)
	}
}

func TestAsyncLexicalReplayStopsWhenLocalOwnershipIsLost(t *testing.T) {
	for name, failAt := range map[string]int{"before_read": 1, "after_collection": 2, "before_apply": 3} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			notifications := newTestGraphNotifications(t)
			lexical := newTestLexicalModule(t)
			spaceID, domainID := domainspace.SpaceID(uuid.New()), graph.DomainID(uuid.New())
			if err := notifications.OnGraphCommitted(ctx, lexicalNodeEvent(spaceID, domainID, graph.NodeID(uuid.New()), 1, "quasar")); err != nil {
				t.Fatal(err)
			}
			owner := &lexicalOwnerForTest{failAt: failAt}
			consumer := &asyncLexicalConsumer{lexical: lexical}
			err := replayAsyncLexicalScope(ctx, nil, notifications, lexical, owner, consumer, graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()})
			if !errors.Is(err, errAsyncLexicalNotLocalOwner) {
				t.Fatalf("replay error = %v, want ownership loss", err)
			}
			if got := lexical.Status(ctx, spaceID.String(), domainID.String(), 1); got.State != lexicalservice.StateUnavailable || got.IndexedGraphRevision != 0 {
				t.Fatalf("non-owner advanced lexical index: %+v", got)
			}
		})
	}
}

type lexicalOwnerForTest struct {
	checks int
	failAt int
}

func (*lexicalOwnerForTest) CurrentDomainRevision(context.Context, string, string) (int64, error) {
	return 1, nil
}

func (o *lexicalOwnerForTest) RequireLocalGraphWriteLeader(context.Context, string) error {
	o.checks++
	if o.checks >= o.failAt {
		return errors.New("not local partition leader")
	}
	return nil
}
