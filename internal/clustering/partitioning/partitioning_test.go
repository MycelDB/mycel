package partitioning

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestPartitionForSpaceStable(t *testing.T) {
	got, err := PartitionForSpace("00000000-0000-0000-0000-000000000123", 64)
	if err != nil {
		t.Fatalf("PartitionForSpace() error = %v", err)
	}
	for i := 0; i < 10; i++ {
		again, err := PartitionForSpace("00000000-0000-0000-0000-000000000123", 64)
		if err != nil {
			t.Fatalf("PartitionForSpace() repeat error = %v", err)
		}
		if again != got {
			t.Fatalf("partition changed: first=%d again=%d", got, again)
		}
	}
}

func TestPartitionForSpaceKnownValues(t *testing.T) {
	tests := []struct {
		spaceID string
		count   uint32
		want    PartitionID
	}{
		{"00000000-0000-0000-0000-000000000001", 64, 62},
		{"00000000-0000-0000-0000-000000000002", 64, 11},
		{"00000000-0000-0000-0000-000000000003", 64, 24},
		{"00000000-0000-0000-0000-000000000099", 32, 11},
	}
	for _, tt := range tests {
		got, err := PartitionForSpace(tt.spaceID, tt.count)
		if err != nil {
			t.Fatalf("PartitionForSpace(%q, %d) error = %v", tt.spaceID, tt.count, err)
		}
		if got != tt.want {
			t.Fatalf("PartitionForSpace(%q, %d)=%d want %d", tt.spaceID, tt.count, got, tt.want)
		}
	}
}

func TestPartitionForSpaceIDMatchesStringHelper(t *testing.T) {
	spaceID := domainspace.SpaceID(uuid.MustParse("00000000-0000-0000-0000-000000000001"))
	fromTyped, err := PartitionForSpaceID(spaceID, 64)
	if err != nil {
		t.Fatalf("PartitionForSpaceID() error = %v", err)
	}
	fromString, err := PartitionForSpace(spaceID.String(), 64)
	if err != nil {
		t.Fatalf("PartitionForSpace() error = %v", err)
	}
	if fromTyped != fromString {
		t.Fatalf("typed partition=%d string partition=%d", fromTyped, fromString)
	}
}

func TestPartitionForSpaceRejectsInvalidInput(t *testing.T) {
	if _, err := PartitionForSpace("", 64); err == nil {
		t.Fatal("expected empty space_id to fail")
	}
	if _, err := PartitionForSpace("not-a-uuid", 64); err == nil {
		t.Fatal("expected non-UUID space_id to fail")
	}
	if _, err := PartitionForSpace("00000000-0000-0000-0000-000000000001", 0); err == nil {
		t.Fatal("expected zero partition count to fail")
	}
	if _, err := PartitionForSpaceID(domainspace.SpaceID(uuid.Nil), 64); err == nil {
		t.Fatal("expected nil typed space_id to fail")
	}
}

func TestPartitionForSpaceDistribution(t *testing.T) {
	const partitions = 64
	seen := map[PartitionID]int{}
	for i := 0; i < 4096; i++ {
		p, err := PartitionForSpace(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1), partitions)
		if err != nil {
			t.Fatalf("PartitionForSpace() error = %v", err)
		}
		seen[p]++
	}
	if len(seen) != partitions {
		t.Fatalf("expected all partitions to be used, got %d/%d", len(seen), partitions)
	}
	for p, count := range seen {
		if count < 32 || count > 96 {
			t.Fatalf("partition %d distribution out of expected range: %d", p, count)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	if err := (Config{PartitionCount: 64}).Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if err := (Config{}).Validate(); err == nil {
		t.Fatal("expected zero partition count to fail")
	}
}
