package storage

import "testing"

func TestPlanCompactionSelectsHighTombstoneSegments(t *testing.T) {
	manifest := Manifest{Segments: []string{"seg_1", "seg_2", "seg_missing"}}
	segments := map[string]SegmentMetadata{
		"seg_1": {SegmentID: "seg_1", DocCount: 10, DeletedCount: 1},
		"seg_2": {SegmentID: "seg_2", DocCount: 10, DeletedCount: 5},
	}
	got := PlanCompaction(manifest, segments, CompactionPolicy{MaxSegmentCount: 10, MaxDeletedRatio: 0.35})
	if len(got) != 1 || got[0] != "seg_2" {
		t.Fatalf("selected = %#v, want [seg_2]", got)
	}
}

func TestPlanCompactionSelectsAllKnownSegmentsWhenTooMany(t *testing.T) {
	manifest := Manifest{Segments: []string{"seg_1", "seg_2", "seg_3"}}
	segments := map[string]SegmentMetadata{
		"seg_1": {SegmentID: "seg_1", DocCount: 1},
		"seg_2": {SegmentID: "seg_2", DocCount: 1},
		"seg_3": {SegmentID: "seg_3", DocCount: 1},
	}
	got := PlanCompaction(manifest, segments, CompactionPolicy{MaxSegmentCount: 2, MaxDeletedRatio: 0.9})
	if len(got) != 3 {
		t.Fatalf("selected = %#v, want all known segments", got)
	}
}
