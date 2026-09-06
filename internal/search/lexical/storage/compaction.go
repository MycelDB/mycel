package storage

// CompactionPolicy describes when immutable lexical segments should be selected
// for future merge/compaction work. V1 does not run automatic compaction; manual
// rebuild remains the operational cleanup path.
type CompactionPolicy struct {
	MaxSegmentCount int
	MaxDeletedRatio float64
}

// DefaultCompactionPolicy is intentionally conservative. It provides a stable
// hook for LS6+ callers without enabling automatic compaction in LS5.
var DefaultCompactionPolicy = CompactionPolicy{
	MaxSegmentCount: 32,
	MaxDeletedRatio: 0.35,
}

// PlanCompaction returns active segment IDs that are eligible for future
// compaction. Missing metadata is ignored so callers can separately surface
// diagnostics or trigger rebuild for corrupt/missing segments.
func PlanCompaction(manifest Manifest, segments map[string]SegmentMetadata, policy CompactionPolicy) []string {
	if policy.MaxSegmentCount <= 0 {
		policy.MaxSegmentCount = DefaultCompactionPolicy.MaxSegmentCount
	}
	if policy.MaxDeletedRatio <= 0 {
		policy.MaxDeletedRatio = DefaultCompactionPolicy.MaxDeletedRatio
	}
	selected := make([]string, 0)
	tooManySegments := len(manifest.Segments) > policy.MaxSegmentCount
	for _, segmentID := range manifest.Segments {
		meta, ok := segments[segmentID]
		if !ok || meta.DocCount == 0 {
			continue
		}
		deletedRatio := float64(meta.DeletedCount) / float64(meta.DocCount)
		if tooManySegments || deletedRatio >= policy.MaxDeletedRatio {
			selected = append(selected, segmentID)
		}
	}
	return selected
}
