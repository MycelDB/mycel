package space

// SpaceSettings defines tunable limits and behavior at space scope.
type SpaceSettings struct {
	MaxSpaceBytes     int64
	TargetChunkBytes  int64
	MaxChunkBytes     int64
	MaxAssetBytes     int64
	MaxPDFBytes       int64
	CompactionEnabled bool
}
