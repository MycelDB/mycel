package storage

import "knot_db/model"

type Chunk struct {
	ChunkID string
	OwnerID model.UserID
	SpaceID model.SpaceID
}
