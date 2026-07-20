package topology

import (
	"context"

	"github.com/myceldb/mycel/internal/clustering/model"
)

type Store interface {
	Load(ctx context.Context) (model.Snapshot, error)
	Save(ctx context.Context, snapshot model.Snapshot) error
}
