package membership

import "context"

type Store interface {
	Load(ctx context.Context) (StoreData, error)
	Save(ctx context.Context, data StoreData) error
	UpsertMember(ctx context.Context, member Member) error
	FindByNodeName(ctx context.Context, name string) (Member, bool, error)
	FindByNodeID(ctx context.Context, id string) (Member, bool, error)
}
