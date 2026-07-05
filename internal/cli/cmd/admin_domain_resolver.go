package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	"google.golang.org/grpc"
)

func daemonResolveAdminDomainID(ctx context.Context, conn *grpc.ClientConn, authCtx context.Context, spaceID string, domainRef string) (string, error) {
	ref := strings.TrimSpace(domainRef)
	if ref == "" {
		ref = "default"
	}
	if id, err := uuid.Parse(ref); err == nil && id != uuid.Nil {
		return id.String(), nil
	}
	res, err := adminv1.NewAdminDomainServiceClient(conn).GetDomain(authCtx, &adminv1.AdminDomainServiceGetDomainRequest{SpaceId: spaceID, DomainRef: ref})
	if err != nil {
		return "", err
	}
	if res.GetDomain().GetDomainId() == "" {
		return "", fmt.Errorf("domain %q not found", domainRef)
	}
	return res.GetDomain().GetDomainId(), nil
}
