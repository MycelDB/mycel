package admin

import (
	"context"
	"fmt"
	"strconv"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultListPageSize = 100
	maxListPageSize     = 1000
)

type OperatorService struct {
	adminv1.UnimplementedAdminOperatorServiceServer
	lister daemonadmin.AdminLister
}

func NewOperatorService(lister daemonadmin.AdminLister) *OperatorService {
	return &OperatorService{lister: lister}
}

func (s *OperatorService) ListOperators(ctx context.Context, req *adminv1.ListOperatorsRequest) (*adminv1.ListOperatorsResponse, error) {
	if _, ok := daemonauth.PrincipalFromContext(ctx); !ok {
		return nil, status.Error(codes.Unauthenticated, "operator authentication is required")
	}
	if s.lister == nil {
		return nil, status.Error(codes.FailedPrecondition, "admin lister is not configured")
	}
	offset, err := parsePageToken(req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = defaultListPageSize
	}
	if pageSize > maxListPageSize {
		pageSize = maxListPageSize
	}

	admins, err := s.lister.ListAdmins(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list admins: %v", err)
	}
	if offset > len(admins) {
		return nil, status.Error(codes.InvalidArgument, "page_token offset is beyond the operator list")
	}
	end := offset + pageSize
	if end > len(admins) {
		end = len(admins)
	}
	operators := make([]*adminv1.Operator, 0, end-offset)
	for _, admin := range admins[offset:end] {
		operators = append(operators, mapAdminSummary(admin))
	}
	var nextToken string
	if end < len(admins) {
		nextToken = strconv.Itoa(end)
	}
	return &adminv1.ListOperatorsResponse{Operators: operators, NextPageToken: nextToken}, nil
}

func mapAdminSummary(admin daemonadmin.AdminSummary) *adminv1.Operator {
	return &adminv1.Operator{
		OperatorId: admin.ID,
		Username:   admin.Username,
		State:      adminv1.OperatorState_OPERATOR_STATE_ACTIVE,
		CreateTime: timestamppb.New(admin.CreatedAt),
	}
}

func parsePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("page_token must be a non-negative integer offset")
	}
	return offset, nil
}
