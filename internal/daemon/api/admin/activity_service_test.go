package admin

import (
	"context"
	"testing"

	"github.com/myceldb/mycel/internal/activity/model"
	"github.com/myceldb/mycel/internal/activity/storage"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type activityAuthz struct {
	capability string
	ok         bool
}

func (a *activityAuthz) HasCapability(_ context.Context, _ string, capability string) (bool, error) {
	a.capability = capability
	return a.ok, nil
}

type activityManagerStub struct{ event model.Event }

func (m *activityManagerStub) Append(context.Context, model.Event) (storage.AppendResult, error) {
	return storage.AppendResult{Event: m.event}, nil
}
func (m *activityManagerStub) Get(context.Context, string) (model.Event, error) { return m.event, nil }
func (m *activityManagerStub) List(context.Context, model.ListFilter) (model.ListResult, error) {
	return model.ListResult{Events: []model.Event{m.event}}, nil
}
func (m *activityManagerStub) Emit(context.Context, string, string, string, string, func(*model.Event)) error {
	return nil
}

func TestAdminActivityRequiresAuditCapabilities(t *testing.T) {
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{PrincipalID: "operator"})
	event := model.Event{EventID: "evt_1", Severity: model.SeverityInfo, Category: model.CategoryLifecycle, Type: "daemon.started", Message: "Daemon started", Source: model.Source{Component: "daemon"}}
	authz := &activityAuthz{ok: true}
	svc := NewAdminActivityService(&activityManagerStub{event: event}, authz)
	if _, err := svc.ListActivityEvents(ctx, &adminv1.ListActivityEventsRequest{}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if authz.capability != capAuditRead {
		t.Fatalf("list capability = %q", authz.capability)
	}
	if _, err := svc.AppendActivityEvent(ctx, &adminv1.AppendActivityEventRequest{Event: eventToProto(event)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if authz.capability != capAuditWrite {
		t.Fatalf("append capability = %q", authz.capability)
	}
}

func TestAdminActivityDenied(t *testing.T) {
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{PrincipalID: "operator"})
	svc := NewAdminActivityService(&activityManagerStub{}, &activityAuthz{})
	_, err := svc.ListActivityEvents(ctx, &adminv1.ListActivityEventsRequest{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, err=%v", status.Code(err), err)
	}
}
