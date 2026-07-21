package runtime

import (
	"context"
	"log/slog"
	"testing"
)

type testHost struct{}

func (testHost) Log() *slog.Logger { return slog.Default() }
func (testHost) DataDir() string   { return "" }

type testService struct{}

func (testService) Name() string { return "test" }
func (testService) Init(context.Context, Host) InitResult {
	return OK("test")
}

func TestServiceUsesCommonHost(t *testing.T) {
	var _ Host = testHost{}
	var _ Service = testService{}
	if result := (testService{}).Init(context.Background(), testHost{}); !result.OK {
		t.Fatalf("Init() = %+v, want OK", result)
	}
}
