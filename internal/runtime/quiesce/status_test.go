package quiesce

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCErrorMapsErrQuiescedToUnavailable(t *testing.T) {
	err := GRPCError(ErrQuiesced)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %s, want %s", status.Code(err), codes.Unavailable)
	}
	if reason, ok := ErrorInfoReason(err); !ok || reason != ReasonClusterQuiesced {
		t.Fatalf("ErrorInfoReason() = %q, %v; want %q, true", reason, ok, ReasonClusterQuiesced)
	}
}

func TestTransactionAbortedByQuiesceErrorUsesStableReason(t *testing.T) {
	err := TransactionAbortedByQuiesceError(ErrQuiesced)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %s, want %s", status.Code(err), codes.Unavailable)
	}
	if reason, ok := ErrorInfoReason(err); !ok || reason != ReasonTransactionAbortedByQuiesce {
		t.Fatalf("ErrorInfoReason() = %q, %v; want %q, true", reason, ok, ReasonTransactionAbortedByQuiesce)
	}
}

func TestGRPCErrorPreservesOtherErrors(t *testing.T) {
	other := errors.New("other")
	if got := GRPCError(other); got != other {
		t.Fatalf("GRPCError() = %v, want original", got)
	}
	if got := GRPCError(nil); got != nil {
		t.Fatalf("GRPCError(nil) = %v, want nil", got)
	}
}
