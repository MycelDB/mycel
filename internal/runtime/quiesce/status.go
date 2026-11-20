package quiesce

import (
	"errors"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// ErrorInfoDomain is the google.rpc.ErrorInfo domain used for stable backup
	// quiesce and restore-readiness error reasons.
	ErrorInfoDomain = "mycel.backup"

	ReasonClusterQuiesced             = "cluster_quiesced"
	ReasonTransactionAbortedByQuiesce = "transaction_aborted_by_quiesce"
)

// GRPCError maps quiesce-domain errors to public gRPC errors. Non-quiesce
// errors are returned unchanged so callers can preserve more specific failures.
func GRPCError(err error) error {
	return GRPCErrorWithReason(err, ReasonClusterQuiesced, "myceld is temporarily quiesced", nil)
}

// TransactionAbortedByQuiesceError maps ErrQuiesced to the stable transaction
// abort reason used when a write transaction cannot be finalized because backup
// quiescence won the admission race. Non-quiesce errors are returned unchanged.
func TransactionAbortedByQuiesceError(err error) error {
	if err == nil {
		return nil
	}
	if reason, ok := ErrorInfoReason(err); ok && reason == ReasonClusterQuiesced {
		return unavailableError(ReasonTransactionAbortedByQuiesce, "transaction aborted by backup quiescence", nil)
	}
	return GRPCErrorWithReason(err, ReasonTransactionAbortedByQuiesce, "transaction aborted by backup quiescence", nil)
}

// GRPCErrorWithReason maps ErrQuiesced to an Unavailable gRPC status decorated
// with google.rpc.ErrorInfo. Non-quiesce errors are returned unchanged so
// callers can preserve more specific failures.
func GRPCErrorWithReason(err error, reason, message string, metadata map[string]string) error {
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrQuiesced) {
		return err
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "myceld is temporarily quiesced"
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = ReasonClusterQuiesced
	}
	return unavailableError(reason, message, metadata)
}

func unavailableError(reason, message string, metadata map[string]string) error {
	st := status.New(codes.Unavailable, message)
	if len(metadata) == 0 {
		metadata = nil
	}
	withDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: ErrorInfoDomain, Metadata: metadata})
	if detailErr != nil {
		return st.Err()
	}
	return withDetails.Err()
}

// ErrorInfoReason extracts a Mycel backup/quiesce ErrorInfo reason from err.
func ErrorInfoReason(err error) (string, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		return "", false
	}
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != ErrorInfoDomain {
			continue
		}
		reason := strings.TrimSpace(info.GetReason())
		if reason != "" {
			return reason, true
		}
	}
	return "", false
}
