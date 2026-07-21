package quiesce

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCError maps quiesce-domain errors to public gRPC errors. Non-quiesce
// errors are returned unchanged so callers can preserve more specific failures.
func GRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrQuiesced) {
		return status.Error(codes.Unavailable, "myceld is temporarily quiesced")
	}
	return err
}
