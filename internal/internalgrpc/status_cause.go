package internalgrpc

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StatusWithCause answers the caller with code and message and gives the
// failure record of the call cause, which the answer does not carry.
func StatusWithCause(code codes.Code, message string, cause error) error {
	return &statusWithCause{status: status.New(code, message), cause: cause}
}

type statusWithCause struct {
	status *status.Status
	cause  error
}

func (e *statusWithCause) Error() string { return e.status.Err().Error() }

func (e *statusWithCause) GRPCStatus() *status.Status { return e.status }

func (e *statusWithCause) RecordedCause() error { return e.cause }
