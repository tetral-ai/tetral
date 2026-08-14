package agentruntimebridge

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This file owns closeout write-rejection sentinels: the closeout writers wrap their
// errors here so api.go answers with a rejected ACK instead of a retryable gRPC error.

const (
	closeoutScopeSupersededCode = "scope_superseded"
	closeoutUnrepairableCode    = "closeout_unrepairable"
)

type closeoutSentinelError struct {
	code string
	err  error
}

func (e *closeoutSentinelError) Error() string {
	return e.err.Error()
}

func (e *closeoutSentinelError) Unwrap() error {
	return e.err
}

func (e *closeoutSentinelError) GRPCStatus() *status.Status {
	return status.Convert(e.err)
}

func scopeSupersededError(err error) error {
	return &closeoutSentinelError{code: closeoutScopeSupersededCode, err: err}
}

func closeoutUnrepairableError(err error) error {
	return &closeoutSentinelError{code: closeoutUnrepairableCode, err: err}
}

func closeoutSentinelCode(err error) (string, bool) {
	var sentinel *closeoutSentinelError
	if !errors.As(err, &sentinel) {
		return "", false
	}
	return sentinel.code, true
}

func isScopeSupersededError(err error) bool {
	code, ok := closeoutSentinelCode(err)
	return ok && code == closeoutScopeSupersededCode
}

func closeoutWriteRejectionCode(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if code, ok := closeoutSentinelCode(err); ok {
		return code, true
	}
	switch status.Code(err) {
	// InvalidArgument and AlreadyExists are structurally unrepairable: no scope retry can clear them.
	case codes.InvalidArgument, codes.AlreadyExists:
		return closeoutUnrepairableCode, true
	default:
		return "", false
	}
}
