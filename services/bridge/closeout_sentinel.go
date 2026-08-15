package agentruntimebridge

import (
	"errors"

	"google.golang.org/grpc/status"
)

// This file owns closeout sentinels. Scope supersession maps to the closed
// stale outcome; unrepairable failures retain their precise gRPC status.

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
