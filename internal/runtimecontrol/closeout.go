package runtimecontrol

import (
	"errors"

	"google.golang.org/grpc/status"
)

// This file owns closeout sentinels. Scope supersession maps to the closed
// stale outcome; unrepairable failures retain their precise gRPC status.

const (
	ScopeSupersededCode       = "scope_superseded"
	UnrepairableCode          = "closeout_unrepairable"
	InterruptBarrierStaleCode = "thread_interrupt_barrier_stale"
	ModelRequestSealedCode    = "model_request_sealed"
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

func ScopeSupersededError(err error) error {
	return &closeoutSentinelError{code: ScopeSupersededCode, err: err}
}

func CloseoutUnrepairableError(err error) error {
	return &closeoutSentinelError{code: UnrepairableCode, err: err}
}

func ThreadInterruptBarrierStaleError(err error) error {
	return &closeoutSentinelError{code: InterruptBarrierStaleCode, err: err}
}

func ModelRequestSealedError(err error) error {
	return &closeoutSentinelError{code: ModelRequestSealedCode, err: err}
}

func SentinelCode(err error) (string, bool) {
	var sentinel *closeoutSentinelError
	if !errors.As(err, &sentinel) {
		return "", false
	}
	return sentinel.code, true
}

func IsScopeSupersededError(err error) bool {
	code, ok := SentinelCode(err)
	return ok && code == ScopeSupersededCode
}

func IsThreadInterruptBarrierStaleError(err error) bool {
	code, ok := SentinelCode(err)
	return ok && code == InterruptBarrierStaleCode
}

func IsModelRequestSealedError(err error) bool {
	code, ok := SentinelCode(err)
	return ok && code == ModelRequestSealedCode
}

func IsConversationMutationStaleError(err error) bool {
	return IsScopeSupersededError(err) || IsThreadInterruptBarrierStaleError(err)
}
