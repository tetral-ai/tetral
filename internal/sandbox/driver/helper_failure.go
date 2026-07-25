package driver

import "fmt"

// HelperFailureError marks cases where the sandbox helper failed before
// emitting an authoritative envelope, so Bridge can synthesize helper_failure.
type HelperFailureError struct {
	Message string
	Cause   error
}

func (e *HelperFailureError) Error() string {
	if e == nil {
		return "sandbox helper failed"
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *HelperFailureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// newHelperFailureError builds the HelperFailureError marker. executeHelper
// returns it in the two cases where the helper produced no authoritative
// envelope: a non-zero helper process exit code, and stdout that fails envelope
// validation. message is a user-safe summary and cause is the optional
// underlying error. The caller synthesizes a helper_failure tool result from
// this marker, keeping it distinct from an exit-0 envelope whose status is
// error, which is authoritative.
func newHelperFailureError(message string, cause error) error {
	return &HelperFailureError{Message: message, Cause: cause}
}
