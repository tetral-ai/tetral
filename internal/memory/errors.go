package memory

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

type PathConflictError struct {
	Message             string
	ConflictingMemoryID string
	ConflictingPath     string
}

func (e *PathConflictError) Error() string { return e.Message }

type PreconditionFailedError struct {
	Message string
}

func (e *PreconditionFailedError) Error() string { return e.Message }

type QuotaError struct {
	Message string
}

func (e *QuotaError) Error() string { return e.Message }

type RequestTooLargeError struct {
	Message string
}

func (e *RequestTooLargeError) Error() string { return e.Message }

type InternalError struct {
	Message string
	Cause   error
}

func (e *InternalError) Error() string { return e.Message }

func (e *InternalError) Unwrap() error { return e.Cause }
