package gitticket

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }
