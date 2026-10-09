package runtimecontrol

type PreparationError struct {
	Kind      string
	Message   string
	Retryable bool
}

func (e PreparationError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Kind
}
