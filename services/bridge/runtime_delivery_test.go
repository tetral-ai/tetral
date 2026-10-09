package agentruntimebridge

import (
	"context"
)

type countingRuntimeCommandTokenSource struct {
	calls int
}

func (s *countingRuntimeCommandTokenSource) Token(context.Context) (string, error) {
	s.calls++
	return "test-token", nil
}
