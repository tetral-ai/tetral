package driver

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

func TestMapDaytonaErrorClassifiesCreateDiskCapacityMessages(t *testing.T) {
	for _, message := range []string{
		"Total disk limit exceeded. Maximum allowed: 30GiB.",
		"Total disk limit exceeded. Maximum allowed: 1KiB.",
		"Total disk limit exceeded. Maximum allowed: 1MiB.",
		"Total disk limit exceeded. Maximum allowed: 1GiB.",
		"Total disk limit exceeded. Maximum allowed: 1TiB.",
		"Total disk limit exceeded. Maximum allowed: 18446744073709551615GiB.",
		" \tTotal disk limit exceeded. Maximum allowed: 30GiB.\n",
		"\u00a0Total disk limit exceeded. Maximum allowed: 30GiB.\u00a0",
	} {
		t.Run(message, func(t *testing.T) {
			mapped := mapDaytonaError(sandbox.StageCreateSandbox, daytonaerrors.NewDaytonaValidationError(message, nil))
			var providerErr *sandbox.ProviderError
			if !errors.As(mapped, &providerErr) {
				t.Fatalf("mapDaytonaError() = %T; want ProviderError", mapped)
			}
			if providerErr.Kind != sandbox.ProviderErrorQuotaExceeded || !providerErr.Retryable ||
				providerErr.StatusCode != http.StatusBadRequest || providerErr.SafeMessage != "sandbox provider capacity is unavailable" {
				t.Fatalf("mapDaytonaError() = %+v; want retryable Create quota classification", providerErr)
			}
			if strings.Contains(providerErr.SafeMessage, "30GiB") || strings.Contains(providerErr.SafeMessage, "Total disk") {
				t.Fatalf("safe message exposed provider capacity detail: %q", providerErr.SafeMessage)
			}
		})
	}
}

func TestMapDaytonaErrorFailsClosedForOtherValidationMessages(t *testing.T) {
	tests := []struct {
		name    string
		stage   sandbox.ProviderStage
		message string
	}{
		{name: "unrelated", stage: sandbox.StageCreateSandbox, message: "invalid snapshot"},
		{name: "missing", stage: sandbox.StageCreateSandbox},
		{name: "prefix only", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded."},
		{name: "suffix", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: 30GiB. retry later"},
		{name: "prefix", stage: sandbox.StageCreateSandbox, message: "capacity: Total disk limit exceeded. Maximum allowed: 30GiB."},
		{name: "embedded", stage: sandbox.StageCreateSandbox, message: "provider said [Total disk limit exceeded. Maximum allowed: 30GiB.]"},
		{name: "zero", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: 0GiB."},
		{name: "leading zero", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: 030GiB."},
		{name: "positive sign", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: +30GiB."},
		{name: "negative sign", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: -30GiB."},
		{name: "malformed number", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: 3O GiB."},
		{name: "ordinary internal whitespace", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded.  Maximum allowed: 30GiB."},
		{name: "non-breaking internal whitespace", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed:\u00a030GiB."},
		{name: "unrecognized unit", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: 30GB."},
		{name: "overflow", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: 18446744073709551616GiB."},
		{name: "non create", stage: sandbox.StageStatus, message: "Total disk limit exceeded. Maximum allowed: 30GiB."},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mapped := mapDaytonaError(test.stage, daytonaerrors.NewDaytonaValidationError(test.message, nil))
			var providerErr *sandbox.ProviderError
			if !errors.As(mapped, &providerErr) {
				t.Fatalf("mapDaytonaError() = %T; want ProviderError", mapped)
			}
			if providerErr.Kind != sandbox.ProviderErrorInvalidRequest || providerErr.Retryable ||
				providerErr.StatusCode != http.StatusBadRequest || providerErr.SafeMessage != "daytona rejected sandbox request" {
				t.Fatalf("mapDaytonaError() = %+v; want existing terminal validation classification", providerErr)
			}
		})
	}
}
