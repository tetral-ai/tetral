package driver

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

func TestMapDaytonaErrorClassifiesCreateCapacityFamilyMessages(t *testing.T) {
	const diskResponse = `Total disk limit exceeded. Maximum allowed: 30GiB.
Consider archiving your unused Sandboxes to free up available storage.
To increase concurrency limits, upgrade your organization's Tier by visiting https://app.daytona.io/dashboard/limits.`
	for _, test := range []struct {
		name    string
		message string
	}{
		{name: "disk response", message: diskResponse},
		{name: "disk CRLF response", message: strings.ReplaceAll(diskResponse, "\n", "\r\n")},
		{name: "changed advisory", message: "Total disk limit exceeded. Maximum allowed: 30GiB.\nProvider guidance changed."},
		{name: "CPU response", message: "Total CPU limit exceeded. Maximum allowed: 10.\nProvider guidance changed."},
		{name: "memory response", message: "Total memory limit exceeded. Maximum allowed: 16GiB.\nProvider guidance changed."},
	} {
		t.Run(test.name, func(t *testing.T) {
			mapped := mapDaytonaError(sandbox.StageCreateSandbox, daytonaerrors.NewDaytonaValidationError(test.message, nil))
			var providerErr *sandbox.ProviderError
			if !errors.As(mapped, &providerErr) {
				t.Fatalf("mapDaytonaError() = %T; want ProviderError", mapped)
			}
			if providerErr.Kind != sandbox.ProviderErrorQuotaExceeded || !providerErr.Retryable ||
				providerErr.StatusCode != http.StatusBadRequest || providerErr.SafeMessage != "sandbox provider capacity is unavailable" {
				t.Fatalf("mapDaytonaError() = %+v; want retryable Create quota classification", providerErr)
			}
			if strings.Contains(providerErr.SafeMessage, "30GiB") || strings.Contains(providerErr.SafeMessage, "Total disk") ||
				strings.Contains(providerErr.SafeMessage, "CPU") || strings.Contains(providerErr.SafeMessage, "memory") {
				t.Fatalf("safe message exposed provider capacity detail: %q", providerErr.SafeMessage)
			}
		})
	}
}

func TestMapDaytonaErrorFailsClosedForOtherValidationMessages(t *testing.T) {
	const liveResponse = `Total disk limit exceeded. Maximum allowed: 30GiB.
Consider archiving your unused Sandboxes to free up available storage.
To increase concurrency limits, upgrade your organization's Tier by visiting https://app.daytona.io/dashboard/limits.`
	tests := []struct {
		name    string
		stage   sandbox.ProviderStage
		message string
	}{
		{name: "unrelated", stage: sandbox.StageCreateSandbox, message: "invalid snapshot"},
		{name: "prefixed first line", stage: sandbox.StageCreateSandbox, message: "capacity: Total disk limit exceeded. Maximum allowed: 30GiB."},
		{name: "embedded later", stage: sandbox.StageCreateSandbox, message: "provider guidance\n" + liveResponse},
		{name: "malformed", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed 30GiB."},
		{name: "zero", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: 0GiB."},
		{name: "unsupported unit", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: 30GB."},
		{name: "same-line suffix", stage: sandbox.StageCreateSandbox, message: "Total disk limit exceeded. Maximum allowed: 30GiB. trailing detail"},
		{name: "unknown resource", stage: sandbox.StageCreateSandbox, message: "Total GPU limit exceeded. Maximum allowed: 1."},
		{name: "CPU byte unit", stage: sandbox.StageCreateSandbox, message: "Total CPU limit exceeded. Maximum allowed: 10GiB."},
		{name: "memory without byte unit", stage: sandbox.StageCreateSandbox, message: "Total memory limit exceeded. Maximum allowed: 16."},
		{name: "non create", stage: sandbox.StageStatus, message: liveResponse},
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

func TestMapDaytonaErrorKeepsAuthenticationOutsideCreateCapacity(t *testing.T) {
	mapped := mapDaytonaError(
		sandbox.StageCreateSandbox,
		daytonaerrors.NewDaytonaAuthenticationError("Total CPU limit exceeded. Maximum allowed: 10.", nil),
	)
	var providerErr *sandbox.ProviderError
	if !errors.As(mapped, &providerErr) {
		t.Fatalf("mapDaytonaError() = %T; want ProviderError", mapped)
	}
	if providerErr.Kind != sandbox.ProviderErrorAuthFailed || providerErr.Retryable ||
		providerErr.StatusCode != http.StatusUnauthorized || providerErr.SafeMessage != "daytona authentication failed" {
		t.Fatalf("mapDaytonaError() = %+v; want existing terminal authentication classification", providerErr)
	}
}
