package agentruntimebridge

import (
	"encoding/json"
	"strings"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const runtimeTerminationFailureMaxBytes = 64 * 1024

func parseRuntimeTerminationFailure(raw string) (runtimecontrol.RuntimeTerminationFailure, string, error) {
	if raw == "" || len(raw) > runtimeTerminationFailureMaxBytes {
		return runtimecontrol.RuntimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure is invalid")
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(raw), &object); err != nil || object == nil {
		return runtimecontrol.RuntimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure must be a JSON object")
	}
	var failure runtimecontrol.RuntimeTerminationFailure
	if err := json.Unmarshal([]byte(raw), &failure); err != nil {
		return runtimecontrol.RuntimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure is invalid")
	}
	if failure.Retryable || failure.RetryStatus.Type != "terminal" || !isWhitelistedRuntimeTerminationFailure(failure) {
		return runtimecontrol.RuntimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime failure is not terminal")
	}
	canonical, err := runtimecontrol.MarshalJSON(object)
	if err != nil {
		return runtimecontrol.RuntimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure is invalid")
	}
	return failure, canonical, nil
}

func isWhitelistedRuntimeTerminationFailure(failure runtimecontrol.RuntimeTerminationFailure) bool {
	if failure.Type == "runtime" {
		return (failure.Code == "runtime_invalid_sequence" && failure.Reason == "runtime_contract_validation") ||
			(failure.Code == "runtime_persistence_exhausted" && failure.Reason == "runtime_input_commit_exhausted")
	}
	if failure.Type != "provider" {
		return false
	}
	switch failure.Code {
	case "credential_required", "platform_keys_exhausted", "provider_key_unavailable",
		"provider_rate_limited", "provider_timeout", "provider_stream_error", "provider_unavailable",
		"provider_cancelled", "attachment_unavailable":
		return false
	default:
		return strings.HasPrefix(failure.Code, "provider_") || failure.Code == "context_overflow"
	}
}
