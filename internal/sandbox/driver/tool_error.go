package driver

import (
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"strings"
	"syscall"
	"unicode/utf8"

	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

// At the SDK's source revision, Daytona returns os.MkdirAll / file write errors in
// HTTP 400 responses. The SDK preserves message, but not a filesystem errno.
// Classify only a PathError for the exact Engine-owned payload path (or its
// staging ancestor), never an arbitrary 400 or a substring of user output.
// Upstream at SDK v0.189.0:
// https://github.com/daytonaio/daytona/blob/8c07569d1f4f88c4b84a8859c905da8b9cb7573f/apps/daemon/pkg/toolbox/fs/create_folder.go
// https://github.com/daytonaio/daytona/blob/8c07569d1f4f88c4b84a8859c905da8b9cb7573f/apps/daemon/pkg/toolbox/fs/upload_files.go
func daytonaToolError(operation string, err error, payloadPaths ...string) error {
	if err == nil {
		return nil
	}
	mapped := mapDaytonaError(sandbox.StageExecuteTool, err)
	var original *sandbox.ProviderError
	if !errors.As(mapped, &original) {
		return mapped
	}
	result := *original
	result.Diagnostic = sandbox.ProviderDiagnostic{
		Operation: operation,
		Message:   safeToolDiagnostic(err.Error(), payloadPaths),
	}
	// CreateFolder uses ConvertToolboxError; UploadFileStream instead uses
	// NewDaytonaErrorFromBody, which returns the base DaytonaError for HTTP 400.
	var validation *daytonaerrors.DaytonaValidationError
	var base *daytonaerrors.DaytonaError
	message := ""
	if errors.As(err, &validation) && validation.DaytonaError != nil {
		message = validation.Message
	} else if errors.As(err, &base) && base.StatusCode == http.StatusBadRequest {
		message = base.Message
		// The SDK's base and typed errors describe the same HTTP rejection.
		// Keep unclassified upload failures consistent with directory failures.
		result.Kind = sandbox.ProviderErrorInvalidRequest
		result.Retryable = false
		result.SafeMessage = "daytona rejected sandbox request"
	}
	if operation == "upload_payload" {
		message = bulkUploadPathError(message, payloadPaths)
	}
	if (operation == "create_payload_directory" || operation == "upload_payload") &&
		message != "" {
		for _, reason := range []struct {
			errno   syscall.Errno
			kind    sandbox.ProviderErrorKind
			message string
		}{
			{syscall.ENOSPC, sandbox.ProviderErrorStorageFull, "Execution environment storage is full."},
			{syscall.EACCES, sandbox.ProviderErrorFilesystemDenied, "Execution environment filesystem access was denied."},
			{syscall.EROFS, sandbox.ProviderErrorFilesystemReadOnly, "Execution environment filesystem is read-only."},
		} {
			if payloadPathError(message, operation, payloadPaths, reason.errno) {
				result.Kind, result.SafeMessage = reason.kind, reason.message
				result.Retryable = false
				break
			}
		}
	}
	return &result
}

func payloadPathError(message, operation string, paths []string, errno syscall.Errno) bool {
	prefixes := []string{"open ", "write ", "close ", "mkdir "}
	if operation == "create_payload_directory" {
		prefixes = []string{"mkdir "}
	}
	suffix := ": " + errno.Error()
	for _, prefix := range prefixes {
		if !strings.HasPrefix(message, prefix) || !strings.HasSuffix(message, suffix) {
			continue
		}
		failedPath := strings.TrimSuffix(strings.TrimPrefix(message, prefix), suffix)
		for _, expected := range paths {
			if failedPath == expected || (prefix == "mkdir " &&
				(failedPath == payloadStageRootPath || strings.HasPrefix(failedPath, payloadStageRootPath+"/")) &&
				strings.HasPrefix(expected, failedPath+"/")) {
				return true
			}
		}
	}
	return false
}

// UploadFileStream sends one file to /files/bulk-upload. Unlike CreateFolder,
// that endpoint responds with {errors: [...], files: [...]}; SDK v0.189.0 keeps
// that JSON as DaytonaError.Message. Unwrap only the single-file error shapes
// emitted by that daemon source, including its destination-path prefix.
func bulkUploadPathError(message string, paths []string) string {
	var response struct {
		Errors []string `json:"errors"`
	}
	if json.Unmarshal([]byte(message), &response) != nil || len(response.Errors) != 1 {
		return ""
	}
	for _, destination := range paths {
		for _, action := range []string{"create", "write", "close", "mkdir " + path.Dir(destination)} {
			prefix := destination + ": " + action + ": "
			if strings.HasPrefix(response.Errors[0], prefix) {
				return strings.TrimPrefix(response.Errors[0], prefix)
			}
		}
	}
	return ""
}

func safeToolDiagnostic(message string, payloadPaths []string) string {
	// Replace Engine-owned payload paths before applying the existing secret and
	// internal-path checks. Check the complete message before truncation.
	for _, path := range payloadPaths {
		message = strings.ReplaceAll(message, path, "<tool-payload>")
	}
	message = strings.ReplaceAll(message, payloadStageRootPath, "<tool-payload-stage>")
	if !utf8.ValidString(message) || sandbox.ValidateProviderSafeMessage(message) != nil {
		return "Provider diagnostic detail redacted."
	}
	message = strings.Join(strings.Fields(message), " ")
	const maxBytes = 512
	if len(message) > maxBytes {
		message = message[:maxBytes]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	return message
}
