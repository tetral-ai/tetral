package sandbox

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"
)

var providerNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
var credentialURLCandidatePattern = regexp.MustCompile(`(?i)https?://[^\s"'<>]+`)

type ProviderStage string

const (
	StageCreateSandbox      ProviderStage = "create_sandbox"
	StageBuildArtifact      ProviderStage = "build_artifact"
	StageCheckBaseTemplate  ProviderStage = "check_base_template"
	StageApplyNetworkPolicy ProviderStage = "apply_network_policy"
	StageMountResources     ProviderStage = "mount_resources"
	StageExecuteTool        ProviderStage = "execute_tool"
	StageStatus             ProviderStage = "status"
	StageStartSandbox       ProviderStage = "start_sandbox"
	StageReleaseSandbox     ProviderStage = "release_sandbox"
)

type ProviderErrorKind string

const (
	ProviderErrorAuthFailed        ProviderErrorKind = "auth_failed"
	ProviderErrorConfigInvalid     ProviderErrorKind = "config_invalid"
	ProviderErrorUnavailable       ProviderErrorKind = "unavailable"
	ProviderErrorQuotaExceeded     ProviderErrorKind = "quota_exceeded"
	ProviderErrorTimeout           ProviderErrorKind = "timeout"
	ProviderErrorInvalidRequest    ProviderErrorKind = "invalid_request"
	ProviderErrorNotFound          ProviderErrorKind = "not_found"
	ProviderErrorConflict          ProviderErrorKind = "conflict"
	ProviderErrorMalformedResponse ProviderErrorKind = "malformed_response"
	ProviderErrorUnknown           ProviderErrorKind = "unknown"
)

type ProviderError struct {
	Provider    string
	Stage       ProviderStage
	Kind        ProviderErrorKind
	Retryable   bool
	StatusCode  int
	SafeMessage string
	Cause       error
}

func (e *ProviderError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.SafeMessage != "" {
		return e.SafeMessage
	}
	if e.Stage != "" {
		return fmt.Sprintf("sandbox provider %s failed", e.Stage)
	}
	return "sandbox provider failed"
}

func (e *ProviderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type SandboxErrorCode string

const (
	SandboxErrorProviderUnconfigured  SandboxErrorCode = "provider_unconfigured"
	SandboxErrorProviderConfigInvalid SandboxErrorCode = "provider_config_invalid"
	SandboxErrorCreateFailed          SandboxErrorCode = "create_failed"
	SandboxErrorBaseTemplateFailed    SandboxErrorCode = "base_template_failed"
	SandboxErrorNetworkFailed         SandboxErrorCode = "network_failed"
	SandboxErrorMountFailed           SandboxErrorCode = "mount_failed"
	SandboxErrorReleaseFailed         SandboxErrorCode = "release_failed"
)

type SandboxError struct {
	Code         SandboxErrorCode
	Message      string
	Cause        error
	CleanupError error
}

func (e *SandboxError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Code != "" {
		return string(e.Code)
	}
	return "sandbox error"
}

func (e *SandboxError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

type ConflictError struct {
	Message string
}

func (e *ConflictError) Error() string { return e.Message }

func ValidateProviderHandle(handle ProviderHandle) error {
	if handle.Provider == "" {
		return &ValidationError{Message: "provider is required"}
	}
	if err := ValidateProviderName(handle.Provider); err != nil {
		return err
	}
	if handle.SandboxID == "" {
		return &ValidationError{Message: "provider sandbox id is required"}
	}
	if valueContainsCredentialURL(handle.SandboxID) || containsSensitiveMetadataValue(handle.SandboxID) {
		return &ValidationError{Message: "provider sandbox id contains secret-shaped value"}
	}
	return ValidateProviderMetadata(handle.Metadata)
}

func ValidateProviderSafeMessage(message string) error {
	if valueContainsCredentialURL(message) ||
		containsSensitiveMetadataValue(message) ||
		containsInternalPath(message) {
		return &ValidationError{Message: "provider safe message contains unsafe value"}
	}
	return nil
}

func ValidateProviderName(provider string) error {
	if provider == "" {
		return &ValidationError{Message: "provider is required"}
	}
	if valueContainsCredentialURL(provider) || containsSensitiveMetadataValue(provider) {
		return &ValidationError{Message: "provider contains secret-shaped value"}
	}
	if !providerNamePattern.MatchString(provider) {
		return &ValidationError{Message: "provider has invalid shape"}
	}
	return nil
}

func ValidateProviderMetadata(metadata map[string]string) error {
	for key, value := range metadata {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		if normalizedKey == "" {
			return &ValidationError{Message: "provider metadata key is required"}
		}
		if containsSensitiveMetadataTerm(normalizedKey) {
			return &ValidationError{Message: "provider metadata contains secret-shaped key"}
		}
		if valueContainsCredentialURL(value) {
			return &ValidationError{Message: "provider metadata contains credential-bearing URL"}
		}
		if containsSensitiveMetadataValue(value) {
			return &ValidationError{Message: "provider metadata contains secret-shaped value"}
		}
	}
	return nil
}

func containsSensitiveMetadataTerm(key string) bool {
	sensitiveTerms := []string{
		"token",
		"authorization",
		"api_key",
		"api-key",
		"apikey",
		"bearer_token",
		"private_key",
		"private-key",
		"access_key",
		"access-key",
		"secret",
		"password",
		"credential",
		"encrypted",
		"raw_body",
		"body",
		"stdout",
		"stderr",
		"command_output",
		"output",
	}
	for _, term := range sensitiveTerms {
		if strings.Contains(key, term) {
			return true
		}
	}
	return false
}

func containsSensitiveMetadataValue(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return false
	}
	if strings.Contains(normalized, "authorization:") ||
		strings.Contains(normalized, "bearer ") ||
		strings.Contains(normalized, "basic ") {
		return true
	}
	sensitiveTerms := []string{
		"access_token",
		"refresh_token",
		"token",
		"api_key",
		"api-key",
		"apikey",
		"bearer_token",
		"private_key",
		"private-key",
		"access_key",
		"access-key",
		"client_secret",
		"secret",
		"password",
		"credential",
		"encrypted_token",
		"encrypted_token_bytes",
		"raw_body",
		"response_body",
		"provider_body",
		"stdout",
		"stderr",
		"command output",
		"command_output",
	}
	for _, term := range sensitiveTerms {
		if containsTermBoundary(normalized, term) {
			return true
		}
	}
	return false
}

func containsInternalPath(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	for _, marker := range []string{
		"/tmp/",
		"/var/",
		"/proc/",
		"/etc/",
		"c:\\",
		"\\users\\",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func containsTermBoundary(value string, term string) bool {
	index := strings.Index(value, term)
	for index >= 0 {
		beforeOK := index == 0 || !isMetadataWordRune(rune(value[index-1]))
		afterIndex := index + len(term)
		afterOK := afterIndex >= len(value) || !isMetadataWordRune(rune(value[afterIndex]))
		if beforeOK && afterOK {
			return true
		}
		nextStart := index + len(term)
		if nextStart >= len(value) {
			return false
		}
		next := strings.Index(value[nextStart:], term)
		if next < 0 {
			return false
		}
		index = nextStart + next
	}
	return false
}

func isMetadataWordRune(value rune) bool {
	return unicode.IsLetter(value) || unicode.IsDigit(value)
}

func valueContainsCredentialURL(value string) bool {
	candidates := []string{value}
	candidates = append(candidates, credentialURLCandidatePattern.FindAllString(value, -1)...)
	for _, candidate := range candidates {
		if parsedURLContainsCredential(candidate) {
			return true
		}
	}
	return false
}

func parsedURLContainsCredential(value string) bool {
	parsed, err := url.Parse(strings.TrimRight(value, ".,;:)]}"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return false
	}
	if parsed.User != nil {
		return true
	}
	for key := range parsed.Query() {
		if containsSensitiveMetadataTerm(strings.ToLower(key)) {
			return true
		}
	}
	return false
}
