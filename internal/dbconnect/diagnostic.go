package dbconnect

import "fmt"

type Provider string

const ProviderPlainDSN Provider = "plain_dsn"

type Phase string

const (
	PhaseParseConfig       Phase = "parse_config"
	PhaseOpenConnection    Phase = "open_connection"
	PhasePing              Phase = "ping"
	PhaseInitializeSchema  Phase = "initialize_schema"
	PhaseMigrateSchema     Phase = "migrate_schema"
	PhaseVerifySchema      Phase = "verify_schema"
	PhaseVerifyRuntimeRole Phase = "verify_runtime_role"
	PhaseRuntimeQuery      Phase = "runtime_query"
)

type Kind string

const (
	KindInvalidConfig              Kind = "invalid_config"
	KindEndpointUnreachable        Kind = "endpoint_unreachable"
	KindAuthenticationFailed       Kind = "authentication_failed"
	KindTLSFailed                  Kind = "tls_failed"
	KindPermissionDenied           Kind = "permission_denied"
	KindSchemaInitializationFailed Kind = "schema_initialization_failed"
	KindSchemaMigrationFailed      Kind = "schema_migration_failed"
	KindSchemaVerificationFailed   Kind = "schema_verification_failed"
	KindRuntimeRoleInvalid         Kind = "runtime_role_invalid"
	KindTimeout                    Kind = "timeout"
	KindCanceled                   Kind = "canceled"
	KindInternalError              Kind = "internal_error"
)

const UnknownDescriptorField = "<unknown>"

type Descriptor struct {
	Host     string
	Database string
}

type DiagnosticError struct {
	Provider   Provider
	Descriptor Descriptor
	Phase      Phase
	Kind       Kind
	Operation  string
	Message    string
	Cause      error
}

func (e *DiagnosticError) Error() string {
	if e.Operation != "" {
		return fmt.Sprintf("postgresql runtime client: provider=%s phase=%s kind=%s operation=%s", e.Provider, e.Phase, e.Kind, e.Operation)
	}
	return fmt.Sprintf("postgresql runtime client: provider=%s phase=%s kind=%s", e.Provider, e.Phase, e.Kind)
}

func (e *DiagnosticError) Unwrap() error {
	return e.Cause
}

func diagnostic(provider Provider, descriptor Descriptor, phase Phase, kind Kind, operation string, message string, cause error) *DiagnosticError {
	return &DiagnosticError{
		Provider:   provider,
		Descriptor: descriptor,
		Phase:      phase,
		Kind:       kind,
		Operation:  operation,
		Message:    message,
		Cause:      cause,
	}
}
