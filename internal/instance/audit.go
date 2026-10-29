package instance

import (
	"context"
	"log/slog"
)

// AuditEvent is the INFO audit record of one credential operation.
const AuditEvent = "instance_credential_audit"

// The credential operations an audit record names.
const (
	AuditIssue    = "issue"
	AuditRevoke   = "revoke"
	AuditFetch    = "fetch"
	AuditActivate = "activate"
)

// Actor is the principal credential that performed a credential operation.
type Actor struct {
	CredentialKind string
	CredentialID   string
}

// AuditCredentialOperation writes the audit record of one credential
// operation once its transaction has committed: the actor, the instance, the
// credential and the operation, never a key. The record's time is the
// operation's time.
func AuditCredentialOperation(ctx context.Context, logger *slog.Logger, actor Actor, instanceID, credentialID, operation string) {
	logger.InfoContext(ctx, AuditEvent,
		slog.String("actor.credential.kind", actor.CredentialKind),
		slog.String("actor.credential.id", actor.CredentialID),
		slog.String("instance.id", instanceID),
		slog.String("credential.id", credentialID),
		slog.String("operation", operation),
	)
}
