package auth

import (
	"context"
	"errors"
)

type auditContextKey struct{}

// WithAuditRecorder borrows the HTTP owner's recorder for domain decisions.
// The recorder retains no credentials, selectors, identities, or issuer URLs.
func WithAuditRecorder(ctx context.Context, recorder AuditRecorder) context.Context {
	return context.WithValue(ctx, auditContextKey{}, recorder)
}

// RecordDecision emits one fixed-stage outcome. Facts are supplied only after
// their owning lookup/signature boundary establishes them; absence stays absent.
func RecordDecision(ctx context.Context, stage string, err error, facts AuditEvent) {
	switch stage {
	case "input", "admission", "rule", "assertion", "grant", "operation", "signed_principal", "issuance", "limiter":
	default:
		return
	}
	if !IsRegisteredOperation(facts.Operation) {
		facts.Operation = ""
	}
	facts.Stage = stage
	facts.Result = "success"
	facts.Code = "auth_" + stage + "_success"
	if err != nil {
		var unavailable *UnavailableError
		var permission *PermissionError
		var invalid *AuthenticationError
		var validation *ValidationError
		switch {
		case errors.As(err, &unavailable):
			facts.Result = "unavailable"
		case errors.As(err, &permission):
			facts.Result = "denied"
		case errors.As(err, &invalid), errors.As(err, &validation):
			facts.Result = "invalid"
		default:
			facts.Result = "unavailable"
			if stage == "input" {
				facts.Result = "invalid"
			}
		}
		if stage == "grant" && facts.Result == "invalid" {
			facts.Result = "denied"
		}
		facts.Code = "auth_" + stage + "_" + facts.Result
		if facts.Result == "unavailable" {
			switch stage {
			case "admission", "rule", "grant", "issuance":
				facts.Code = "auth_store_unavailable"
			case "assertion":
				facts.Code = "auth_jwks_unavailable"
			}
		}
	}
	if stage == "limiter" {
		facts.Result = "limited"
		facts.Code = "auth_limiter_limited"
	}
	if facts.IdentityKind != IdentityHuman && facts.IdentityKind != IdentityService {
		facts.IdentityKind = ""
	}
	if facts.RuleRevision < 0 {
		facts.RuleRevision = 0
	}
	if facts.IdentityRevision < 0 {
		facts.IdentityRevision = 0
	}
	if facts.GrantRevision < 0 {
		facts.GrantRevision = 0
	}
	recorder, _ := ctx.Value(auditContextKey{}).(AuditRecorder)
	recordAuthEvent(ctx, recorder, facts)
}

// AuditFacts deliberately excludes identity IDs and authority selectors.
func AuditFacts(p Principal) AuditEvent {
	if p.Validate() != nil {
		return AuditEvent{}
	}
	facts := AuditEvent{RuleRevision: p.Authority.RuleRevision, IdentityRevision: p.Authority.IdentityRevision, GrantRevision: p.Authority.GrantRevision}
	if p.Identity != nil {
		facts.IdentityKind = p.Identity.Kind
	}
	return facts
}

// AuthorizeWithAudit is the shared public gate with its denial diagnostic.
// Successful operation checks stay quiet; no business effect occurs here.
func AuthorizeWithAudit(ctx context.Context, p Principal, operation Operation, resource ResourceReference) error {
	err := Authorize(p, operation, resource)
	if err != nil {
		facts := AuditFacts(p)
		facts.Operation = operation
		RecordDecision(ctx, "operation", err, facts)
	}
	return err
}
