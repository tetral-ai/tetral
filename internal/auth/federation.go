package auth

import "time"

// FederationRule is a provisioned security snapshot. Revision covers issuer,
// audience, endpoint, algorithm, egress, and TLS trust changes together.
type FederationRule struct {
	ID             string   `json:"id"`
	OrganizationID string   `json:"organization_id"`
	Issuer         string   `json:"issuer"`
	Audience       string   `json:"audience"`
	DiscoveryURL   string   `json:"discovery_url,omitempty"`
	JWKSURL        string   `json:"jwks_url,omitempty"`
	AllowedOrigins []string `json:"allowed_origins,omitempty"`
	AllowedCIDRs   []string `json:"allowed_cidrs,omitempty"`
	TrustedCAPEM   string   `json:"trusted_ca_pem,omitempty"`
	Algorithm      string   `json:"algorithm"`
	Enabled        bool     `json:"enabled"`
	Revision       int64    `json:"revision,omitempty"`
}

// VerifiedAssertion is an immutable proof of one exact rule revision. Authority
// issuance consumes this value without changing or re-stamping its revision.
type VerifiedAssertion struct {
	ruleID       string
	ruleRevision int64
	issuer       string
	subject      string
	expiresAt    time.Time
}

func (p VerifiedAssertion) RuleID() string       { return p.ruleID }
func (p VerifiedAssertion) RuleRevision() int64  { return p.ruleRevision }
func (p VerifiedAssertion) Issuer() string       { return p.issuer }
func (p VerifiedAssertion) Subject() string      { return p.subject }
func (p VerifiedAssertion) ExpiresAt() time.Time { return p.expiresAt }

// UnavailableError indicates a failed dependency rather than rejected identity.
type UnavailableError struct{}

func (*UnavailableError) Error() string { return "authentication unavailable" }
