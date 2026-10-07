package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// InternalPrincipalAudience is the only audience public services accept.
const InternalPrincipalAudience = "tetral-public-api"

// MaxInternalPrincipalBytes bounds a compact signed token before any decoding
// or signature work, so oversized input costs no parsing.
const MaxInternalPrincipalBytes = 32 * 1024

// MaxInternalPrincipalTTL is the longest lifetime (exp minus iat) a signer may
// mint and a verifier accepts.
const MaxInternalPrincipalTTL = 5 * time.Minute

// Signed request metadata bounds shared by MintWithRequestMetadata and Verify.
const (
	maxInternalPrincipalMethodBytes       = 16
	maxInternalPrincipalPathBytes         = 4096
	maxInternalPrincipalRequestIDBytes    = 256
	maxInternalPrincipalForwardedForBytes = 1024
	// internalPrincipalIssuedAtSkew tolerates clock skew between Auth and the
	// receiving service: iat may be at most this far ahead of the verifier clock.
	internalPrincipalIssuedAtSkew = 5 * time.Second
)

// InternalPrincipalClaims is the Auth-signed request admission snapshot. aud
// must equal InternalPrincipalAudience. method and path bind the token to the
// exact original request. iat and exp bound its lifetime to at most
// MaxInternalPrincipalTTL. jti is a bounded unique token ID. request_id is the
// edge-generated request ID, which receivers record only for diagnostic
// correlation (see WithVerifiedEdgeRequestID); forwarded_for is bounded audit
// metadata with no current reader. The workspace, credential,
// identity and authority fields form the discriminated principal union that
// Principal.Validate checks.
type InternalPrincipalClaims struct {
	WorkspaceID  string     `json:"workspace_id"`
	APIKeyID     string     `json:"api_key_id,omitempty"`
	Identity     *Identity  `json:"identity,omitempty"`
	Credential   Credential `json:"credential"`
	Authority    Authority  `json:"authority"`
	Audience     string     `json:"aud"`
	Method       string     `json:"method"`
	Path         string     `json:"path"`
	IssuedAt     string     `json:"iat"`
	ExpiresAt    string     `json:"exp"`
	TokenID      string     `json:"jti"`
	RequestID    string     `json:"request_id"`
	ForwardedFor string     `json:"forwarded_for,omitempty"`
}

type InternalPrincipalSigner struct {
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
	now        func() time.Time
}

type InternalPrincipalVerifier struct {
	publicKey ed25519.PublicKey
	now       func() time.Time
}

func NewInternalPrincipalSigner(privateKey ed25519.PrivateKey) (*InternalPrincipalSigner, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, &ValidationError{Message: "internal principal private key must be an Ed25519 private key"}
	}
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok {
		return nil, &ValidationError{Message: "internal principal public key derivation failed"}
	}
	return &InternalPrincipalSigner{privateKey: privateKey, publicKey: publicKey, now: time.Now}, nil
}

func NewInternalPrincipalSignerFromBase64(raw string) (*InternalPrincipalSigner, error) {
	privateKey, err := DecodeEd25519PrivateKey(raw)
	if err != nil {
		return nil, err
	}
	return NewInternalPrincipalSigner(privateKey)
}

func NewInternalPrincipalVerifier(publicKey ed25519.PublicKey) (*InternalPrincipalVerifier, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return nil, &ValidationError{Message: "internal principal public key must be an Ed25519 public key"}
	}
	return &InternalPrincipalVerifier{publicKey: publicKey, now: time.Now}, nil
}

func NewInternalPrincipalVerifierFromBase64(raw string) (*InternalPrincipalVerifier, error) {
	publicKey, err := DecodeEd25519PublicKey(raw)
	if err != nil {
		return nil, err
	}
	return NewInternalPrincipalVerifier(publicKey)
}

func DecodeEd25519PrivateKey(raw string) (ed25519.PrivateKey, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(strings.TrimSpace(raw))
	}
	if err != nil {
		return nil, &ValidationError{Message: "internal principal private key must be base64"}
	}
	switch len(decoded) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(decoded), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(decoded), nil
	default:
		return nil, &ValidationError{Message: "internal principal private key has invalid length"}
	}
}

func DecodeEd25519PublicKey(raw string) (ed25519.PublicKey, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(strings.TrimSpace(raw))
	}
	if err != nil {
		return nil, &ValidationError{Message: "internal principal public key must be base64"}
	}
	if len(decoded) != ed25519.PublicKeySize {
		return nil, &ValidationError{Message: "internal principal public key has invalid length"}
	}
	return ed25519.PublicKey(decoded), nil
}

func GenerateEd25519PrivateKeyBase64() (string, error) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(privateKey), nil
}

func (s *InternalPrincipalSigner) PublicKeyBase64() string {
	if s == nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(s.publicKey)
}

func (s *InternalPrincipalSigner) SignCursor(payload any) (string, error) {
	if s == nil {
		return "", &AuthenticationError{Message: "internal principal signing unavailable"}
	}
	return signCompactJSON(s.privateKey, "tetral-cursor", payload)
}

func (s *InternalPrincipalSigner) VerifyCursor(token string, payload any) error {
	if s == nil {
		return &AuthenticationError{Message: "internal principal signing unavailable"}
	}
	return verifyCompactJSON(s.publicKey, token, "tetral-cursor", payload)
}

func (s *InternalPrincipalSigner) Mint(principal Principal, method string, path string, requestID string, ttl time.Duration) (string, error) {
	return s.MintWithRequestMetadata(principal, method, path, requestID, "", ttl)
}

func (s *InternalPrincipalSigner) MintWithRequestMetadata(principal Principal, method string, path string, requestID string, forwardedFor string, ttl time.Duration) (string, error) {
	if s == nil {
		return "", &AuthenticationError{Message: "internal principal signing unavailable"}
	}
	if err := principal.Validate(); err != nil {
		return "", err
	}
	if method == "" || len(method) > maxInternalPrincipalMethodBytes || path == "" || len(path) > maxInternalPrincipalPathBytes || len(requestID) > maxInternalPrincipalRequestIDBytes || len(forwardedFor) > maxInternalPrincipalForwardedForBytes {
		return "", &ValidationError{Message: "invalid request metadata"}
	}
	if ttl <= 0 || ttl > MaxInternalPrincipalTTL {
		return "", &ValidationError{Message: "internal principal ttl must be positive and at most five minutes"}
	}
	now := s.now().UTC()
	claims := InternalPrincipalClaims{
		WorkspaceID: string(principal.Workspace.ID),
		APIKeyID:    principal.APIKeyID,
		Identity:    principal.Identity, Credential: principal.Credential, Authority: principal.Authority,
		Audience:     InternalPrincipalAudience,
		Method:       method,
		Path:         path,
		IssuedAt:     now.Format(time.RFC3339),
		ExpiresAt:    now.Add(ttl).Format(time.RFC3339),
		TokenID:      id.New("itok_"),
		RequestID:    requestID,
		ForwardedFor: forwardedFor,
	}
	return signCompactJSON(s.privateKey, "tetral-internal-principal", claims)
}

// Verify applies InternalPrincipalVerifier.Verify with the signer's own public
// key.
func (s *InternalPrincipalSigner) Verify(token string, method string, path string) (Principal, InternalPrincipalClaims, error) {
	if s == nil {
		return Principal{}, InternalPrincipalClaims{}, &AuthenticationError{Message: "internal principal verification unavailable"}
	}
	verifier := &InternalPrincipalVerifier{publicKey: s.publicKey, now: s.now}
	return verifier.Verify(token, method, path)
}

// Verify admits a signed principal only for the exact request it was minted
// for. It requires a token of at most MaxInternalPrincipalBytes; an
// EdDSA header with the internal-principal typ; a valid signature, checked
// before the payload is decoded; strict JSON for header and payload; the
// public-service audience; the exact method and path; an exp in the future; an
// iat at most internalPrincipalIssuedAtSkew ahead of the verifier clock; exp
// after iat with a lifetime of at most MaxInternalPrincipalTTL; bounded method,
// path, request ID, forwarded-for and jti values; and a principal union that
// passes Principal.Validate.
func (v *InternalPrincipalVerifier) Verify(token string, method string, path string) (Principal, InternalPrincipalClaims, error) {
	if v == nil {
		return Principal{}, InternalPrincipalClaims{}, &AuthenticationError{Message: "internal principal verification unavailable"}
	}
	var claims InternalPrincipalClaims
	if err := verifyCompactJSON(v.publicKey, token, "tetral-internal-principal", &claims); err != nil {
		return Principal{}, InternalPrincipalClaims{}, err
	}
	if claims.Audience != InternalPrincipalAudience {
		return Principal{}, InternalPrincipalClaims{}, &AuthenticationError{Message: "invalid internal principal"}
	}
	if claims.Method != method || claims.Path != path {
		return Principal{}, InternalPrincipalClaims{}, &AuthenticationError{Message: "invalid internal principal"}
	}
	expiresAt, err := time.Parse(time.RFC3339, claims.ExpiresAt)
	if err != nil || !v.now().UTC().Before(expiresAt) {
		return Principal{}, InternalPrincipalClaims{}, &AuthenticationError{Message: "internal principal expired"}
	}
	issuedAt, issuedErr := time.Parse(time.RFC3339, claims.IssuedAt)
	if issuedErr != nil || issuedAt.After(v.now().UTC().Add(internalPrincipalIssuedAtSkew)) || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > MaxInternalPrincipalTTL || claims.Method == "" || len(claims.Method) > maxInternalPrincipalMethodBytes || len(claims.Path) > maxInternalPrincipalPathBytes || claims.Path == "" || len(claims.RequestID) > maxInternalPrincipalRequestIDBytes || len(claims.ForwardedFor) > maxInternalPrincipalForwardedForBytes || !boundedIdentity(claims.TokenID) {
		return Principal{}, InternalPrincipalClaims{}, &AuthenticationError{Message: "invalid internal principal"}
	}
	principal := Principal{
		Workspace: workspace.Workspace{ID: workspace.ID(claims.WorkspaceID), Type: "workspace"},
		APIKeyID:  claims.APIKeyID,
		Identity:  claims.Identity, Credential: claims.Credential, Authority: claims.Authority,
	}
	if err := principal.Validate(); err != nil {
		return Principal{}, InternalPrincipalClaims{}, err
	}
	return principal, claims, nil
}

func signCompactJSON(privateKey ed25519.PrivateKey, tokenType string, payload any) (string, error) {
	header := map[string]string{"alg": "EdDSA", "typ": tokenType}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(headerJSON)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payloadJSON)
	signingInput := encodedHeader + "." + encodedPayload
	signature := ed25519.Sign(privateKey, []byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// verifyCompactJSON bounds the token size before decoding anything and verifies
// the Ed25519 signature before decoding the payload, so unauthenticated bytes
// never reach the payload decoder. Header and payload use DecodeStrictJSON,
// which rejects duplicate fields, excessive nesting and trailing values (and
// unknown payload fields) rather than relying on last-wins parsing.
func verifyCompactJSON(publicKey ed25519.PublicKey, token string, tokenType string, payload any) error {
	if len(token) > MaxInternalPrincipalBytes {
		return &AuthenticationError{Message: "invalid internal principal"}
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return &AuthenticationError{Message: "invalid internal principal"}
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return &AuthenticationError{Message: "invalid internal principal"}
	}
	var header map[string]string
	if err := DecodeStrictJSON(headerJSON, &header); err != nil {
		return &AuthenticationError{Message: "invalid internal principal"}
	}
	if header["alg"] != "EdDSA" || header["typ"] != tokenType {
		return &AuthenticationError{Message: "invalid internal principal"}
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return &AuthenticationError{Message: "invalid internal principal"}
	}
	signingInput := parts[0] + "." + parts[1]
	if !ed25519.Verify(publicKey, []byte(signingInput), signature) {
		return &AuthenticationError{Message: "invalid internal principal"}
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return &AuthenticationError{Message: "invalid internal principal"}
	}
	if err := DecodeStrictJSON(payloadJSON, payload); err != nil {
		return &AuthenticationError{Message: "invalid internal principal"}
	}
	return nil
}
