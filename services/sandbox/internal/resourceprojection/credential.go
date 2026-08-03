package resourceprojection

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/cloudflare/cloudflare-go/v7"
	"github.com/cloudflare/cloudflare-go/v7/option"
	"github.com/cloudflare/cloudflare-go/v7/r2"
)

// CredentialMintConfig carries the parent R2 credential, which is dual-purpose:
// ParentAPIToken is the Bearer value that authorizes the mint call, and
// ParentAccessKeyID is the S3 access-key-id the minted read-only credential is
// derived from. Only the minted read-only triple (access key, secret, session
// token) is ever injected into the sandbox rclone; the backend copy path
// (CopyObject/HeadObject in internal/blob) keeps using the parent read-write
// key and never the minted triple.
//
// UPDATE-WITH: mount.go (RcloneEnv, which injects the minted triple),
// services/sandbox/daytona_file_resource_materializer.go (backend copy path).
type CredentialMintConfig struct {
	AccountID           string
	Bucket              string
	ParentAccessKeyID   string
	ParentAPIToken      string
	TemporaryCredential temporaryCredentialAPI
}

type CredentialMintRequest struct {
	WorkspaceID string
	SessionID   string
	TTL         time.Duration
	Now         time.Time
}

type CredentialMintResult struct {
	Credential Credential
	ExpiresAt  time.Time
	Prefix     string
}

type Credential struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type CredentialMintError struct {
	Operation string
	Cause     error
}

type temporaryCredentialAPI interface {
	New(context.Context, r2.TemporaryCredentialNewParams, ...option.RequestOption) (*r2.TemporaryCredentialNewResponse, error)
}

type CredentialMinter struct {
	accountID         string
	bucket            string
	parentAccessKeyID string
	api               temporaryCredentialAPI
}

func NewCredentialMinter(config CredentialMintConfig) (*CredentialMinter, error) {
	if config.AccountID == "" {
		return nil, errors.New("r2 account_id is required")
	}
	if config.Bucket == "" {
		return nil, errors.New("r2 bucket is required")
	}
	if config.ParentAccessKeyID == "" {
		return nil, errors.New("r2 parent access key id is required")
	}
	api := config.TemporaryCredential
	if api == nil {
		if config.ParentAPIToken == "" {
			return nil, errors.New("r2 parent api token is required")
		}
		client := cloudflare.NewClient(option.WithAPIToken(config.ParentAPIToken))
		api = client.R2.TemporaryCredentials
	}
	return &CredentialMinter{
		accountID:         config.AccountID,
		bucket:            config.Bucket,
		parentAccessKeyID: config.ParentAccessKeyID,
		api:               api,
	}, nil
}

func (m *CredentialMinter) Mint(ctx context.Context, request CredentialMintRequest) (CredentialMintResult, error) {
	if m == nil || m.api == nil {
		return CredentialMintResult{}, errors.New("resource credential minter is required")
	}
	params, prefix, ttl, err := buildTemporaryCredentialParams(m.accountID, m.bucket, m.parentAccessKeyID, request)
	if err != nil {
		return CredentialMintResult{}, err
	}
	response, err := m.api.New(ctx, params)
	if err != nil {
		return CredentialMintResult{}, &CredentialMintError{Operation: "mint", Cause: err}
	}
	credential, err := credentialFromResponse(response)
	if err != nil {
		return CredentialMintResult{}, err
	}
	now := request.Now
	if now.IsZero() {
		now = storage.Now()
	}
	// The mint response carries no expiry field, so ExpiresAt is computed
	// locally as now()+ttl and is the authoritative value the refresh gate
	// reads. The TTL is always passed explicitly on every mint (see
	// buildTemporaryCredentialParams), never left to a server default.
	return CredentialMintResult{
		Credential: credential,
		ExpiresAt:  now.UTC().Add(ttl),
		Prefix:     prefix,
	}, nil
}

func buildTemporaryCredentialParams(accountID string, bucket string, parentAccessKeyID string, request CredentialMintRequest) (r2.TemporaryCredentialNewParams, string, time.Duration, error) {
	if accountID == "" {
		return r2.TemporaryCredentialNewParams{}, "", 0, errors.New("r2 account_id is required")
	}
	if bucket == "" {
		return r2.TemporaryCredentialNewParams{}, "", 0, errors.New("r2 bucket is required")
	}
	if parentAccessKeyID == "" {
		return r2.TemporaryCredentialNewParams{}, "", 0, errors.New("r2 parent access key id is required")
	}
	if request.TTL <= 0 {
		return r2.TemporaryCredentialNewParams{}, "", 0, errors.New("resource credential ttl must be positive")
	}
	prefix := SessionResourcesPrefix(request.WorkspaceID, request.SessionID)
	if err := assertSessionCredentialPrefixes(request.WorkspaceID, request.SessionID, []string{prefix}); err != nil {
		return r2.TemporaryCredentialNewParams{}, "", 0, err
	}
	params := r2.TemporaryCredentialNewParams{
		AccountID: cloudflare.F(accountID),
		TemporaryCredential: r2.TemporaryCredentialParam{
			Bucket:            cloudflare.F(bucket),
			ParentAccessKeyID: cloudflare.F(parentAccessKeyID),
			Permission:        cloudflare.F(r2.TemporaryCredentialPermissionObjectReadOnly),
			TTLSeconds:        cloudflare.F(request.TTL.Seconds()),
			Prefixes:          cloudflare.F([]string{prefix}),
		},
	}
	return params, prefix, request.TTL, nil
}

// assertSessionCredentialPrefixes: the object-read-only permission alone grants
// bucket-wide LIST, so the single session-resources prefix entry is what
// actually restricts GET and LIST to this session. It requires exactly that one
// non-empty prefix before every mint; without it the credential would grant
// whole-bucket enumeration and read of every workspace's canonical files.
//
// UPDATE-WITH: buildTemporaryCredentialParams (its sole caller, which builds
// the Prefixes from the same SessionResourcesPrefix).
func assertSessionCredentialPrefixes(workspaceID string, sessionID string, prefixes []string) error {
	if workspaceID == "" {
		return errors.New("workspace_id is required")
	}
	if sessionID == "" {
		return errors.New("session_id is required")
	}
	expected := SessionResourcesPrefix(workspaceID, sessionID)
	if len(prefixes) != 1 || prefixes[0] != expected {
		return errors.New("resource credential prefixes must equal the session resources prefix")
	}
	return nil
}

func credentialFromResponse(response *r2.TemporaryCredentialNewResponse) (Credential, error) {
	if response == nil {
		return Credential{}, &CredentialMintError{Operation: "parse", Cause: errors.New("empty credential response")}
	}
	if response.AccessKeyID == "" || response.SecretAccessKey == "" || response.SessionToken == "" {
		return Credential{}, &CredentialMintError{Operation: "parse", Cause: errors.New("credential response is incomplete")}
	}
	return Credential{
		AccessKeyID:     response.AccessKeyID,
		SecretAccessKey: response.SecretAccessKey,
		SessionToken:    response.SessionToken,
	}, nil
}

func (e *CredentialMintError) Error() string {
	if e == nil {
		return ""
	}
	if e.Operation == "" {
		return "resource credential operation failed"
	}
	return "resource credential " + e.Operation + " failed"
}

func (e *CredentialMintError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *CredentialMintError) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, e.Error())
}

func (c Credential) String() string {
	return "resource credential (redacted)"
}

func (c Credential) GoString() string {
	return "resourceprojection.Credential{redacted}"
}

func (c Credential) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, c.String())
}

func (r CredentialMintResult) String() string {
	return "resource credential mint result (redacted)"
}

func (r CredentialMintResult) GoString() string {
	return "resourceprojection.CredentialMintResult{redacted}"
}

func (r CredentialMintResult) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, r.String())
}

func (c CredentialMintConfig) String() string {
	return "resource credential mint config (redacted)"
}

func (c CredentialMintConfig) GoString() string {
	return "resourceprojection.CredentialMintConfig{redacted}"
}

func (c CredentialMintConfig) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, c.String())
}
