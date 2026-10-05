package auth

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"sync"
	"time"
)

const MaxAssertionBytes = 16 * 1024
const issuerKeyTTL = 10 * time.Minute
const issuerUnknownKidCooldown = 30 * time.Second
const maxIssuerCacheEntries = 128

func decodeIssuerJSON(input []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, 0); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("trailing issuer JSON")
	}
	return json.Unmarshal(input, target)
}

type cachedIssuer struct {
	keys         map[string]*rsa.PublicKey
	validUntil   time.Time
	refreshAfter time.Time
	lastForced   time.Time
	lastAttempt  time.Time
	refreshing   chan struct{}
	lastError    error
}

// AssertionVerifier owns only registered issuer proof, never workspace authority.
// The bounded cache is keyed by immutable rule/trust revision. Close cancels and
// joins HTTP refreshes; waiting callers retain their own cancellation boundary.
type AssertionVerifier struct {
	mu        sync.Mutex
	cache     map[issuerRevision]*cachedIssuer
	current   map[string]int64
	ctx       context.Context
	cancel    context.CancelFunc
	work      sync.WaitGroup
	closed    bool
	refreshes int
	now       func() time.Time
	keyTTL    time.Duration
}

type AssertionVerifierConfig struct {
	KeyCacheTTL time.Duration
}

// NewConfiguredAssertionVerifier bounds issuer key validity independently of
// HTTP cache headers. Production defaults to ten minutes.
func NewConfiguredAssertionVerifier(ctx context.Context, config AssertionVerifierConfig) (*AssertionVerifier, error) {
	if config.KeyCacheTTL < time.Second || config.KeyCacheTTL > issuerKeyTTL {
		return nil, &ValidationError{Message: "issuer key cache TTL must be between 1 and 600 seconds"}
	}
	v := NewAssertionVerifier(ctx)
	v.keyTTL = config.KeyCacheTTL
	return v, nil
}

func NewAssertionVerifier(ctx context.Context) *AssertionVerifier {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &AssertionVerifier{cache: make(map[issuerRevision]*cachedIssuer), current: make(map[string]int64), ctx: ctx, cancel: cancel, now: time.Now, keyTTL: issuerKeyTTL}
}
func (v *AssertionVerifier) Close() {
	if v == nil {
		return
	}
	v.mu.Lock()
	v.closed = true
	v.cancel()
	v.mu.Unlock()
	v.work.Wait()
}

func (v *AssertionVerifier) Verify(ctx context.Context, rule FederationRule, assertion string) (VerifiedAssertion, error) {
	invalid := func() (VerifiedAssertion, error) {
		return VerifiedAssertion{}, &AuthenticationError{Message: "invalid assertion"}
	}
	if v == nil {
		return VerifiedAssertion{}, &UnavailableError{}
	}
	if err := ctx.Err(); err != nil {
		return VerifiedAssertion{}, err
	}
	if !rule.Enabled || rule.Revision <= 0 || ValidateFederationRule(rule) != nil || len(assertion) == 0 || len(assertion) > MaxAssertionBytes {
		return invalid()
	}
	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		return invalid()
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return invalid()
	}
	var header map[string]json.RawMessage
	if err := DecodeStrictJSON(headerBytes, &header); err != nil {
		return invalid()
	}
	var algorithm, kid string
	if json.Unmarshal(header["alg"], &algorithm) != nil || algorithm != rule.Algorithm || json.Unmarshal(header["kid"], &kid) != nil || !boundedIdentity(kid) {
		return invalid()
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return invalid()
	}
	var claims map[string]json.RawMessage
	if DecodeStrictJSON(payloadBytes, &claims) != nil {
		return invalid()
	}
	var issuer, subject string
	var expiry int64
	if json.Unmarshal(claims["iss"], &issuer) != nil || issuer != rule.Issuer || json.Unmarshal(claims["sub"], &subject) != nil || !boundedIdentity(subject) || json.Unmarshal(claims["exp"], &expiry) != nil || expiry <= 0 {
		return invalid()
	}
	var audiences []string
	var audience string
	if json.Unmarshal(claims["aud"], &audience) == nil {
		audiences = []string{audience}
	} else if json.Unmarshal(claims["aud"], &audiences) != nil {
		return invalid()
	}
	matched := false
	if len(audiences) > 16 {
		return invalid()
	}
	for _, aud := range audiences {
		if aud == rule.Audience {
			matched = true
		}
	}
	if !matched {
		return invalid()
	}
	now := v.now().UTC()
	expiresAt := time.Unix(expiry, 0)
	if !now.Before(expiresAt) {
		return invalid()
	}
	if raw, exists := claims["nbf"]; exists {
		var nbf int64
		if json.Unmarshal(raw, &nbf) != nil || time.Unix(nbf, 0).After(now.Add(30*time.Second)) {
			return invalid()
		}
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return invalid()
	}
	key, err := v.key(ctx, rule, kid)
	if err != nil {
		return VerifiedAssertion{}, err
	}
	if err := ctx.Err(); err != nil {
		return VerifiedAssertion{}, err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return invalid()
	}
	return VerifiedAssertion{ruleID: rule.ID, ruleRevision: rule.Revision, issuer: issuer, subject: subject, expiresAt: expiresAt}, nil
}

type issuerRevision struct {
	ID       string
	Revision int64
}

func issuerCacheKey(rule FederationRule) issuerRevision {
	return issuerRevision{ID: rule.ID, Revision: rule.Revision}
}
func (v *AssertionVerifier) key(ctx context.Context, rule FederationRule, kid string) (*rsa.PublicKey, error) {
	keyID := issuerCacheKey(rule)
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil, &UnavailableError{}
	}
	// Evict previous revisions immediately. A refresh begun for one revision can
	// never publish into a subsequent rule/trust revision's cache.
	if current, exists := v.current[rule.ID]; exists && current > rule.Revision {
		v.mu.Unlock()
		return nil, &AuthenticationError{Message: "stale federation rule"}
	}
	if v.current[rule.ID] != rule.Revision {
		for old := range v.cache {
			if old.ID == rule.ID {
				delete(v.cache, old)
			}
		}
		if _, registered := v.current[rule.ID]; !registered && len(v.current) >= maxIssuerCacheEntries {
			for id, revision := range v.current {
				active := v.cache[issuerRevision{ID: id, Revision: revision}]
				if active == nil || active.refreshing == nil {
					delete(v.current, id)
					break
				}
			}
			if len(v.current) >= maxIssuerCacheEntries {
				v.mu.Unlock()
				return nil, &UnavailableError{}
			}
		}
		v.current[rule.ID] = rule.Revision
	}
	entry := v.cache[keyID]
	if entry == nil {
		if len(v.cache) >= maxIssuerCacheEntries {
			for id, old := range v.cache {
				if old.refreshing == nil {
					delete(v.cache, id)
					break
				}
			}
		}
		if len(v.cache) >= maxIssuerCacheEntries {
			v.mu.Unlock()
			return nil, &UnavailableError{}
		}
		entry = &cachedIssuer{}
		v.cache[keyID] = entry
	}
	now := v.now()
	known := entry.keys[kid]
	if known != nil && now.Before(entry.refreshAfter) {
		v.mu.Unlock()
		return known, nil
	}
	if entry.refreshing != nil {
		done := entry.refreshing
		v.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-v.ctx.Done():
			return nil, &UnavailableError{}
		case <-done:
		}
		v.mu.Lock()
		key := entry.keys[kid]
		valid := v.now().Before(entry.validUntil)
		lastError := entry.lastError
		v.mu.Unlock()
		if key != nil && valid {
			return key, nil
		}
		if lastError != nil {
			return nil, lastError
		}
		return nil, &AuthenticationError{Message: "invalid assertion"}
	}
	if entry.lastError != nil && now.Before(entry.lastAttempt.Add(issuerUnknownKidCooldown)) {
		lastError := entry.lastError
		valid := now.Before(entry.validUntil)
		v.mu.Unlock()
		if known != nil && valid {
			return known, nil
		}
		return nil, lastError
	}
	if known == nil && now.Before(entry.validUntil) && now.Before(entry.lastForced.Add(issuerUnknownKidCooldown)) {
		lastError := entry.lastError
		v.mu.Unlock()
		if lastError != nil {
			return nil, lastError
		}
		return nil, &AuthenticationError{Message: "invalid assertion"}
	}
	if v.refreshes >= 16 {
		valid := now.Before(entry.validUntil)
		v.mu.Unlock()
		if known != nil && valid {
			return known, nil
		}
		return nil, &UnavailableError{}
	}
	if known == nil && entry.keys != nil {
		entry.lastForced = now
	}
	v.refreshes++
	entry.lastAttempt = now
	entry.refreshing = make(chan struct{})
	done := entry.refreshing
	v.work.Add(1)
	v.mu.Unlock()
	refreshCtx, cancel := context.WithTimeout(ctx, issuerHTTPTimeout)
	stop := context.AfterFunc(v.ctx, cancel)
	keys, err := loadIssuerKeys(refreshCtx, rule)
	// Include post-fetch key validation in the refresh cancellation boundary.
	// Read it before our unconditional cancel, which only releases resources.
	if contextErr := refreshCtx.Err(); contextErr != nil {
		err = contextErr
	}
	stop()
	cancel()
	v.mu.Lock()
	if err == nil && !v.closed && v.current[rule.ID] == rule.Revision && v.cache[keyID] == entry {
		entry.keys = keys
		entry.validUntil = v.now().Add(v.keyTTL)
		entry.refreshAfter = v.now().Add(v.keyTTL * 9 / 10)
	}
	entry.lastError = err
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		entry.lastError = &UnavailableError{}
	}
	v.refreshes--
	entry.refreshing = nil
	close(done)
	key := entry.keys[kid]
	valid := v.now().Before(entry.validUntil)
	v.mu.Unlock()
	v.work.Done()
	if key != nil && valid {
		return key, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, &AuthenticationError{Message: "invalid assertion"}
}

func loadIssuerKeys(ctx context.Context, rule FederationRule) (map[string]*rsa.PublicKey, error) {
	endpoint := rule.JWKSURL
	if endpoint == "" {
		var discovery struct {
			Issuer  string `json:"issuer"`
			JWKSURL string `json:"jwks_uri"`
		}
		if err := fetchIssuerJSON(ctx, rule, rule.DiscoveryURL, &discovery); err != nil {
			return nil, err
		}
		if discovery.Issuer != rule.Issuer {
			return nil, &AuthenticationError{Message: "invalid issuer discovery"}
		}
		endpoint = discovery.JWKSURL
	}
	var document struct {
		Keys []struct {
			KTY string `json:"kty"`
			KID string `json:"kid"`
			Use string `json:"use"`
			Alg string `json:"alg"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := fetchIssuerJSON(ctx, rule, endpoint, &document); err != nil {
		return nil, err
	}
	if len(document.Keys) == 0 || len(document.Keys) > 64 {
		return nil, &AuthenticationError{Message: "invalid issuer keys"}
	}
	keys := make(map[string]*rsa.PublicKey)
	for _, jwk := range document.Keys {
		if jwk.KTY != "RSA" || (jwk.Use != "" && jwk.Use != "sig") || (jwk.Alg != "" && jwk.Alg != "RS256") {
			continue
		}
		if !boundedIdentity(jwk.KID) || keys[jwk.KID] != nil {
			return nil, &AuthenticationError{Message: "invalid issuer keys"}
		}
		modulus, err := base64.RawURLEncoding.DecodeString(jwk.N)
		if err != nil || len(modulus) < 256 || len(modulus) > 1024 {
			return nil, &AuthenticationError{Message: "invalid issuer keys"}
		}
		exponentBytes, err := base64.RawURLEncoding.DecodeString(jwk.E)
		if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 4 {
			return nil, &AuthenticationError{Message: "invalid issuer keys"}
		}
		exponent := new(big.Int).SetBytes(exponentBytes).Int64()
		if exponent < 3 || exponent > 2147483647 || exponent%2 == 0 {
			return nil, &AuthenticationError{Message: "invalid issuer keys"}
		}
		modulusInt := new(big.Int).SetBytes(modulus)
		if modulusInt.BitLen() < 2048 {
			return nil, &AuthenticationError{Message: "invalid issuer keys"}
		}
		keys[jwk.KID] = &rsa.PublicKey{N: modulusInt, E: int(exponent)}
	}
	if len(keys) == 0 {
		return nil, &AuthenticationError{Message: "invalid issuer keys"}
	}
	return keys, nil
}
