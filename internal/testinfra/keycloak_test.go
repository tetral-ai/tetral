package testinfra

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestKeycloakHTTPSIdentityAndSigningKeyLifecycle(t *testing.T) {
	f, err := LoadKeycloakFixture()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), KeycloakProvisionTimeout)
	defer cancel()
	type provisionResult struct {
		realm *KeycloakRealm
		err   error
	}
	provisioned := make(chan provisionResult, 2)
	for range 2 {
		go func() { realm, err := f.ProvisionRealm(ctx); provisioned <- provisionResult{realm, err} }()
	}
	var realms []*KeycloakRealm
	var provisionErr error
	for range 2 {
		result := <-provisioned
		if result.err != nil {
			provisionErr = errors.Join(provisionErr, result.err)
		}
		if result.realm != nil {
			realms = append(realms, result.realm)
		}
	}
	if provisionErr != nil {
		for _, realm := range realms {
			_ = realm.Close(ctx)
		}
		t.Fatal(provisionErr)
	}
	r, other := realms[0], realms[1]
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*KeycloakRequestTimeout)
		defer cancel()
		if err := other.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	if r.Issuer == other.Issuer || r.HumanSubject == other.HumanSubject || r.ServiceSubject == other.ServiceSubject || r.ID == other.ID {
		t.Fatal("simultaneous consumers share realm identity")
	}
	closed := false
	t.Cleanup(func() {
		if closed {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 2*KeycloakRequestTimeout)
		defer cancel()
		if err := r.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	client, err := f.HTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	var discovery struct {
		Issuer  string `json:"issuer"`
		JWKSURL string `json:"jwks_uri"`
	}
	if err := keycloakHTTP(ctx, client, http.MethodGet, r.Issuer+"/.well-known/openid-configuration", "", nil, &discovery); err != nil {
		t.Fatal(err)
	}
	if discovery.Issuer != r.Issuer || discovery.JWKSURL != r.JWKSURL {
		t.Fatal("actual discovery changed the configured HTTPS issuer or JWKS endpoint")
	}
	// Positive trust control above uses the explicitly supplied CA. System
	// roots alone must not authenticate this isolated private issuer.
	untrusted := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: KeycloakRequestTimeout}
	defer untrusted.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.JWKSURL, nil)
	if err != nil {
		t.Fatal("construct untrusted control")
	}
	if response, err := untrusted.Do(request); err == nil {
		_ = response.Body.Close()
		t.Fatal("fixture HTTPS unexpectedly accepted system roots without its CA")
	}
	human, err := r.HumanAssertion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	service, err := r.ServiceAssertion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	readKeys := func() fixtureJWKS {
		var keys fixtureJWKS
		if err := keycloakHTTP(ctx, client, http.MethodGet, r.JWKSURL, "", nil, &keys); err != nil {
			t.Fatal(err)
		}
		return keys
	}
	oldKeys := readKeys()
	otherAssertion, err := other.HumanAssertion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var otherKeys fixtureJWKS
	if err := keycloakHTTP(ctx, client, http.MethodGet, other.JWKSURL, "", nil, &otherKeys); err != nil {
		t.Fatal(err)
	}
	otherKid, err := verifyKeycloakAssertion(otherAssertion, other.HumanSubject, other.Issuer, other.Audience, otherKeys)
	if err != nil {
		t.Fatal(err)
	}
	oldKid, err := verifyKeycloakAssertion(human, r.HumanSubject, r.Issuer, r.Audience, oldKeys)
	if err != nil {
		t.Fatal(err)
	}
	serviceKid, err := verifyKeycloakAssertion(service, r.ServiceSubject, r.Issuer, r.Audience, oldKeys)
	if err != nil {
		t.Fatal(err)
	}
	if oldKid != serviceKid || r.HumanSubject == r.ServiceSubject || r.ServiceAccountID == r.ServiceSubject {
		t.Fatal("human/service claims or signing identities are not distinct as specified")
	}
	metadata, err := r.SigningKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldComponent := ""
	for _, key := range metadata {
		if key.Kid == oldKid {
			oldComponent = key.ComponentID
		}
	}
	if oldComponent == "" {
		t.Fatal("actual assertion key lacks an admin-controlled provider")
	}
	rotated, err := r.RotateSigningKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.Kid == oldKid || rotated.Status != "ACTIVE" {
		t.Fatal("rotation did not create a fresh active signing key")
	}
	newHuman, err := r.HumanAssertion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	overlap := readKeys()
	newKid, err := verifyKeycloakAssertion(newHuman, r.HumanSubject, r.Issuer, r.Audience, overlap)
	if err != nil || newKid != rotated.Kid {
		t.Fatal("fresh assertion did not use the new real signing key")
	}
	if _, err := verifyKeycloakAssertion(human, r.HumanSubject, r.Issuer, r.Audience, overlap); err != nil {
		t.Fatal("old signing key was lost during overlap")
	}
	if err := r.SetSigningKeyState(ctx, oldComponent, false, true); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyKeycloakAssertion(human, r.HumanSubject, r.Issuer, r.Audience, readKeys()); err != nil {
		t.Fatal("passive enabled key disappeared before retirement")
	}
	if err := r.SetSigningKeyState(ctx, oldComponent, false, false); err != nil {
		t.Fatal(err)
	}
	retired := readKeys()
	if _, err := verifyKeycloakAssertion(human, r.HumanSubject, r.Issuer, r.Audience, retired); !errors.Is(err, errFixtureSigningKeyMissing) {
		t.Fatal("retired signing key remained available in actual JWKS")
	}
	if _, err := verifyKeycloakAssertion(newHuman, r.HumanSubject, r.Issuer, r.Audience, retired); err != nil {
		t.Fatal("retirement removed the current signing key")
	}
	t.Logf("actual signing-key IDs old=%q new=%q; JWKS overlap=%d retirement=%d", oldKid, newKid, len(overlap.Keys), len(retired.Keys))
	if err := keycloakHTTP(ctx, client, http.MethodGet, other.JWKSURL, "", nil, &otherKeys); err != nil {
		t.Fatal(err)
	}
	if kid, err := verifyKeycloakAssertion(otherAssertion, other.HumanSubject, other.Issuer, other.Audience, otherKeys); err != nil || kid != otherKid {
		t.Fatal("one consumer's rotation changed another realm's signing identity")
	}
	credentials := []string{r.HumanPasswordPath, r.ClientSecretPath}
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	closed = true
	for _, path := range credentials {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("realm cleanup retained private test credentials")
		}
	}
	if err := keycloakHTTP(ctx, client, http.MethodGet, r.JWKSURL, "", nil, &fixtureJWKS{}); err == nil {
		t.Fatal("realm cleanup retained the issuer")
	}
	t.Logf("real HTTPS Keycloak %s; realm recipe %s; distinct human/service subjects; signing-key overlap and retirement observed", f.Version, f.RealmChecksum)
}

type fixtureJWKS struct {
	Keys []struct {
		Kid       string `json:"kid"`
		Type      string `json:"kty"`
		Use       string `json:"use"`
		Algorithm string `json:"alg"`
		N         string `json:"n"`
		E         string `json:"e"`
	} `json:"keys"`
}

var errFixtureSigningKeyMissing = errors.New("actual JWKS does not contain the assertion signing key")

// This oracle verifies upstream JWT bytes using public RSA mathematics and
// independently expected claims. It does not reuse the production verifier.
func verifyKeycloakAssertion(assertion, subject, issuer, audience string, keys fixtureJWKS) (string, error) {
	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		return "", errors.New("actual assertion is not a signed JWT")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", errors.New("actual assertion header is invalid")
	}
	var header struct {
		Kid       string `json:"kid"`
		Algorithm string `json:"alg"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil || header.Algorithm != "RS256" || header.Kid == "" {
		return "", errors.New("actual assertion is not the configured RS256 signer")
	}
	var public *rsa.PublicKey
	for _, key := range keys.Keys {
		if key.Kid != header.Kid {
			continue
		}
		if key.Type != "RSA" || key.Use != "sig" || key.Algorithm != "RS256" {
			return "", errors.New("actual assertion signing key type is invalid")
		}
		n, err := base64.RawURLEncoding.DecodeString(key.N)
		if err != nil {
			return "", errors.New("actual public RSA modulus is invalid")
		}
		e, err := base64.RawURLEncoding.DecodeString(key.E)
		if err != nil {
			return "", errors.New("actual public RSA exponent is invalid")
		}
		public = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	}
	if public == nil {
		return "", errFixtureSigningKeyMissing
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", errors.New("actual assertion signature encoding is invalid")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(public, crypto.SHA256, digest[:], signature); err != nil {
		return "", errors.New("actual assertion signature did not verify")
	}
	claimsBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("actual assertion claims encoding is invalid")
	}
	var claims struct {
		Subject  string          `json:"sub"`
		Issuer   string          `json:"iss"`
		Audience json.RawMessage `json:"aud"`
		Expiry   int64           `json:"exp"`
	}
	if err := json.Unmarshal(claimsBytes, &claims); err != nil || claims.Subject != subject || claims.Issuer != issuer || claims.Expiry <= time.Now().Unix()+120 {
		return "", errors.New("actual assertion identity or SDK-compatible lifetime differs from fixture contract")
	}
	var audiences []string
	if err := json.Unmarshal(claims.Audience, &audiences); err != nil {
		var single string
		if err := json.Unmarshal(claims.Audience, &single); err != nil {
			return "", errors.New("actual assertion audience is invalid")
		}
		audiences = []string{single}
	}
	if !contains(audiences, audience) {
		return "", errors.New("actual assertion omits the Engine audience")
	}
	return header.Kid, nil
}

func TestKeycloakDescriptorRejectsUnconfiguredIdentity(t *testing.T) {
	lock, err := loadKeycloakLock()
	if err != nil {
		t.Fatal(err)
	}
	valid := KeycloakFixture{Endpoint: "https://127.0.0.1:8443", CAPath: "ca", AdminUsernamePath: "username", AdminPasswordPath: "password", AllowedCIDRs: []string{"127.0.0.1/32"}, Container: "owned", Image: lock.Image, Version: lock.Version, RealmChecksum: KeycloakRealmChecksum()}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"http://127.0.0.1:8443", "https://localhost:8443", "https://127.0.0.1:8443/path", "https://user:secret@127.0.0.1:8443"} {
		invalid := valid
		invalid.Endpoint = endpoint
		if err := invalid.validate(); err == nil {
			t.Fatal("unconfigured fixture endpoint was accepted")
		}
	}
	invalid := valid
	invalid.AllowedCIDRs = []string{"0.0.0.0/0"}
	if err := invalid.validate(); err == nil {
		t.Fatal("unrestricted fixture egress was accepted")
	}
	invalid = valid
	invalid.Image = "quay.io/keycloak/keycloak:latest"
	if err := invalid.validate(); err == nil {
		t.Fatal("unpinned fixture image was accepted")
	}
}

func TestKeycloakDependencyUsesNativeCustody(t *testing.T) {
	called := 0
	directory := filepath.Join(t.TempDir(), "private")
	starters := dependencyStarters{keycloak: func(ctx context.Context, m *dependencyManager) error {
		called++
		if err := os.Mkdir(directory, 0700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(directory, "test.password"), []byte("ephemeral"), 0600); err != nil {
			return err
		}
		m.directories = append(m.directories, directory)
		m.directoryDependencies = append(m.directoryDependencies, "keycloak")
		m.evidence = append(m.evidence, DependencyEvidence{Name: "keycloak", Identity: "test"})
		return nil
	}}
	m, err := startDependenciesWith(context.Background(), []string{"keycloak"}, nil, starters)
	if err != nil || called != 1 {
		t.Fatal("native dependency starter was not selected")
	}
	if err := m.stopBounded(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("native cleanup retained fixture credentials")
	}
	if len(m.evidence) != 1 || m.evidence[0].SetupElapsed <= 0 || m.evidence[0].TeardownElapsed <= 0 {
		t.Fatal("native fixture evidence omitted lifecycle timing")
	}
	if _, err := startDependenciesWith(context.Background(), []string{"keycloak"}, nil, dependencyStarters{}); err == nil {
		t.Fatal("missing native starter did not fail closed")
	}
}

func TestKeycloakNativeProfilesAndRaceShards(t *testing.T) {
	root := repositoryRootForTest(t)
	const fixtureTest = "TestKeycloakHTTPSIdentityAndSigningKeyLifecycle"
	packages, err := listGoPackages(root)
	if err != nil {
		t.Fatal(err)
	}
	var pkg listedPackage
	for _, candidate := range packages {
		if candidate.ImportPath == "github.com/tetral-ai/tetral/internal/testinfra" {
			pkg = candidate
		}
	}
	fast, exclusions, err := noInfrastructureTests(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if contains(fast, fixtureTest) {
		t.Fatal("Fast executed the real issuer fixture")
	}
	found := false
	for _, exclusion := range exclusions {
		if exclusion.Runnable == fixtureTest {
			found = contains(append([]string{exclusion.Capability}, exclusion.Capabilities...), "keycloak")
		}
	}
	if !found {
		t.Fatal("Fast lost the declared Keycloak requirement")
	}
	full, _, err := fullGoSelections(root, "fixture profile guard")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for index := 0; index < 4; index++ {
		shard, err := SelectPlan(Plan{Profile: ProfileFull, Selections: full}, []string{"go"}, index, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, selection := range shard.Selections {
			if !contains(selection.Tests, fixtureTest) {
				continue
			}
			count++
			if !contains(selection.Dependencies, "keycloak") || !contains(shard.Dependencies, "keycloak") {
				t.Fatal("normal Go Race shard omitted the fixture's native dependency")
			}
		}
	}
	if count != 1 {
		t.Fatal("normal CI shards do not execute the fixture exactly once")
	}
	if err := VerifyPullRequestWorkflow(root); err != nil {
		t.Fatal(err)
	}
}
