package testinfra

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	EnvKeycloakFixture       = "TETRAL_TEST_KEYCLOAK_CONFIG"
	KeycloakRequestTimeout   = 5 * time.Second
	KeycloakReadinessTimeout = 120 * time.Second
	KeycloakProvisionTimeout = 180 * time.Second
	keycloakResponseLimit    = 1 << 20
)

//go:embed keycloak.lock.json
var keycloakLockJSON []byte

//go:embed keycloak-realm.json
var keycloakRealmJSON []byte

type keycloakLock struct {
	Schema        string `json:"schema"`
	Version       string `json:"version"`
	Image         string `json:"image"`
	RealmTemplate string `json:"realm_template"`
}

func loadKeycloakLock() (keycloakLock, error) {
	var lock keycloakLock
	if err := json.Unmarshal(keycloakLockJSON, &lock); err != nil {
		return lock, err
	}
	_, digest, found := strings.Cut(lock.Image, "@sha256:")
	if lock.Schema != "tetral.test-keycloak/v1" || lock.Version == "" || !strings.Contains(lock.Image, ":"+lock.Version+"@sha256:") || !found || len(digest) != 64 || lock.RealmTemplate != "keycloak-realm.json" {
		return lock, errors.New("keycloak fixture lock is invalid")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return lock, errors.New("keycloak fixture digest is invalid")
	}
	return lock, nil
}

func PinnedKeycloakImage() (string, error) {
	lock, err := loadKeycloakLock()
	return lock.Image, err
}

// KeycloakRealmChecksum identifies the public recipe before generated realm
// names and credentials are injected. No credential or token digest is evidence.
func KeycloakRealmChecksum() string {
	digest := sha256.Sum256(keycloakRealmJSON)
	return hex.EncodeToString(digest[:])
}

// KeycloakFixture is a native runner-owned HTTPS dependency. Only credential
// file handles travel to child processes; descriptor contents are private.
type KeycloakFixture struct {
	Endpoint          string   `json:"endpoint"`
	CAPath            string   `json:"ca_path"`
	AllowedCIDRs      []string `json:"allowed_cidrs"`
	AdminUsernamePath string   `json:"admin_username_path"`
	AdminPasswordPath string   `json:"admin_password_path"`
	Container         string   `json:"container"`
	Image             string   `json:"image"`
	Version           string   `json:"version"`
	RealmChecksum     string   `json:"realm_checksum"`
}

func LoadKeycloakFixture() (KeycloakFixture, error) {
	var fixture KeycloakFixture
	name := os.Getenv(EnvKeycloakFixture)
	if name == "" {
		return fixture, errors.New("keycloak fixture is required; run the Affected or Full dependency profile")
	}
	//nolint:gosec // The native runner owns this private descriptor.
	data, err := os.ReadFile(name)
	if err != nil {
		return fixture, errors.New("keycloak fixture descriptor is unavailable")
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		return fixture, errors.New("keycloak fixture descriptor is invalid")
	}
	if err := fixture.validate(); err != nil {
		return fixture, err
	}
	return fixture, nil
}

func (f KeycloakFixture) validate() error {
	u, err := url.Parse(f.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.Path != "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("keycloak fixture HTTPS address is invalid")
	}
	lock, err := loadKeycloakLock()
	if err != nil {
		return err
	}
	if f.CAPath == "" || f.AdminUsernamePath == "" || f.AdminPasswordPath == "" || f.Container == "" || f.Image != lock.Image || f.Version != lock.Version || f.RealmChecksum != KeycloakRealmChecksum() || len(f.AllowedCIDRs) != 1 || f.AllowedCIDRs[0] != "127.0.0.1/32" {
		return errors.New("keycloak fixture identity is incomplete")
	}
	return nil
}

// HTTPClient verifies the fixture's CA and literal IP SAN. It has no proxy,
// rejects redirects, and cannot dial outside its exact configured endpoint.
func (f KeycloakFixture) HTTPClient() (*http.Client, error) {
	if err := f.validate(); err != nil {
		return nil, err
	}
	//nolint:gosec // The descriptor supplies a native runner-owned trust file.
	ca, err := os.ReadFile(f.CAPath)
	if err != nil {
		return nil, errors.New("keycloak fixture CA is unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("keycloak fixture CA is invalid")
	}
	u, _ := url.Parse(f.Endpoint)
	dialer := net.Dialer{Timeout: KeycloakRequestTimeout}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != u.Host {
			return nil, errors.New("keycloak fixture destination is not configured")
		}
		return dialer.DialContext(ctx, network, address)
	}}
	return &http.Client{Transport: transport, Timeout: KeycloakRequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("keycloak fixture redirects are denied") }}, nil
}

func (m *dependencyManager) startKeycloak(ctx context.Context) error {
	ctx, stop := context.WithTimeout(ctx, KeycloakReadinessTimeout)
	defer stop()
	if err := dockerAvailable(ctx); err != nil {
		return err
	}
	if err := cleanupOrphanedDependencyContainers(ctx); err != nil {
		return err
	}
	if err := m.ensureRunID(); err != nil {
		return err
	}
	lock, err := loadKeycloakLock()
	if err != nil {
		return err
	}
	if err := runQuiet(ctx, "docker", "image", "inspect", lock.Image); err != nil {
		if err := runQuiet(ctx, "docker", "pull", lock.Image); err != nil {
			return fmt.Errorf("prepare pinned Keycloak image: %w", err)
		}
	}
	identity, err := dockerImageDigest(ctx, lock.Image)
	if err != nil || !strings.HasSuffix(identity, strings.Split(lock.Image, "@")[1]) {
		return errors.New("keycloak image differs from its immutable lock")
	}
	directory, err := os.MkdirTemp("", "tetral-test-keycloak-")
	if err != nil {
		return err
	}
	m.directories = append(m.directories, directory)
	m.directoryDependencies = append(m.directoryDependencies, "keycloak")
	tlsDirectory := filepath.Join(directory, "tls")
	//nolint:gosec // Private mode0700 parent; mounted TLS directory crosses host/container UIDs.
	if err := os.Mkdir(tlsDirectory, 0755); err != nil {
		return err
	}
	caPath, err := createKeycloakTLS(tlsDirectory)
	if err != nil {
		return err
	}
	username, err := randomIdentity(16)
	if err != nil {
		return err
	}
	password, err := randomIdentity(32)
	if err != nil {
		return err
	}
	f := KeycloakFixture{CAPath: caPath, AllowedCIDRs: []string{"127.0.0.1/32"}, AdminUsernamePath: filepath.Join(directory, "admin.username"), AdminPasswordPath: filepath.Join(directory, "admin.password"), Image: lock.Image, Version: lock.Version, RealmChecksum: KeycloakRealmChecksum()}
	for path, value := range map[string]string{f.AdminUsernamePath: username, f.AdminPasswordPath: password, filepath.Join(directory, "bootstrap.env"): "KC_BOOTSTRAP_ADMIN_USERNAME=" + username + "\nKC_BOOTSTRAP_ADMIN_PASSWORD=" + password + "\n"} {
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			return err
		}
	}
	name, err := dependencyContainerName("keycloak")
	if err != nil {
		return err
	}
	f.Container = name
	ports, release, err := ReserveLoopbackPorts(8443)
	if err != nil {
		return err
	}
	defer release()
	f.Endpoint = "https://127.0.0.1:" + strconv.Itoa(ports[8443])
	arguments := append([]string{"run", "-d", "--name", name}, dependencyContainerLabels("keycloak", m.runID)...)
	arguments = append(arguments, "-p", "127.0.0.1:"+strconv.Itoa(ports[8443])+":8443", "--env-file", filepath.Join(directory, "bootstrap.env"), "-v", tlsDirectory+":/fixture:ro", lock.Image, "start-dev", "--http-enabled=false", "--hostname="+f.Endpoint, "--https-certificate-file=/fixture/server.crt", "--https-certificate-key-file=/fixture/server.key", "--log-level=warn")
	m.containers = append(m.containers, name)
	m.containerDependencies = append(m.containerDependencies, "keycloak")
	ready, cancel := context.WithTimeout(ctx, KeycloakReadinessTimeout)
	defer cancel()
	release()
	if err := runQuiet(ready, "docker", arguments...); err != nil {
		return fmt.Errorf("start Keycloak dependency: %w", err)
	}
	client, err := f.HTTPClient()
	if err != nil {
		return err
	}
	defer client.CloseIdleConnections()
	for {
		var discovery struct {
			Issuer string `json:"issuer"`
		}
		if keycloakHTTP(ready, client, http.MethodGet, f.Endpoint+"/realms/master/.well-known/openid-configuration", "", nil, &discovery) == nil && discovery.Issuer == f.Endpoint+"/realms/master" {
			break
		}
		select {
		case <-ready.Done():
			return errors.New("keycloak HTTPS dependency did not become ready")
		case <-time.After(100 * time.Millisecond):
		}
	}
	version, err := dockerOutput(ready, "exec", name, "/opt/keycloak/bin/kc.sh", "--version")
	if err != nil || !strings.HasPrefix(version, "Keycloak "+lock.Version+"\n") {
		return errors.New("running Keycloak version differs from its lock")
	}
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	path := filepath.Join(directory, "fixture.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	m.environment = append(withoutEnvironmentVariable(m.environment, EnvKeycloakFixture), EnvKeycloakFixture+"="+path)
	m.evidence = append(m.evidence, DependencyEvidence{Name: "keycloak", Source: "runner-container", Identity: identity, Version: version, ConfigurationIdentity: "realm-sha256:" + f.RealmChecksum, RunID: m.runID})
	return nil
}

func createKeycloakTLS(directory string) (string, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Tetral test Keycloak CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return "", err
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", err
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "127.0.0.1"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		return "", err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		return "", err
	}
	// The parent is mode0700. These mounted leaf files must be readable by
	// Keycloak's fixed container UID even when the host runner UID differs.
	for path, block := range map[string]*pem.Block{filepath.Join(directory, "ca.crt"): {Type: "CERTIFICATE", Bytes: caDER}, filepath.Join(directory, "server.crt"): {Type: "CERTIFICATE", Bytes: leafDER}, filepath.Join(directory, "server.key"): {Type: "PRIVATE KEY", Bytes: keyDER}} {
		//nolint:gosec // Private mode0700 ancestor; Keycloak's container UID must read its mounted leaf.
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0644); err != nil {
			return "", err
		}
	}
	return filepath.Join(directory, "ca.crt"), nil
}

// keycloakHTTP intentionally reports no body/URL/headers: errors may contain
// credentials or assertions. Every request has its own clipped five-second cap.
func keycloakHTTP(ctx context.Context, client *http.Client, method, endpoint, bearer string, body []byte, destination any) error {
	ctx, cancel := context.WithTimeout(ctx, KeycloakRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("keycloak fixture request is invalid")
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("keycloak fixture HTTPS request failed")
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, keycloakResponseLimit+1))
	if err != nil || len(data) > keycloakResponseLimit {
		return errors.New("keycloak fixture response exceeds its read bound")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("keycloak fixture request rejected (%d)", response.StatusCode)
	}
	if destination != nil {
		if err := json.Unmarshal(data, destination); err != nil {
			return errors.New("keycloak fixture response is invalid")
		}
	}
	return nil
}

func keycloakToken(ctx context.Context, client *http.Client, endpoint string, values url.Values) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, KeycloakRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return "", errors.New("keycloak token request is invalid")
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.Do(request)
	if err != nil {
		return "", errors.New("keycloak token HTTPS request failed")
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, keycloakResponseLimit+1))
	if err != nil || len(data) > keycloakResponseLimit {
		return "", errors.New("keycloak token response exceeds its read bound")
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("keycloak token request rejected (%d)", response.StatusCode)
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(data, &token); err != nil || token.AccessToken == "" {
		return "", errors.New("keycloak token response is invalid")
	}
	return token.AccessToken, nil
}

func readKeycloakSecret(path string) (string, error) {
	//nolint:gosec // Secrets are private native fixture files, never diagnostic text.
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return "", errors.New("keycloak credential file is unavailable")
	}
	return string(data), nil
}

func (f KeycloakFixture) adminToken(ctx context.Context, client *http.Client) (string, error) {
	username, err := readKeycloakSecret(f.AdminUsernamePath)
	if err != nil {
		return "", err
	}
	password, err := readKeycloakSecret(f.AdminPasswordPath)
	if err != nil {
		return "", err
	}
	return keycloakToken(ctx, client, f.Endpoint+"/realms/master/protocol/openid-connect/token", url.Values{"grant_type": {"password"}, "client_id": {"admin-cli"}, "username": {username}, "password": {password}})
}
