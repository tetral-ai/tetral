package testinfra

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// KeycloakRealm owns independent signing keys and test credentials on the
// shared runner dependency. Subjects are immutable IdP IDs, not usernames.
type KeycloakRealm struct {
	Name              string
	ID                string
	Issuer            string
	JWKSURL           string
	CAPath            string
	AllowedCIDRs      []string
	Audience          string
	ClientID          string
	HumanSubject      string
	ServiceSubject    string
	ServiceAccountID  string
	HumanUsername     string
	HumanPasswordPath string
	ClientSecretPath  string
	fixture           KeycloakFixture
	client            *http.Client
	directory         string
	clientInternalID  string
}

func (f KeycloakFixture) ProvisionRealm(ctx context.Context) (_ *KeycloakRealm, resultErr error) {
	ctx, cancel := context.WithTimeout(ctx, KeycloakProvisionTimeout)
	defer cancel()
	client, err := f.HTTPClient()
	if err != nil {
		return nil, err
	}
	suffix, err := randomIdentity(8)
	if err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	directory, err := os.MkdirTemp(filepath.Dir(f.AdminPasswordPath), "realm-")
	if err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	// Keycloak stores user/client IDs globally. Fixed logical names with
	// per-realm immutable IDs let concurrent consumers retain their own keys.
	r := &KeycloakRealm{
		Name: "tetral-" + suffix, fixture: f, client: client, directory: directory,
		CAPath: f.CAPath, AllowedCIDRs: append([]string(nil), f.AllowedCIDRs...),
		Audience: "tetral-engine", ClientID: "tetral-fixture", HumanUsername: "fixture-human",
		HumanSubject: "fixture-human-" + suffix, ServiceSubject: "fixture-service-" + suffix,
		ServiceAccountID:  "fixture-service-account",
		HumanPasswordPath: filepath.Join(directory, "human.password"), ClientSecretPath: filepath.Join(directory, "client.secret"),
		clientInternalID: "fixture-client-" + suffix,
	}
	r.Issuer = f.Endpoint + "/realms/" + r.Name
	r.JWKSURL = r.Issuer + "/protocol/openid-connect/certs"
	created := false
	defer func() {
		if resultErr == nil {
			return
		}
		if created {
			cleanup, cancel := context.WithTimeout(context.Background(), KeycloakRequestTimeout)
			defer cancel()
			_ = r.Close(cleanup)
		} else {
			client.CloseIdleConnections()
			_ = os.RemoveAll(directory)
		}
	}()
	password, err := randomIdentity(32)
	if err != nil {
		return nil, err
	}
	secret, err := randomIdentity(32)
	if err != nil {
		return nil, err
	}
	for path, value := range map[string]string{r.HumanPasswordPath: password, r.ClientSecretPath: secret} {
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			return nil, err
		}
	}
	var configuration map[string]any
	if err := json.Unmarshal(keycloakRealmJSON, &configuration); err != nil {
		return nil, err
	}
	configuration["realm"] = r.Name
	clients := configuration["clients"].([]any)
	clients[0].(map[string]any)["secret"] = secret
	clients[0].(map[string]any)["id"] = r.clientInternalID
	users := configuration["users"].([]any)
	users[0].(map[string]any)["id"] = r.HumanSubject
	users[1].(map[string]any)["id"] = r.ServiceSubject
	credentials := users[0].(map[string]any)["credentials"].([]any)
	credentials[0].(map[string]any)["value"] = password
	body, err := json.Marshal(configuration)
	if err != nil {
		return nil, err
	}
	token, err := f.adminToken(ctx, client)
	if err != nil {
		return nil, err
	}
	// Register cleanup before dispatch: a canceled response can hide a
	// committed realm. Its unique name is still this consumer's responsibility.
	created = true
	if err := keycloakHTTP(ctx, client, http.MethodPost, f.Endpoint+"/admin/realms", token, body, nil); err != nil {
		return nil, err
	}
	var importedRealm struct {
		ID string `json:"id"`
	}
	// URLs address realm names; key-component parents require the immutable
	// realm ID, which Keycloak may generate independently of that name.
	if err := keycloakHTTP(ctx, client, http.MethodGet, r.adminURL(""), token, nil, &importedRealm); err != nil {
		return nil, err
	}
	if importedRealm.ID == "" {
		return nil, errors.New("keycloak imported realm ID is unavailable")
	}
	r.ID = importedRealm.ID
	var serviceUser struct {
		ID string `json:"id"`
	}
	if err := keycloakHTTP(ctx, client, http.MethodGet, r.adminURL("/clients/"+r.clientInternalID+"/service-account-user"), token, nil, &serviceUser); err != nil {
		return nil, err
	}
	if serviceUser.ID != r.ServiceSubject {
		return nil, errors.New("keycloak imported service subject differs from its fixed fixture identity")
	}
	return r, nil
}

func (r *KeycloakRealm) adminURL(path string) string {
	return r.fixture.Endpoint + "/admin/realms/" + url.PathEscape(r.Name) + path
}

func (r *KeycloakRealm) HumanAssertion(ctx context.Context) (string, error) {
	password, err := readKeycloakSecret(r.HumanPasswordPath)
	if err != nil {
		return "", err
	}
	secret, err := readKeycloakSecret(r.ClientSecretPath)
	if err != nil {
		return "", err
	}
	return keycloakToken(ctx, r.client, r.Issuer+"/protocol/openid-connect/token", url.Values{"grant_type": {"password"}, "client_id": {r.ClientID}, "client_secret": {secret}, "username": {r.HumanUsername}, "password": {password}})
}

func (r *KeycloakRealm) ServiceAssertion(ctx context.Context) (string, error) {
	secret, err := readKeycloakSecret(r.ClientSecretPath)
	if err != nil {
		return "", err
	}
	return keycloakToken(ctx, r.client, r.Issuer+"/protocol/openid-connect/token", url.Values{"grant_type": {"client_credentials"}, "client_id": {r.ClientID}, "client_secret": {secret}})
}

type keycloakComponent struct {
	ID           string              `json:"id,omitempty"`
	Name         string              `json:"name"`
	ParentID     string              `json:"parentId"`
	ProviderID   string              `json:"providerId"`
	ProviderType string              `json:"providerType"`
	Config       map[string][]string `json:"config"`
}

type KeycloakSigningKey struct {
	ComponentID string `json:"providerId"`
	Kid         string `json:"kid"`
	Algorithm   string `json:"algorithm"`
	Status      string `json:"status"`
	Type        string `json:"type"`
}

func (r *KeycloakRealm) SigningKeys(ctx context.Context) ([]KeycloakSigningKey, error) {
	token, err := r.fixture.adminToken(ctx, r.client)
	if err != nil {
		return nil, err
	}
	var metadata struct {
		Keys []KeycloakSigningKey `json:"keys"`
	}
	if err := keycloakHTTP(ctx, r.client, http.MethodGet, r.adminURL("/keys"), token, nil, &metadata); err != nil {
		return nil, err
	}
	var keys []KeycloakSigningKey
	for _, key := range metadata.Keys {
		if key.Algorithm == "RS256" && key.Type == "RSA" {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// RotateSigningKey creates a real higher-priority RSA provider. Existing
// providers remain enabled for overlap; retirement is an explicit control.
func (r *KeycloakRealm) RotateSigningKey(ctx context.Context) (KeycloakSigningKey, error) {
	ctx, cancel := context.WithTimeout(ctx, KeycloakProvisionTimeout)
	defer cancel()
	token, err := r.fixture.adminToken(ctx, r.client)
	if err != nil {
		return KeycloakSigningKey{}, err
	}
	var components []keycloakComponent
	if err := keycloakHTTP(ctx, r.client, http.MethodGet, r.adminURL("/components?type=org.keycloak.keys.KeyProvider"), token, nil, &components); err != nil {
		return KeycloakSigningKey{}, err
	}
	priority := 100
	for _, component := range components {
		if values := component.Config["priority"]; len(values) == 1 {
			value, err := strconv.Atoi(values[0])
			if err == nil && value >= priority {
				priority = value + 100
			}
		}
	}
	suffix, err := randomIdentity(8)
	if err != nil {
		return KeycloakSigningKey{}, err
	}
	component := keycloakComponent{Name: "rsa-" + suffix, ParentID: r.ID, ProviderID: "rsa-generated", ProviderType: "org.keycloak.keys.KeyProvider", Config: map[string][]string{"priority": {strconv.Itoa(priority)}, "enabled": {"true"}, "active": {"true"}, "algorithm": {"RS256"}, "keySize": {"2048"}}}
	body, err := json.Marshal(component)
	if err != nil {
		return KeycloakSigningKey{}, err
	}
	if err := keycloakHTTP(ctx, r.client, http.MethodPost, r.adminURL("/components"), token, body, nil); err != nil {
		return KeycloakSigningKey{}, err
	}
	if err := keycloakHTTP(ctx, r.client, http.MethodGet, r.adminURL("/components?name="+component.Name+"&type=org.keycloak.keys.KeyProvider"), token, nil, &components); err != nil {
		return KeycloakSigningKey{}, err
	}
	componentID := ""
	for _, candidate := range components {
		if candidate.Name == component.Name && candidate.ID != "" {
			if componentID != "" {
				return KeycloakSigningKey{}, errors.New("keycloak rotated key provider is ambiguous")
			}
			componentID = candidate.ID
		}
	}
	if componentID == "" {
		return KeycloakSigningKey{}, errors.New("keycloak rotated key provider is unavailable")
	}
	keys, err := r.SigningKeys(ctx)
	if err != nil {
		return KeycloakSigningKey{}, err
	}
	for _, key := range keys {
		if key.ComponentID == componentID && key.Kid != "" {
			return key, nil
		}
	}
	return KeycloakSigningKey{}, errors.New("keycloak rotated signing key is unavailable")
}

// SetSigningKeyState exercises actual admin enable/disable and active controls.
// Disabled old providers disappear from JWKS without replacing their key bytes.
func (r *KeycloakRealm) SetSigningKeyState(ctx context.Context, componentID string, active, enabled bool) error {
	if componentID == "" || strings.ContainsAny(componentID, "/?#") {
		return errors.New("keycloak key provider ID is invalid")
	}
	token, err := r.fixture.adminToken(ctx, r.client)
	if err != nil {
		return err
	}
	var component keycloakComponent
	endpoint := r.adminURL("/components/" + url.PathEscape(componentID))
	if err := keycloakHTTP(ctx, r.client, http.MethodGet, endpoint, token, nil, &component); err != nil {
		return err
	}
	if component.ProviderID != "rsa-generated" || component.Config == nil {
		return errors.New("keycloak fixture key provider is not an RSA signer")
	}
	component.Config["active"] = []string{strconv.FormatBool(active)}
	component.Config["enabled"] = []string{strconv.FormatBool(enabled)}
	body, err := json.Marshal(component)
	if err != nil {
		return err
	}
	return keycloakHTTP(ctx, r.client, http.MethodPut, endpoint, token, body, nil)
}

func (r *KeycloakRealm) Close(ctx context.Context) (resultErr error) {
	defer r.client.CloseIdleConnections()
	defer func() {
		if err := os.RemoveAll(r.directory); err != nil {
			resultErr = errors.Join(resultErr, errors.New("keycloak realm credential cleanup failed"))
		}
	}()
	token, err := r.fixture.adminToken(ctx, r.client)
	if err != nil {
		return err
	}
	return keycloakHTTP(ctx, r.client, http.MethodDelete, r.adminURL(""), token, nil, nil)
}
