package testinfra

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"
)

// EnvNATSFixture names a run-owned descriptor. Credentials stay in files rather
// than test command arguments, URLs or evidence records.
const EnvNATSFixture = "TETRAL_TEST_NATS_CONFIG"

type NATSRole struct {
	UserPath     string    `json:"user_path"`
	PasswordPath string    `json:"password_path"`
	TLS          NATSFiles `json:"tls"`
}

type NATSFiles struct {
	CAPath   string `json:"ca_path,omitempty"`
	CertPath string `json:"cert_path,omitempty"`
	KeyPath  string `json:"key_path,omitempty"`
}

// NATSFixture gives integration owners the real broker identity and the same
// mounted-file interface used by production clients. Container remains stable
// across a stop/start fault; no queued previews are recovered by this fixture.
type NATSFixture struct {
	Servers    []string `json:"servers"`
	Publisher  NATSRole `json:"publisher"`
	Subscriber NATSRole `json:"subscriber"`
	Container  string   `json:"container"`
	Image      string   `json:"image"`
}

func LoadNATSFixture() (NATSFixture, error) {
	name := os.Getenv(EnvNATSFixture)
	if name == "" {
		return NATSFixture{}, errors.New("NATS fixture is required; run the Affected or Full dependency profile")
	}
	return loadNATSFixture(name)
}

func loadNATSFixture(name string) (NATSFixture, error) {
	var fixture NATSFixture
	// The runner owns the fixture path and never publishes its file contents.
	//nolint:gosec
	data, err := os.ReadFile(name)
	if err != nil {
		return fixture, errors.New("NATS fixture descriptor is unavailable")
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		return fixture, errors.New("NATS fixture descriptor is invalid")
	}
	if len(fixture.Servers) == 0 || fixture.Container == "" || fixture.Image == "" {
		return fixture, errors.New("NATS fixture identity is incomplete")
	}
	for _, server := range fixture.Servers {
		parsed, err := url.Parse(server)
		if err != nil || parsed.User != nil || parsed.Hostname() == "" || (parsed.Scheme != "nats" && parsed.Scheme != "tls") {
			return fixture, errors.New("NATS fixture address is invalid")
		}
	}
	for _, role := range []NATSRole{fixture.Publisher, fixture.Subscriber} {
		if role.UserPath == "" || role.PasswordPath == "" {
			return fixture, errors.New("NATS fixture role is incomplete")
		}
	}
	return fixture, nil
}

// NewNATSFixture owns a separate broker for tests that interrupt its lifetime.
// It uses the runner's pinned starter and never changes the shared descriptor or
// process environment. Register close immediately; it removes even a stopped
// broker and its credential directory after all of the test's clients join.
func NewNATSFixture(ctx context.Context, root string) (NATSFixture, func(context.Context) error, error) {
	manager := &dependencyManager{root: root}
	if err := manager.startNATS(ctx); err != nil {
		return NATSFixture{}, nil, errors.Join(err, manager.stopBounded())
	}
	var descriptor string
	for _, variable := range manager.environment {
		if path, ok := strings.CutPrefix(variable, EnvNATSFixture+"="); ok {
			descriptor = path
		}
	}
	fixture, err := loadNATSFixture(descriptor)
	if err != nil {
		return NATSFixture{}, nil, errors.Join(err, manager.stopBounded())
	}
	return fixture, manager.stop, nil
}

// PinnedNATSImage shares the deployment lock with local broker compositions.
func PinnedNATSImage(root string) (string, error) {
	//nolint:gosec // root is the repository-owned source checkout.
	data, err := os.ReadFile(filepath.Join(root, "deploy", "dependencies.lock.json"))
	if err != nil {
		return "", err
	}
	var lock struct {
		Schema string `json:"schema"`
		NATS   struct {
			Images []struct {
				Component, Reference string
				Digest               string `json:"top_level_digest"`
			} `json:"images"`
		} `json:"nats"`
	}
	if err := json.Unmarshal(data, &lock); err != nil {
		return "", err
	}
	if lock.Schema != "tetral.deployment-dependencies/v1" {
		return "", errors.New("unsupported deployment dependency lock")
	}
	var image string
	for _, entry := range lock.NATS.Images {
		if entry.Component != "server" {
			continue
		}
		if image != "" || len(entry.Digest) != len("sha256:")+64 || !strings.HasPrefix(entry.Digest, "sha256:") || !strings.HasSuffix(entry.Reference, "@"+entry.Digest) {
			return "", errors.New("invalid or duplicate pinned NATS image")
		}
		image = entry.Reference
	}
	if image == "" {
		return "", errors.New("pinned NATS image is absent")
	}
	return image, nil
}

// NATSPolicy is the broker policy that local fixtures share with the production
// NATS release: the client roles' subject permissions, the payload ceiling and
// the hardened listener and route TLS options. Fixtures keep their own
// credentials, certificate files, listeners and routes.
type NATSPolicy struct {
	PublisherPermissions  map[string]any
	SubscriberPermissions map[string]any
	MaxPayload            int
	ClientTLS             map[string]any
	RouteTLS              map[string]any
}

// natsReleaseRoles maps each release user placeholder to its fixture role and
// the password placeholder that must accompany it.
var natsReleaseRoles = map[string]struct{ role, password string }{
	"<< $NATS_PUBLISHER_USER >>":  {role: "publisher", password: "<< $NATS_PUBLISHER_PASSWORD >>"},   //nolint:gosec // G101: release placeholder name, not a credential.
	"<< $NATS_SUBSCRIBER_USER >>": {role: "subscriber", password: "<< $NATS_SUBSCRIBER_PASSWORD >>"}, //nolint:gosec // G101: release placeholder name, not a credential.
}

type natsReleaseTLS struct {
	Enabled bool           `yaml:"enabled"`
	Merge   map[string]any `yaml:"merge"`
}

// NATSBrokerPolicy projects the policy from the release values under
// deploy/nats: values.yaml supplies the client users' permissions and
// max_payload, and values-hardened.yaml the client and route TLS options. The
// live ACL and TLS tests therefore run the deployed policy rather than a copy.
// A missing, renamed, duplicated or additional client user, or an absent
// setting, fails the projection instead of starting a broker with another
// policy.
func NATSBrokerPolicy(root string) (NATSPolicy, error) {
	var policy NATSPolicy
	var base struct {
		Config struct {
			Merge struct {
				MaxPayload    int            `yaml:"max_payload"`
				Authorization map[string]any `yaml:"authorization"`
			} `yaml:"merge"`
		} `yaml:"config"`
	}
	if err := readNATSReleaseValues(root, "values.yaml", &base); err != nil {
		return policy, err
	}
	users, ok := base.Config.Merge.Authorization["users"].([]any)
	if !ok || len(base.Config.Merge.Authorization) != 1 || len(users) != len(natsReleaseRoles) {
		return policy, errors.New("NATS release authorization must declare exactly the publisher and subscriber users")
	}
	permissions := map[string]map[string]any{}
	for _, entry := range users {
		user, _ := entry.(map[string]any)
		placeholder, _ := user["user"].(string)
		role, known := natsReleaseRoles[placeholder]
		grants, _ := user["permissions"].(map[string]any)
		if !known || len(user) != 3 || user["password"] != role.password || len(grants) == 0 || permissions[role.role] != nil {
			return policy, errors.New("NATS release user must pair one role's user and password placeholders with its permissions")
		}
		permissions[role.role] = grants
	}
	policy.PublisherPermissions, policy.SubscriberPermissions = permissions["publisher"], permissions["subscriber"]
	if policy.MaxPayload = base.Config.Merge.MaxPayload; policy.MaxPayload <= 0 {
		return policy, errors.New("NATS release max_payload must be a positive byte count")
	}
	var hardened struct {
		Config struct {
			NATS struct {
				TLS natsReleaseTLS `yaml:"tls"`
			} `yaml:"nats"`
			Cluster struct {
				TLS natsReleaseTLS `yaml:"tls"`
			} `yaml:"cluster"`
		} `yaml:"config"`
	}
	if err := readNATSReleaseValues(root, "values-hardened.yaml", &hardened); err != nil {
		return policy, err
	}
	client, route := hardened.Config.NATS.TLS, hardened.Config.Cluster.TLS
	if !client.Enabled || len(client.Merge) == 0 || !route.Enabled || len(route.Merge) == 0 {
		return policy, errors.New("NATS hardened release must enable client and route TLS with explicit options")
	}
	policy.ClientTLS, policy.RouteTLS = client.Merge, route.Merge
	return policy, nil
}

func readNATSReleaseValues(root, name string, destination any) error {
	//nolint:gosec // root is the repository-owned source checkout.
	data, err := os.ReadFile(filepath.Join(root, "deploy", "nats", name))
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(data, destination); err != nil {
		return fmt.Errorf("NATS release %s is invalid: %w", name, err)
	}
	return nil
}

// AuthorizationBlock returns the broker authorization value that grants the
// fixture's role credentials the release's subject permissions.
func (p NATSPolicy) AuthorizationBlock(publisherUser, publisherPassword, subscriberUser, subscriberPassword string) (string, error) {
	return natsConfigValue(map[string]any{"users": []any{
		map[string]any{"user": publisherUser, "password": publisherPassword, "permissions": p.PublisherPermissions},
		map[string]any{"user": subscriberUser, "password": subscriberPassword, "permissions": p.SubscriberPermissions},
	}})
}

// ClientTLSBlock returns the client listener TLS value: the release's options
// with the fixture's mounted trust and certificate files.
func (p NATSPolicy) ClientTLSBlock(files NATSFiles) (string, error) {
	return natsTLSBlock(p.ClientTLS, files)
}

// RouteTLSBlock returns the route TLS value: the release's options with the
// fixture's mounted trust and certificate files.
func (p NATSPolicy) RouteTLSBlock(files NATSFiles) (string, error) {
	return natsTLSBlock(p.RouteTLS, files)
}

func natsTLSBlock(options map[string]any, files NATSFiles) (string, error) {
	block := make(map[string]any, len(options)+3)
	maps.Copy(block, options)
	block["ca_file"], block["cert_file"], block["key_file"] = files.CAPath, files.CertPath, files.KeyPath
	return natsConfigValue(block)
}

// natsConfigValue encodes a value as JSON, which NATS configuration accepts.
// Go's default HTML escaping would replace the subject wildcard ">" with a JSON
// \u escape, so it is disabled to keep wildcards literal for the NATS parser.
func natsConfigValue(value any) (string, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	return strings.TrimSpace(encoded.String()), nil
}

func (m *dependencyManager) startNATS(ctx context.Context) error {
	if err := dockerAvailable(ctx); err != nil {
		return err
	}
	if err := cleanupOrphanedDependencyContainers(ctx); err != nil {
		return err
	}
	if err := m.ensureRunID(); err != nil {
		return err
	}
	image, err := PinnedNATSImage(m.root)
	if err != nil {
		return err
	}
	directory, err := os.MkdirTemp("", "tetral-test-nats-")
	if err != nil {
		return err
	}
	m.directories = append(m.directories, directory)
	m.directoryDependencies = append(m.directoryDependencies, "nats")
	credentials := make([]string, 4)
	for i := range credentials {
		credentials[i], err = randomIdentity(24)
		if err != nil {
			return err
		}
	}
	fixture := NATSFixture{Image: image, Publisher: NATSRole{UserPath: filepath.Join(directory, "publisher.user"), PasswordPath: filepath.Join(directory, "publisher.password")}, Subscriber: NATSRole{UserPath: filepath.Join(directory, "subscriber.user"), PasswordPath: filepath.Join(directory, "subscriber.password")}}
	for i, path := range []string{fixture.Publisher.UserPath, fixture.Publisher.PasswordPath, fixture.Subscriber.UserPath, fixture.Subscriber.PasswordPath} {
		if err := os.WriteFile(path, []byte(credentials[i]), 0600); err != nil {
			return err
		}
	}
	// The role permissions and payload ceiling come from the production
	// release values, so the ACL proofs run the deployed policy. No JetStream
	// state is needed for best-effort preview fan-out.
	policy, err := NATSBrokerPolicy(m.root)
	if err != nil {
		return err
	}
	authorization, err := policy.AuthorizationBlock(credentials[0], credentials[1], credentials[2], credentials[3])
	if err != nil {
		return err
	}
	configuration := fmt.Sprintf("port: 4222\nhttp: 8222\njetstream: false\nmax_payload: %d\nauthorization: %s\n", policy.MaxPayload, authorization)
	if err := os.WriteFile(filepath.Join(directory, "nats.conf"), []byte(configuration), 0600); err != nil {
		return err
	}
	name, err := dependencyContainerName("nats")
	if err != nil {
		return err
	}
	bindings, release, err := ReserveLoopbackPorts(4222, 8222)
	if err != nil {
		return err
	}
	defer release()
	arguments := append([]string{"run", "-d", "--name", name}, dependencyContainerLabels("nats", m.runID)...)
	arguments = append(arguments, "-p", "127.0.0.1:"+strconv.Itoa(bindings[4222])+":4222", "-p", "127.0.0.1:"+strconv.Itoa(bindings[8222])+":8222", "-v", directory+":/fixture:ro", image, "-c", "/fixture/nats.conf")
	// Dispatch can succeed even if the CLI is canceled before its response.
	// Register the reserved run-owned name before dispatch so the bounded
	// cleanup also joins that uncertain startup outcome.
	m.containers = append(m.containers, name)
	m.containerDependencies = append(m.containerDependencies, "nats")
	release()
	if err := runQuiet(ctx, "docker", arguments...); err != nil {
		return fmt.Errorf("start NATS dependency: %w", err)
	}
	port, err := dockerPort(ctx, name, "4222/tcp")
	if err != nil {
		return err
	}
	if port != strconv.Itoa(bindings[4222]) {
		return errors.New("NATS published binding differs from reserved endpoint")
	}
	fixture.Container = name
	fixture.Servers = []string{"nats://127.0.0.1:" + port}
	ready, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	for {
		if err := verifyNATSReady(fixture.Servers[0], credentials); err == nil {
			break
		}
		select {
		case <-ready.Done():
			return errors.New("NATS dependency did not become ready")
		case <-time.After(100 * time.Millisecond):
		}
	}
	data, err := json.Marshal(fixture)
	if err != nil {
		return err
	}
	path := filepath.Join(directory, "fixture.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	m.environment = append(withoutEnvironmentVariable(m.environment, EnvNATSFixture), EnvNATSFixture+"="+path)
	identity, err := dockerImageDigest(ctx, image)
	if err != nil {
		return err
	}
	version, err := dockerOutput(ctx, "exec", name, "nats-server", "--version")
	if err != nil {
		return err
	}
	m.evidence = append(m.evidence, DependencyEvidence{Name: "nats", Source: "runner-container", Identity: identity, Version: version, RunID: m.runID})
	return nil
}

func verifyNATSReady(server string, credentials []string) error {
	consumer, err := nats.Connect(server, nats.UserInfo(credentials[2], credentials[3]), nats.NoReconnect(), nats.Timeout(time.Second))
	if err != nil {
		return err
	}
	defer consumer.Close()
	sub, err := consumer.SubscribeSync("preview.v1.readiness.probe")
	if err != nil {
		return err
	}
	if err := consumer.FlushTimeout(time.Second); err != nil {
		return err
	}
	producer, err := nats.Connect(server, nats.UserInfo(credentials[0], credentials[1]), nats.NoReconnect(), nats.Timeout(time.Second))
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := producer.Publish("preview.v1.readiness.probe", []byte("ready")); err != nil {
		return err
	}
	if err := producer.FlushTimeout(time.Second); err != nil {
		return err
	}
	message, err := sub.NextMsg(time.Second)
	if err != nil {
		return err
	}
	if string(message.Data) != "ready" {
		return errors.New("NATS readiness payload changed")
	}
	return nil
}
