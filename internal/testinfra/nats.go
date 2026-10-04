package testinfra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
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
	var fixture NATSFixture
	name := os.Getenv(EnvNATSFixture)
	if name == "" {
		return fixture, errors.New("NATS fixture is required; run the Affected or Full dependency profile")
	}
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
	// Separate roles cannot subscribe/publish respectively. No JetStream state
	// or implicit reply permission is needed for best-effort preview fan-out.
	configuration := fmt.Sprintf("port: 4222\nhttp: 8222\njetstream: false\nmax_payload: 1048576\nauthorization {users:[{user:%q,password:%q,permissions:{publish:[\"preview.v1.>\"],subscribe:{deny:[\">\"]}}},{user:%q,password:%q,permissions:{publish:{deny:[\">\"]},subscribe:[\"preview.v1.>\"]}}]}\n", credentials[0], credentials[1], credentials[2], credentials[3])
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
