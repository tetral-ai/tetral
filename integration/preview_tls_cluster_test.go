package integration

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/testinfra"
)

const previewPublisherURI = "spiffe://cluster.local/ns/tetral-system/sa/provider-gateway"
const previewSubscriberURI = "spiffe://cluster.local/ns/tetral-system/sa/event-stream"

type previewTLSBroker struct {
	Container                                                    *testinfra.DockerContainer
	Directory, Name, ClientAddress, RouteAddress, MonitorAddress string
	ClientLeaf, RouteLeaf                                        transporttest.Leaf
	HostPorts                                                    map[int]int
	ReleasePorts                                                 func()
}
type previewTLSCluster struct {
	Directory                     string
	Root                          *transporttest.Authority
	Resources                     *testinfra.DockerResources
	Brokers                       []*previewTLSBroker
	Fixture                       testinfra.NATSFixture
	Policy                        testinfra.NATSPolicy
	PublisherLeaf, SubscriberLeaf transporttest.Leaf
	RouteUser, RoutePassword      string
}

func newPreviewTLSCluster(t *testing.T) *previewTLSCluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	c := &previewTLSCluster{Directory: t.TempDir(), Root: transporttest.Must(transporttest.NewAuthority("preview-root-r1"))}
	resources, err := testinfra.NewDockerResources(ctx, "preview-tls")
	if err != nil {
		t.Fatal(err)
	}
	c.Resources = resources
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Resources.Close(cleanup); err != nil {
			t.Errorf("preview TLS cluster joined cleanup: %v", err)
		}
	})
	network, err := resources.Network(ctx)
	if err != nil {
		t.Fatal(err)
	}
	root := transporttest.RepositoryRoot(t)
	image, err := testinfra.PinnedNATSImage(root)
	if err != nil {
		t.Fatal(err)
	}
	if c.Policy, err = testinfra.NATSBrokerPolicy(root); err != nil {
		t.Fatal(err)
	}
	c.Fixture.Image = image
	c.PublisherLeaf = transporttest.Must(c.Root.ValidLeaf("localhost", previewPublisherURI))
	c.SubscriberLeaf = transporttest.Must(c.Root.ValidLeaf("localhost", previewSubscriberURI))
	c.Fixture.Publisher = c.role(t, "publisher", c.PublisherLeaf)
	c.Fixture.Subscriber = c.role(t, "subscriber", c.SubscriberLeaf)
	c.RouteUser, c.RoutePassword = previewRandom(t), previewRandom(t)
	for i := range 3 {
		broker := &previewTLSBroker{Name: fmt.Sprintf("preview-nats-%d", i), Directory: filepath.Join(c.Directory, fmt.Sprintf("broker-%d", i))}
		broker.HostPorts, broker.ReleasePorts, err = testinfra.ReserveLoopbackPorts(4222, 6222, 8222)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(broker.ReleasePorts)
		broker.ClientAddress = net.JoinHostPort("localhost", strconv.Itoa(broker.HostPorts[4222]))
		broker.ClientLeaf = transporttest.Must(c.Root.ValidLeaf("localhost", "spiffe://cluster.local/ns/tetral-system/sa/nats-client-listener"))
		broker.RouteLeaf = transporttest.Must(c.Root.ValidLeaf(broker.Name, "spiffe://cluster.local/ns/tetral-system/sa/nats-route"))
		previewProjectBroker(t, broker, "initial", c.Root.PEM)
		c.Brokers = append(c.Brokers, broker)
	}
	for _, broker := range c.Brokers {
		c.writeConfiguration(t, broker)
		broker.ReleasePorts()
		container, err := resources.Run(ctx, testinfra.ContainerSpec{Image: image, Network: network, Aliases: []string{broker.Name}, Mounts: []testinfra.DockerMount{{Source: broker.Directory, Target: "/fixture", ReadOnly: true}}, Ports: []int{4222, 6222, 8222}, HostPorts: broker.HostPorts, Command: []string{"-c", "/fixture/nats.conf"}})
		if err != nil {
			t.Fatal(err)
		}
		broker.Container = container
		clientPort, err := container.Port(ctx, 4222)
		if err != nil {
			t.Fatal(err)
		}
		if clientPort != strconv.Itoa(broker.HostPorts[4222]) {
			t.Fatal("native broker endpoint differs from reserved binding")
		}
		broker.ClientAddress = net.JoinHostPort("localhost", clientPort)
		broker.RouteAddress = transporttest.Must(container.Address(ctx, 6222))
		broker.MonitorAddress = "http://" + transporttest.Must(container.Address(ctx, 8222))
		c.Fixture.Servers = append(c.Fixture.Servers, "tls://"+broker.ClientAddress)
		previewAwait(t, 10*time.Second, func() bool {
			connection, err := c.connect(broker, c.Fixture.Publisher, c.PublisherLeaf, c.Root.PEM, "localhost")
			if err == nil {
				connection.Close()
				return true
			}
			return false
		})
	}
	c.Fixture.Container = c.Brokers[0].Container.Name
	previewAwait(t, 120*time.Second, func() bool {
		for _, broker := range c.Brokers {
			var route struct {
				NumRoutes int `json:"num_routes"`
			}
			if previewMonitor(broker, "/routez", &route) != nil || route.NumRoutes < 2 {
				return false
			}
		}
		return true
	})
	for _, broker := range c.Brokers {
		var state struct {
			ID        string                     `json:"server_id"`
			Name      string                     `json:"server_name"`
			JetStream map[string]json.RawMessage `json:"jetstream"`
		}
		if err := previewMonitor(broker, "/varz", &state); err != nil {
			t.Fatal(err)
		}
		// v2.15.0 serializes the zero-valued JetStreamVarz as {}. Every
		// active JetStream server populates config and stats in Varz().
		if state.ID == "" || state.Name != broker.Name || len(state.JetStream) != 0 {
			t.Fatal("actual broker identity/Core NATS state differs")
		}
		connection, err := c.connect(broker, c.Fixture.Publisher, c.PublisherLeaf, c.Root.PEM, "localhost")
		if err != nil {
			t.Fatal(err)
		}
		advertised := connection.Servers()
		expected := map[string]*previewTLSBroker{}
		for _, candidate := range c.Brokers {
			expected[candidate.ClientAddress] = candidate
		}
		observed := map[string]bool{}
		for _, address := range advertised {
			destination, err := url.Parse(address)
			if err != nil || expected[destination.Host] == nil {
				connection.Close()
				t.Fatalf("unexpected advertised broker destination %q", address)
			}
			observed[destination.Host] = true
			fresh, err := c.connect(expected[destination.Host], c.Fixture.Publisher, c.PublisherLeaf, c.Root.PEM, destination.Hostname())
			if err != nil {
				connection.Close()
				t.Fatalf("fresh verified handshake to advertised broker %s failed", destination.Host)
			}
			fresh.Close()
		}
		if len(observed) != len(expected) {
			connection.Close()
			t.Fatalf("advertised broker set incomplete: %v", advertised)
		}
		t.Logf("TLS broker name=%s verified_advertised_destinations=%v", broker.Name, advertised)
		enabled, _ := connection.ConnectedServerJetStream()
		connection.Close()
		if enabled {
			t.Fatal("actual broker INFO advertises JetStream enabled")
		}
		t.Logf("TLS broker name=%s server_id=%s image_id=%s client=%s route=%s", broker.Name, state.ID, broker.Container.ImageID, broker.ClientAddress, broker.RouteAddress)
	}
	return c
}

func previewRandom(t *testing.T) string {
	t.Helper()
	data := make([]byte, 24)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(data)
}
func (c *previewTLSCluster) role(t *testing.T, name string, leaf transporttest.Leaf) testinfra.NATSRole {
	t.Helper()
	directory := filepath.Join(c.Directory, name)
	if err := transporttest.Project(directory, "initial", map[string][]byte{"ca.crt": c.Root.PEM, "tls.crt": leaf.Certificate, "tls.key": leaf.Key, "user": []byte(previewRandom(t)), "password": []byte(previewRandom(t))}); err != nil {
		t.Fatal(err)
	}
	return testinfra.NATSRole{UserPath: filepath.Join(directory, "user"), PasswordPath: filepath.Join(directory, "password"), TLS: testinfra.NATSFiles{CAPath: filepath.Join(directory, "ca.crt"), CertPath: filepath.Join(directory, "tls.crt"), KeyPath: filepath.Join(directory, "tls.key")}}
}
func previewProjectBroker(t *testing.T, b *previewTLSBroker, generation string, roots []byte) {
	t.Helper()
	if err := transporttest.Project(filepath.Join(b.Directory, "tls"), generation, map[string][]byte{"ca.crt": roots, "client.crt": b.ClientLeaf.Certificate, "client.key": b.ClientLeaf.Key, "route.crt": b.RouteLeaf.Certificate, "route.key": b.RouteLeaf.Key}); err != nil {
		t.Fatal(err)
	}
}
func (c *previewTLSCluster) writeConfiguration(t *testing.T, b *previewTLSBroker) {
	t.Helper()
	user := func(path string) string {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal("fixture credential unavailable")
		}
		return string(data)
	}
	var routes []string
	for _, peer := range c.Brokers {
		if peer != b {
			routes = append(routes, fmt.Sprintf("%q", "tls://"+c.RouteUser+":"+c.RoutePassword+"@"+peer.Name+":6222"))
		}
	}
	advertise := b.ClientAddress
	if advertise == "" {
		t.Fatal("broker must reserve its final advertised endpoint before startup")
	}
	// Client authorization, max_payload and the listener and route TLS options
	// are projected from the production release values. Broker names,
	// listeners, routes, credentials and certificate files stay fixture-owned.
	authorization, err := c.Policy.AuthorizationBlock(user(c.Fixture.Publisher.UserPath), user(c.Fixture.Publisher.PasswordPath), user(c.Fixture.Subscriber.UserPath), user(c.Fixture.Subscriber.PasswordPath))
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := c.Policy.ClientTLSBlock(testinfra.NATSFiles{CAPath: "/fixture/tls/ca.crt", CertPath: "/fixture/tls/client.crt", KeyPath: "/fixture/tls/client.key"})
	if err != nil {
		t.Fatal(err)
	}
	routeTLS, err := c.Policy.RouteTLSBlock(testinfra.NATSFiles{CAPath: "/fixture/tls/ca.crt", CertPath: "/fixture/tls/route.crt", KeyPath: "/fixture/tls/route.key"})
	if err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf("server_name:%q\nport:4222\nhttp:8222\njetstream:false\nclient_advertise:%q\nmax_payload:%d\ntls:%s\nauthorization:%s\ncluster {name:\"preview-tls\",port:6222,no_advertise:false,pool_size:-1,authorization:{user:%q,password:%q},routes:[%s],tls:%s}\n", b.Name, advertise, c.Policy.MaxPayload, clientTLS, authorization, c.RouteUser, c.RoutePassword, strings.Join(routes, ","), routeTLS)
	if err := os.WriteFile(filepath.Join(b.Directory, "nats.conf"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
}
func (c *previewTLSCluster) connect(b *previewTLSBroker, role testinfra.NATSRole, leaf transporttest.Leaf, roots []byte, dns string) (*nats.Conn, error) {
	u, err := os.ReadFile(role.UserPath)
	if err != nil {
		return nil, err
	}
	p, err := os.ReadFile(role.PasswordPath)
	if err != nil {
		return nil, err
	}
	config, err := previewTLSConfig(roots, leaf, dns)
	if err != nil {
		return nil, err
	}
	return nats.Connect("tls://"+b.ClientAddress, nats.UserInfo(string(u), string(p)), nats.Secure(config), nats.TLSHandshakeFirst(), nats.NoReconnect(), nats.Timeout(time.Second))
}
func previewTLSConfig(roots []byte, leaf transporttest.Leaf, dns string) (*tls.Config, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(roots) {
		return nil, fmt.Errorf("fixture trust unavailable")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: dns, SessionTicketsDisabled: true}
	if len(leaf.Certificate) > 0 {
		pair, err := tls.X509KeyPair(leaf.Certificate, leaf.Key)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{pair}
	}
	return config, nil
}
func previewMonitor(b *previewTLSBroker, path string, result any) error {
	client := http.Client{Timeout: time.Second}
	response, err := client.Get(b.MonitorAddress + path)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	return json.NewDecoder(response.Body).Decode(result)
}
func previewAwait(t *testing.T, bound time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.NewTimer(bound)
	defer deadline.Stop()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for {
		if predicate() {
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatal("preview fixture canceled")
		case <-deadline.C:
			t.Fatal("preview fixture required observation timed out")
		case <-poll.C:
		}
	}
}
