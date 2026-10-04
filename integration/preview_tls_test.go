package integration

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/testinfra"
)

func TestPostgreSQLPreviewTLSLifecycle(t *testing.T) {
	cluster := newPreviewTLSCluster(t)
	t.Run("authorized-cross-route-and-rejected-peers", func(t *testing.T) {
		previewTLSControl(t, cluster)
		wrong := transporttest.Must(transporttest.NewAuthority("untrusted-preview-root"))
		expired := transporttest.Must(cluster.Root.Issue("localhost", previewPublisherURI, time.Now().Add(-2*time.Minute), time.Now().Add(-time.Minute)))
		for _, test := range []struct {
			name  string
			leaf  transporttest.Leaf
			roots []byte
			dns   string
			role  testinfra.NATSRole
		}{
			{"untrusted-issuer", transporttest.Must(wrong.ValidLeaf("localhost", previewPublisherURI)), cluster.Root.PEM, "localhost", cluster.Fixture.Publisher},
			{"untrusted-broker", cluster.PublisherLeaf, wrong.PEM, "localhost", cluster.Fixture.Publisher},
			{"expired-client", expired, cluster.Root.PEM, "localhost", cluster.Fixture.Publisher},
			{"wrong-broker-name", cluster.PublisherLeaf, cluster.Root.PEM, "wrong.preview.test", cluster.Fixture.Publisher},
			{"absent-client-leaf", transporttest.Leaf{}, cluster.Root.PEM, "localhost", cluster.Fixture.Publisher},
		} {
			t.Run(test.name, func(t *testing.T) {
				if connection, err := cluster.connect(cluster.Brokers[0], test.role, test.leaf, test.roots, test.dns); err == nil {
					connection.Close()
					t.Fatal("protected broker admitted denied peer")
				}
				previewTLSControl(t, cluster)
			})
		}
		t.Run("wrong-subject-credentials", func(t *testing.T) {
			bad := cluster.Fixture.Publisher
			bad.PasswordPath = filepath.Join(t.TempDir(), "wrong-password")
			if err := os.WriteFile(bad.PasswordPath, []byte("wrong-fixture-password"), 0600); err != nil {
				t.Fatal(err)
			}
			connection, err := cluster.connect(cluster.Brokers[0], bad, cluster.PublisherLeaf, cluster.Root.PEM, "localhost")
			if err == nil {
				connection.Close()
				t.Fatal("valid client leaf bypassed subject credentials")
			}
			previewTLSControl(t, cluster)
		})
		t.Run("no-plaintext-listener", func(t *testing.T) {
			connection, err := nats.Connect("nats://"+cluster.Brokers[0].ClientAddress, nats.NoReconnect(), nats.Timeout(200*time.Millisecond))
			if err == nil {
				connection.Close()
				t.Fatal("TLS-first broker accepted plaintext")
			}
			previewTLSControl(t, cluster)
		})
		t.Run("obsolete-tls-protocol", func(t *testing.T) {
			config, err := previewTLSConfig(cluster.Root.PEM, cluster.PublisherLeaf, "localhost")
			if err != nil {
				t.Fatal(err)
			}
			// Only this rejected-peer probe offers protocols below the required
			// native TLS 1.2 minimum. Production clients never lower the floor.
			config.MinVersion, config.MaxVersion = tls.VersionTLS10, tls.VersionTLS11
			user := transporttest.Must(os.ReadFile(cluster.Fixture.Publisher.UserPath))
			password := transporttest.Must(os.ReadFile(cluster.Fixture.Publisher.PasswordPath))
			connection, err := nats.Connect(cluster.Fixture.Servers[0], nats.UserInfo(string(user), string(password)), nats.Secure(config), nats.TLSHandshakeFirst(), nats.NoReconnect(), nats.Timeout(time.Second))
			if err == nil {
				connection.Close()
				t.Fatal("protected broker accepted an obsolete TLS protocol")
			}
			previewTLSControl(t, cluster)
		})
		for _, wrong := range []bool{false, true} {
			t.Run(fmt.Sprintf("route-credential-denial-%t", wrong), func(t *testing.T) { previewDeniedRoute(t, cluster, wrong); previewTLSControl(t, cluster) })
		}
	})
	t.Run("held-model-leaf-reload-and-malformed-generation", func(t *testing.T) {
		reloads := previewObserveNativeReloads(t)
		h := previewTLSHarness(t, cluster)
		h.open(t, "tls-on", []string{"agent.message"}, "")
		h.open(t, "tls-off", nil, "")
		h.send(t)
		h.waitFragments(t)
		h.releaseFragments(t, 1)
		h.waitEvent(t, "tls-on", "event_delta", 1)
		for i, broker := range cluster.Brokers {
			old := broker.ClientLeaf.Parsed.SerialNumber.String()
			broker.ClientLeaf = transporttest.Must(cluster.Root.ValidLeaf("localhost", "spiffe://cluster.local/ns/tetral-system/sa/nats-client-listener"))
			broker.RouteLeaf = transporttest.Must(cluster.Root.ValidLeaf(broker.Name, "spiffe://cluster.local/ns/tetral-system/sa/nats-route"))
			previewProjectBroker(t, broker, fmt.Sprintf("renewed-%d", i), cluster.Root.PEM)
			if err := broker.Container.Signal(t.Context(), "HUP"); err != nil {
				t.Fatal(err)
			}
			previewAwaitReload(t, "broker-leaf-renewal", func() bool {
				return previewServedSerial(broker, cluster.Root.PEM, cluster.PublisherLeaf) == broker.ClientLeaf.Parsed.SerialNumber.String() && previewServedRouteSerial(broker, cluster.Root.PEM, cluster.PublisherLeaf) == broker.RouteLeaf.Parsed.SerialNumber.String()
			})
			if previewServedSerial(broker, cluster.Root.PEM, cluster.PublisherLeaf) == old {
				t.Fatal("server did not activate renewed leaf")
			}
			previewTLSControl(t, cluster)
		}
		broker := cluster.Brokers[0]
		lastGood := broker.ClientLeaf.Parsed.SerialNumber.String()
		if err := transporttest.Project(filepath.Join(broker.Directory, "tls"), "malformed", map[string][]byte{"ca.crt": cluster.Root.PEM, "client.crt": broker.ClientLeaf.Certificate, "client.key": []byte("malformed fixture key"), "route.crt": broker.RouteLeaf.Certificate, "route.key": broker.RouteLeaf.Key}); err != nil {
			t.Fatal(err)
		}
		if err := broker.Container.Signal(t.Context(), "HUP"); err != nil {
			t.Fatal(err)
		}
		// The signal is observed through the server's actual reload rejection,
		// then a fresh handshake proves last-known-good material still serves.
		previewAwaitReload(t, "broker-malformed-rejection", func() bool {
			logs, err := broker.Container.Logs(t.Context())
			return err == nil && strings.Contains(logs, "Failed to reload server configuration")
		})
		if previewServedSerial(broker, cluster.Root.PEM, cluster.PublisherLeaf) != lastGood {
			t.Fatal("malformed update replaced valid served material")
		}
		previewProjectBroker(t, broker, "restored", cluster.Root.PEM)
		if err := broker.Container.Signal(t.Context(), "HUP"); err != nil {
			t.Fatal(err)
		}
		previewTLSControl(t, cluster)
		before := previewRequireRoleConnections(t, cluster)
		if !previewBothRolesConnected(before) {
			t.Fatalf("malformed-client baseline requires one owned connection per role: %v", before)
		}
		// Restore mounts even if the strict identity assertion aborts this
		// subtest, so later trust tests inspect their own intended generations.
		for _, role := range []struct {
			config testinfra.NATSRole
			leaf   transporttest.Leaf
		}{{cluster.Fixture.Publisher, cluster.PublisherLeaf}, {cluster.Fixture.Subscriber, cluster.SubscriberLeaf}} {
			t.Cleanup(func() { previewProjectRole(t, role.config, "cleanup-client", cluster.Root.PEM, role.leaf) })
		}
		malformedPublisher, malformedSubscriber := cluster.PublisherLeaf, cluster.SubscriberLeaf
		malformedPublisher.Key, malformedSubscriber.Key = []byte("invalid mounted key"), []byte("invalid mounted key")
		previewProjectRole(t, cluster.Fixture.Publisher, "malformed-client", cluster.Root.PEM, malformedPublisher)
		previewProjectRole(t, cluster.Fixture.Subscriber, "malformed-client", cluster.Root.PEM, malformedSubscriber)
		previewAwaitReload(t, "client-malformed-rejection", func() bool {
			return reloads.Load() > 0 && previewGatewayOutcome(t, h, "preview.credential_reload_failed", "invalid_generation")
		})
		after := previewRequireRoleConnections(t, cluster)
		t.Logf("malformed-client owned connection identities before=%v after=%v", before, after)
		if !reflect.DeepEqual(before, after) {
			t.Fatal("malformed client generation retired still-valid last-known-good connections")
		}
		h.releaseFragments(t, 1)
		h.waitEvent(t, "tls-on", "event_delta", 2)
		previewProjectRole(t, cluster.Fixture.Publisher, "restored-client", cluster.Root.PEM, cluster.PublisherLeaf)
		previewProjectRole(t, cluster.Fixture.Subscriber, "restored-client", cluster.Root.PEM, cluster.SubscriberLeaf)
		previewAwaitReload(t, "client-malformed-recovery", func() bool { return previewGatewayOutcome(t, h, "preview.credential_reload_recovered", "recovered") })
		h.releaseFragments(t, 2)
		h.finish(t)
		for _, name := range []string{"tls-on", "tls-off"} {
			result := h.waitEvent(t, name, "span.model_request_end", 1)
			h.assertFormal(t, result, []string{"alpha βeta omega\n", "second\n"})
			assertPublicPreviewShapes(t, result, map[string][]string{"tls-on": {"agent.message"}}[name], []string{"alpha βeta omega\n", "second\n"})
		}
		h.assertPrivateContent(t)
		publicLogAssertion(t, "actual-tls-cross-route-sdk-identity-final-during-leaf-reload")
	})
	t.Run("issuer-unavailable-and-malformed-startup", func(t *testing.T) {
		for _, variant := range []string{"missing", "malformed"} {
			directory := t.TempDir()
			config := `port:4222\ntls {cert_file:"/fixture/tls.crt",key_file:"/fixture/tls.key",ca_file:"/fixture/ca.crt",verify:true,handshake_first:true}`
			config = strings.ReplaceAll(config, `\n`, "\n")
			if err := os.WriteFile(filepath.Join(directory, "nats.conf"), []byte(config), 0600); err != nil {
				t.Fatal(err)
			}
			if variant == "malformed" {
				for _, file := range []string{"tls.crt", "tls.key", "ca.crt"} {
					if err := os.WriteFile(filepath.Join(directory, file), []byte("invalid fixture material"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			container, err := cluster.Resources.Run(t.Context(), testinfra.ContainerSpec{Image: cluster.Fixture.Image, Mounts: []testinfra.DockerMount{{Source: directory, Target: "/fixture", ReadOnly: true}}, Ports: []int{4222}, Command: []string{"-c", "/fixture/nats.conf"}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			code, err := container.Wait(ctx)
			cancel()
			if err != nil || code == 0 {
				t.Fatal("protected cold startup admitted absent/invalid issuer material")
			}
		}
		previewTLSControl(t, cluster)
	})
	t.Run("valid-client-trust-removal-stops-previews", func(t *testing.T) { previewTLSRejectedTrust(t, cluster) })
	t.Run("advertised-broker-reconnect-and-trust-retirement", func(t *testing.T) { previewTLSTrustTransition(t, cluster) })
}

func previewTLSHarness(t *testing.T, c *previewTLSCluster) *publicStreamingHarness {
	t.Helper()
	publisher := c.Fixture
	publisher.Servers = []string{c.Fixture.Servers[0]}
	return newPublicStreamingHarness(t, "public-text", publicStreamingOptions{broker: &c.Fixture, subscriberServers: []string{c.Fixture.Servers[1]}, gateway: map[string]any{"previewNats": publicPublisherConfig(publisher)}})
}
func previewTLSControl(t *testing.T, c *previewTLSCluster) {
	t.Helper()
	consumer, err := c.connect(c.Brokers[1], c.Fixture.Subscriber, c.SubscriberLeaf, c.Root.PEM, "localhost")
	if err != nil {
		t.Fatal("allowed subscriber control failed")
	}
	defer previewCloseObservedControl(t, c.Brokers[1], consumer)()
	producer, err := c.connect(c.Brokers[0], c.Fixture.Publisher, c.PublisherLeaf, c.Root.PEM, "localhost")
	if err != nil {
		t.Fatal("allowed publisher control failed")
	}
	defer previewCloseObservedControl(t, c.Brokers[0], producer)()
	subject := "preview.v1.tls.control." + previewRandom(t)
	sub, err := consumer.SubscribeSync(subject)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	// A client flush confirms only its connected broker. Require the fresh
	// subscription in the publishing broker's route sublist before the single
	// measured publish. A unique subject excludes stale prior-control interest.
	started := time.Now()
	var routes, remoteRoutes, remoteSubscriptions int
	var ready, monitorOK bool
	func() {
		defer func() {
			t.Logf("cross-route control readiness publisher=%s subscriber=%s elapsed=%s bound=5s monitor_ok=%t routes=%d remote_routes=%d remote_subscriptions=%d exact_interest=%t", c.Brokers[0].Name, c.Brokers[1].Name, time.Since(started), monitorOK, routes, remoteRoutes, remoteSubscriptions, ready)
		}()
		previewAwait(t, 5*time.Second, func() bool {
			var state struct {
				NumRoutes int `json:"num_routes"`
				Routes    []struct {
					RemoteName    string   `json:"remote_name"`
					Subscriptions []string `json:"subscriptions_list"`
				} `json:"routes"`
			}
			monitorOK = previewMonitor(c.Brokers[0], "/routez?subs=1", &state) == nil
			routes, remoteRoutes, remoteSubscriptions = state.NumRoutes, 0, 0
			ready = false
			if !monitorOK {
				return false
			}
			for _, route := range state.Routes {
				if route.RemoteName != c.Brokers[1].Name {
					continue
				}
				remoteRoutes++
				remoteSubscriptions += len(route.Subscriptions)
				for _, installed := range route.Subscriptions {
					if installed == subject {
						ready = true
					}
				}
			}
			return ready
		})
	}()
	if err := producer.Publish(subject, []byte("authorized-across-route")); err != nil {
		t.Fatal(err)
	}
	if err := producer.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	message, err := sub.NextMsg(5 * time.Second)
	if err != nil || string(message.Data) != "authorized-across-route" {
		t.Fatal("actual route fan-out failed positive control")
	}
}

// Local Close does not join the broker's read loop/removal. Observe removal
// of this exact temporary control CID before later role-identity measurements.
func previewCloseObservedControl(t *testing.T, broker *previewTLSBroker, connection *nats.Conn) func() {
	t.Helper()
	cid, err := connection.GetClientID()
	if err != nil {
		connection.Close()
		t.Fatal("temporary control client identity unavailable")
	}
	serverID := connection.ConnectedServerId()
	return func() {
		connection.Close()
		started := time.Now()
		defer func() {
			t.Logf("control client removal broker=%s server_id=%s cid=%d elapsed=%s bound=5s", broker.Name, serverID, cid, time.Since(started))
		}()
		previewAwait(t, 5*time.Second, func() bool {
			var state struct {
				ServerID    string `json:"server_id"`
				Connections []struct {
					CID uint64 `json:"cid"`
				} `json:"connections"`
			}
			return previewMonitor(broker, fmt.Sprintf("/connz?cid=%d", cid), &state) == nil && state.ServerID == serverID && len(state.Connections) == 0
		})
	}
}
func previewServedSerial(b *previewTLSBroker, roots []byte, leaf transporttest.Leaf) string {
	return previewServedLeafSerial(b.ClientAddress, "localhost", roots, leaf)
}
func previewServedRouteSerial(b *previewTLSBroker, roots []byte, leaf transporttest.Leaf) string {
	return previewServedLeafSerial(b.RouteAddress, b.Name, roots, leaf)
}
func previewServedLeafSerial(address, dns string, roots []byte, leaf transporttest.Leaf) string {
	config, err := previewTLSConfig(roots, leaf, dns)
	if err != nil {
		return ""
	}
	connection, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", address, config)
	if err != nil {
		return ""
	}
	defer func() { _ = connection.Close() }()
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	info, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil || !strings.HasPrefix(info, "INFO ") {
		return ""
	}
	state := connection.ConnectionState()
	if state.DidResume || len(state.PeerCertificates) == 0 {
		return ""
	}
	return state.PeerCertificates[0].SerialNumber.String()
}
func previewDeniedRoute(t *testing.T, c *previewTLSCluster, wrong bool) {
	t.Helper()
	broker := c.Brokers[0]
	var before struct {
		NumRoutes int `json:"num_routes"`
	}
	if err := previewMonitor(broker, "/routez", &before); err != nil {
		t.Fatal(err)
	}
	config, err := previewTLSConfig(c.Root.PEM, c.PublisherLeaf, broker.Name)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", broker.RouteAddress, config)
	if err != nil {
		t.Fatal("valid application certificate could not reach route credential boundary")
	}
	defer func() { _ = connection.Close() }()
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	info := map[string]any{"name": "denied-fixture", "cluster": "preview-tls", "verbose": true}
	if wrong {
		info["user"] = c.RouteUser
		info["pass"] = "wrong-cluster-fixture-password"
	}
	encoded, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write(append(append([]byte("CONNECT "), encoded...), []byte("\r\nPING\r\n")...)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(connection)
	denied := false
	for range 8 {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "Authorization Violation") {
			denied = true
			break
		}
	}
	if !denied {
		t.Fatal("valid application certificate bypassed separate cluster credentials")
	}
	var after struct {
		NumRoutes int `json:"num_routes"`
	}
	if err := previewMonitor(broker, "/routez", &after); err != nil || after.NumRoutes != before.NumRoutes {
		t.Fatal("denied route changed admitted route membership")
	}
}

func previewTLSTrustTransition(t *testing.T, c *previewTLSCluster) {
	t.Helper()
	h := previewTLSHarness(t, c)
	h.open(t, "trust-on", []string{"agent.message"}, "")
	h.open(t, "trust-off", nil, "")
	h.send(t)
	h.waitFragments(t)
	h.releaseFragments(t, 1)
	h.waitEvent(t, "trust-on", "event_delta", 1)
	before := previewRequireRoleConnections(t, c)
	if !previewBothRolesConnected(before) {
		t.Fatal("expected one production publisher and one subscriber connection")
	}
	// The Go owner discovers the advertised destinations. Restart its seed
	// and observe a new live connection, rather than merely testing seed TLS.
	command := exec.CommandContext(t.Context(), "docker", "restart", c.Brokers[1].Container.Name)
	if err := command.Run(); err != nil {
		t.Fatal("restart run-owned subscriber seed")
	}
	previewAssertStableBindings(t, c.Brokers[1])
	previewAwaitRoleChange(t, c, "subscriber-seed-reconnect", before, false)
	before = previewRequireRoleConnections(t, c)
	oldRoot, oldPublisher := c.Root, c.PublisherLeaf
	second := transporttest.Must(transporttest.NewAuthority("preview-root-r2"))
	overlap := append(append([]byte(nil), oldRoot.PEM...), second.PEM...)
	previewProjectRole(t, c.Fixture.Publisher, "overlap", overlap, c.PublisherLeaf)
	previewProjectRole(t, c.Fixture.Subscriber, "overlap", overlap, c.SubscriberLeaf)
	previewAwaitRoleChange(t, c, "overlap-trust-activation", before, true)
	c.Root = second
	for i, broker := range c.Brokers {
		broker.ClientLeaf = transporttest.Must(second.ValidLeaf("localhost", "spiffe://cluster.local/ns/tetral-system/sa/nats-client-listener"))
		broker.RouteLeaf = transporttest.Must(second.ValidLeaf(broker.Name, "spiffe://cluster.local/ns/tetral-system/sa/nats-route"))
		previewProjectBroker(t, broker, fmt.Sprintf("overlap-r2-%d", i), overlap)
		if err := broker.Container.Signal(t.Context(), "HUP"); err != nil {
			t.Fatal(err)
		}
		previewAwaitReload(t, "broker-r2-leaf-activation", func() bool {
			return previewServedSerial(broker, overlap, oldPublisher) == broker.ClientLeaf.Parsed.SerialNumber.String() && previewServedRouteSerial(broker, overlap, oldPublisher) == broker.RouteLeaf.Parsed.SerialNumber.String()
		})
	}
	before = previewRequireRoleConnections(t, c)
	c.PublisherLeaf = transporttest.Must(second.ValidLeaf("localhost", previewPublisherURI))
	c.SubscriberLeaf = transporttest.Must(second.ValidLeaf("localhost", previewSubscriberURI))
	previewProjectRole(t, c.Fixture.Publisher, "r2-leaf", overlap, c.PublisherLeaf)
	previewProjectRole(t, c.Fixture.Subscriber, "r2-leaf", overlap, c.SubscriberLeaf)
	previewAwaitRoleChange(t, c, "r2-client-leaf-activation", before, false)
	previewTLSControl(t, c)
	// Retire pre-switch route connections while both issuers are trusted.
	// Restart one broker at a time and prove surviving full route membership.
	for _, broker := range c.Brokers {
		if err := exec.CommandContext(t.Context(), "docker", "restart", broker.Container.Name).Run(); err != nil {
			t.Fatal("retire old broker route connections")
		}
		previewAssertStableBindings(t, broker)
		previewAwait(t, 60*time.Second, func() bool {
			for _, peer := range c.Brokers {
				var state struct {
					NumRoutes int `json:"num_routes"`
				}
				if previewMonitor(peer, "/routez", &state) != nil || state.NumRoutes != 2 {
					return false
				}
			}
			return true
		})
	}
	// Route restarts may recover before application reconnect completes.
	// Both live role connections must exist before the trust-retirement oracle
	// can establish that each old connection was actually retired.
	previewAwaitBothRoles(t, c, h, "post-route-restarts")
	before = previewRequireRoleConnections(t, c)
	previewProjectRole(t, c.Fixture.Publisher, "r2-only", second.PEM, c.PublisherLeaf)
	previewProjectRole(t, c.Fixture.Subscriber, "r2-only", second.PEM, c.SubscriberLeaf)
	previewAwaitRoleChange(t, c, "r1-client-trust-retirement", before, true)
	for i, broker := range c.Brokers {
		previewProjectBroker(t, broker, fmt.Sprintf("r2-only-%d", i), second.PEM)
		if err := broker.Container.Signal(t.Context(), "HUP"); err != nil {
			t.Fatal(err)
		}
		previewAwaitReload(t, "broker-r1-trust-retirement", func() bool {
			return previewServedSerial(broker, second.PEM, c.PublisherLeaf) == broker.ClientLeaf.Parsed.SerialNumber.String() && previewServedRouteSerial(broker, second.PEM, c.PublisherLeaf) == broker.RouteLeaf.Parsed.SerialNumber.String()
		})
		if previewServedRouteSerial(broker, overlap, oldPublisher) != "" {
			t.Fatal("fresh R1 route client bypassed removed trust")
		}
		if connection, err := c.connect(broker, c.Fixture.Publisher, oldPublisher, overlap, "localhost"); err == nil {
			connection.Close()
			t.Fatal("fresh R1 client bypassed removed trust")
		}
		if connection, err := c.connect(broker, c.Fixture.Publisher, c.PublisherLeaf, oldRoot.PEM, "localhost"); err == nil {
			connection.Close()
			t.Fatal("R1-only consumer trusted new R2 broker")
		}
	}
	previewTLSControl(t, c)
	h.releaseFragments(t, 3)
	h.finish(t)
	for _, name := range []string{"trust-on", "trust-off"} {
		result := h.waitEvent(t, name, "span.model_request_end", 1)
		h.assertFormal(t, result, []string{"alpha βeta omega\n", "second\n"})
		assertPublicPreviewShapes(t, result, map[string][]string{"trust-on": {"agent.message"}}[name], []string{"alpha βeta omega\n", "second\n"})
	}
	// A new independent model request must actually preview after recovery;
	// exact formal content alone cannot pass a globally disabled transport.
	recovered := previewTLSHarness(t, c)
	recovered.open(t, "recovered", []string{"agent.message"}, "")
	recovered.send(t)
	recovered.waitFragments(t)
	recovered.releaseFragments(t, 1)
	recovered.waitEvent(t, "recovered", "event_delta", 1)
	recovered.releaseFragments(t, 3)
	recovered.finish(t)
	result := recovered.waitEvent(t, "recovered", "span.model_request_end", 1)
	recovered.assertFormal(t, result, []string{"alpha βeta omega\n", "second\n"})
	publicLogAssertion(t, "actual-production-client-generation-replacement-advertised-reconnect-and-r1-removal")
}
func previewProjectRole(t *testing.T, role testinfra.NATSRole, generation string, roots []byte, leaf transporttest.Leaf) {
	t.Helper()
	user, err := os.ReadFile(role.UserPath)
	if err != nil {
		t.Fatal(err)
	}
	password, err := os.ReadFile(role.PasswordPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := transporttest.Project(filepath.Dir(role.UserPath), generation, map[string][]byte{"ca.crt": roots, "tls.crt": leaf.Certificate, "tls.key": leaf.Key, "user": user, "password": password}); err != nil {
		t.Fatal(err)
	}
}
func previewRoleConnections(t *testing.T, c *previewTLSCluster) map[string]bool {
	t.Helper()
	state := previewRoleConnectionState(t, c)
	if state == nil {
		return nil
	}
	connections := map[string]bool{}
	for id := range state {
		connections[id] = true
	}
	return connections
}

// Polls treat nil as an incomplete sweep; stable before/after snapshots must
// fail rather than interpret monitor unavailability as a changed connection.
func previewRequireRoleConnections(t *testing.T, c *previewTLSCluster) map[string]bool {
	t.Helper()
	connections := previewRoleConnections(t, c)
	if connections == nil {
		t.Fatal("required role connection snapshot unavailable")
	}
	return connections
}

// Monitoring credentials are matched in memory and replaced with fixed public
// role labels. Only connection identities and subscription counts escape.
// Nil means incomplete monitoring; an allocated empty map means no role peers.
func previewRoleConnectionState(t *testing.T, c *previewTLSCluster) map[string]int {
	t.Helper()
	roles := map[string]string{}
	for label, role := range map[string]testinfra.NATSRole{"publisher": c.Fixture.Publisher, "subscriber": c.Fixture.Subscriber} {
		user, err := os.ReadFile(role.UserPath)
		if err != nil {
			t.Fatal(err)
		}
		roles[string(user)] = label
	}
	connections := map[string]int{}
	for _, broker := range c.Brokers {
		var state struct {
			ServerID    string `json:"server_id"`
			Connections []struct {
				CID           uint64 `json:"cid"`
				User          string `json:"authorized_user"`
				Subscriptions int    `json:"subscriptions"`
			} `json:"connections"`
		}
		if err := previewMonitor(broker, "/connz?auth=true", &state); err != nil {
			return nil
		}
		for _, conn := range state.Connections {
			if label := roles[conn.User]; label != "" {
				connections[fmt.Sprintf("%s/%s:%s:%d", label, broker.Name, state.ServerID, conn.CID)] = conn.Subscriptions
			}
		}
	}
	return connections
}
func previewRoleConnectionsChanged(old, current map[string]bool) bool {
	if !previewBothRolesConnected(current) {
		return false
	}
	for id := range old {
		if current[id] {
			return false
		}
	}
	return true
}

func previewBothRolesConnected(connections map[string]bool) bool {
	if len(connections) != 2 {
		return false
	}
	counts := map[string]int{}
	for id := range connections {
		role, _, _ := strings.Cut(id, "/")
		counts[role]++
	}
	return counts["publisher"] == 1 && counts["subscriber"] == 1
}

func previewAwaitBothRoles(t *testing.T, c *previewTLSCluster, h *publicStreamingHarness, phase string) {
	t.Helper()
	var observed map[string]bool
	started := time.Now()
	defer func() {
		observation := h.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
		var metrics string
		if err := json.Unmarshal(observation["previewMetrics"], &metrics); err != nil {
			t.Fatal("native publisher metrics unavailable")
		}
		publisherConnected := "missing"
		for _, line := range strings.Split(metrics, "\n") {
			if value, found := strings.CutPrefix(line, "providergateway_preview_connected "); found {
				publisherConnected = value
			}
		}
		state := previewRoleConnectionState(t, c)
		t.Logf("production role recovery phase=%s elapsed=%s bound=10s observed=%v live_native_subscription_counts=%v publisher_connected=%s subscriber_watched_sessions=%.0f subscriber_disconnects=%.0f subscriber_reconnects=%.0f monitor_complete=%t", phase, time.Since(started), observed, state, publisherConnected, h.metric(t, "event_stream_preview_subscriptions"), h.metric(t, "event_stream_nats_disconnects_total"), h.metric(t, "event_stream_nats_reconnects_total"), state != nil)
	}()
	previewAwait(t, 10*time.Second, func() bool { observed = previewRoleConnections(t, c); return previewBothRolesConnected(observed) })
}
func previewSomeRoleConnectionChanged(old, current map[string]bool) bool {
	if !previewBothRolesConnected(current) {
		return false
	}
	for id := range current {
		if !old[id] {
			return true
		}
	}
	return false
}

func previewAwaitRoleChange(t *testing.T, c *previewTLSCluster, phase string, before map[string]bool, all bool) {
	t.Helper()
	var observed map[string]bool
	started := time.Now()
	bound := 5 * time.Second
	if phase == "subscriber-seed-reconnect" {
		bound = 10 * time.Second
	}
	defer func() {
		t.Logf("production TLS phase=%s elapsed=%s bound=%s before=%v observed=%v require_all_replaced=%t monitor_complete=%t", phase, time.Since(started), bound, before, observed, all, observed != nil)
	}()
	previewAwait(t, bound, func() bool {
		observed = previewRoleConnections(t, c)
		if all {
			return previewRoleConnectionsChanged(before, observed)
		}
		if phase == "subscriber-seed-reconnect" {
			if !previewBothRolesConnected(observed) {
				return false
			}
			for id := range observed {
				if strings.HasPrefix(id, "subscriber/") && !before[id] {
					return true
				}
			}
			return false
		}
		return previewSomeRoleConnectionChanged(before, observed)
	})
}

func previewGatewayOutcome(t *testing.T, h *publicStreamingHarness, event, outcome string) bool {
	t.Helper()
	observation := h.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
	var values []struct {
		Event   string `json:"event"`
		Outcome string `json:"outcome"`
	}
	if err := json.Unmarshal(observation["previewOutcomes"], &values); err != nil {
		t.Fatal("native publisher credential outcomes unavailable")
	}
	for _, value := range values {
		if value.Event == event && value.Outcome == outcome {
			return true
		}
	}
	return false
}
func previewTLSRejectedTrust(t *testing.T, c *previewTLSCluster) {
	t.Helper()
	h := previewTLSHarness(t, c)
	h.open(t, "removed-trust", []string{"agent.message"}, "")
	h.open(t, "removed-trust-off", nil, "")
	h.send(t)
	h.waitFragments(t)
	h.releaseFragments(t, 1)
	h.waitEvent(t, "removed-trust", "event_delta", 1)
	second := transporttest.Must(transporttest.NewAuthority("rejected-client-trust"))
	publisher := transporttest.Must(second.ValidLeaf("localhost", previewPublisherURI))
	subscriber := transporttest.Must(second.ValidLeaf("localhost", previewSubscriberURI))
	previewProjectRole(t, c.Fixture.Publisher, "valid-rejecting-trust", second.PEM, publisher)
	previewProjectRole(t, c.Fixture.Subscriber, "valid-rejecting-trust", second.PEM, subscriber)
	previewAwaitReload(t, "valid-client-trust-removal-retirement", func() bool {
		connections := previewRoleConnections(t, c)
		return connections != nil && len(connections) == 0 && previewGatewayOutcome(t, h, "preview.credential_reload_failed", "invalid_generation")
	})
	h.releaseFragments(t, 3)
	h.finish(t)
	for _, name := range []string{"removed-trust", "removed-trust-off"} {
		result := h.waitEvent(t, name, "span.model_request_end", 1)
		h.assertFormal(t, result, []string{"alpha βeta omega\n", "second\n"})
		assertPublicPreviewShapes(t, result, map[string][]string{"removed-trust": {"agent.message"}}[name], []string{"alpha βeta omega\n", "second\n"})
	}
	previewProjectRole(t, c.Fixture.Publisher, "restore-accepted-trust", c.Root.PEM, c.PublisherLeaf)
	previewProjectRole(t, c.Fixture.Subscriber, "restore-accepted-trust", c.Root.PEM, c.SubscriberLeaf)
	previewAwait(t, 10*time.Second, func() bool { return previewBothRolesConnected(previewRoleConnections(t, c)) })
	previewTLSControl(t, c)
	publicLogAssertion(t, "valid-native-client-trust-removal-retires-old-connections-formal-end-still-completes")
}

type previewReloadObserver struct {
	handler  slog.Handler
	rejected *atomic.Uint64
}

func (h previewReloadObserver) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}
func (h previewReloadObserver) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "transport.credential_reload_failed" {
		native := false
		record.Attrs(func(a slog.Attr) bool {
			if a.Key == "component" && a.Value.String() == "nats" {
				native = true
			}
			return true
		})
		if native {
			h.rejected.Add(1)
		}
	}
	return h.handler.Handle(ctx, record)
}
func (h previewReloadObserver) WithAttrs(attrs []slog.Attr) slog.Handler {
	return previewReloadObserver{h.handler.WithAttrs(attrs), h.rejected}
}
func (h previewReloadObserver) WithGroup(name string) slog.Handler {
	return previewReloadObserver{h.handler.WithGroup(name), h.rejected}
}
func previewObserveNativeReloads(t *testing.T) *atomic.Uint64 {
	t.Helper()
	previous := slog.Default()
	previousWriter, previousFlags, previousPrefix := log.Writer(), log.Flags(), log.Prefix()
	rejected := &atomic.Uint64{}
	// Wrapping slog's defaultHandler would recurse through log.Default after
	// SetDefault redirects it to this handler. Own an independent writer.
	slog.SetDefault(slog.New(previewReloadObserver{slog.NewTextHandler(os.Stderr, nil), rejected}))
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})
	return rejected
}

func previewAwaitReload(t *testing.T, phase string, predicate func() bool) {
	t.Helper()
	started := time.Now()
	defer func() {
		t.Logf("native credential reload phase=%s elapsed=%s acceptance_bound=5s", phase, time.Since(started))
	}()
	previewAwait(t, 5*time.Second, predicate)
}
func previewAssertStableBindings(t *testing.T, broker *previewTLSBroker) {
	t.Helper()
	for containerPort, hostPort := range broker.HostPorts {
		actual, err := broker.Container.Port(t.Context(), containerPort)
		if err != nil || actual != fmt.Sprint(hostPort) {
			t.Fatalf("broker restart changed retained binding container_port=%d wanted=%d actual=%s error=%v", containerPort, hostPort, actual, err)
		}
	}
}
