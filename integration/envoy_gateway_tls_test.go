package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"github.com/tetral-ai/tetral/integration/transporttest"
)

// Generations change only mounted TLS material in the original fixture input.
// Both translators still own every resource. The served policy graph is never
// patched to make a negative handshake or reload succeed.
type edgeTLSMaterial struct {
	Leaves map[string]transporttest.Leaf
	Trust  []byte
}

func translateEdgeTLSGeneration(t *testing.T, fixture *envoyGatewayTranslation, name string, material edgeTLSMaterial) *translatedEnvoySnapshot {
	t.Helper()
	decoder := yaml.NewDecoder(bytes.NewReader(fixture.Snapshot.FixtureYAML))
	objects := []map[string]any{}
	changed := map[string]bool{}
	secretRoles := map[string]string{"tetral-api-public-tls": "public-api", "tetral-git-public-tls": "public-git", "tetral-edge-client-tls": "edge"}
	for {
		var object map[string]any
		if err := decoder.Decode(&object); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal("decode original TLS fixture")
		}
		if object == nil {
			continue
		}
		metadata := object["metadata"].(map[string]any)
		if object["kind"] == "Secret" {
			role := secretRoles[metadata["name"].(string)]
			if leaf, found := material.Leaves[role]; found {
				data := object["data"].(map[string]any)
				data["tls.crt"] = base64.StdEncoding.EncodeToString(leaf.Certificate)
				data["tls.key"] = base64.StdEncoding.EncodeToString(leaf.Key)
				changed[role] = true
			}
		}
		if object["kind"] == "ConfigMap" && metadata["name"] == "tetral-edge-native-trust" && material.Trust != nil {
			object["data"].(map[string]any)["ca.crt"] = string(material.Trust)
			changed["trust"] = true
		}
		objects = append(objects, object)
	}
	if len(changed) != len(material.Leaves)+boolInt(material.Trust != nil) || len(changed) == 0 {
		t.Fatal("TLS generation omitted or added a material target")
	}
	ownerContext := context.Background()
	if fixture.Control != nil {
		ownerContext = fixture.Control.context
	}
	ctx, cancel := context.WithTimeout(ownerContext, 45*time.Second)
	defer cancel()
	next := runOfficialEnvoyTranslation(ctx, t, fixture.Directory, name, objects)
	completeEnvoyGatewaySecrets(ctx, t, next)
	requireOnlyEdgeTLSMaterialDelta(t, fixture, next, material)
	return next
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func requireOnlyEdgeTLSMaterialDelta(t *testing.T, fixture *envoyGatewayTranslation, next *translatedEnvoySnapshot, material edgeTLSMaterial) {
	t.Helper()
	original := fixture.Snapshot
	if !reflect.DeepEqual(original.Bootstrap, next.Bootstrap) || !reflect.DeepEqual(original.ProxyDrainArgs, next.ProxyDrainArgs) || len(original.Resources) != len(next.Resources) {
		t.Fatal("TLS generation changed process configuration or resource kinds")
	}
	changedRoles := map[string]bool{}
	changedTrust := 0
	changedSecrets := 0
	for kind, originals := range original.Resources {
		actuals := next.Resources[kind]
		if len(originals) != len(actuals) {
			t.Fatal("TLS generation changed resource count")
		}
		for i, resource := range originals {
			if cachev3.GetResourceName(resource) != cachev3.GetResourceName(actuals[i]) {
				t.Fatal("TLS generation changed resource identity/order")
			}
			expected := proto.Clone(resource)
			if kind == resourcev3.SecretType {
				secret := expected.(*tlsv3.Secret)
				if certificate := secret.GetTlsCertificate(); certificate != nil {
					for role, leaf := range material.Leaves {
						if bytes.Equal(certificate.GetCertificateChain().GetInlineBytes(), fixture.Leaves[role].Certificate) {
							// Preserve the whole DataSource, including unknown fields. Only its
							// already-selected inline byte value changes.
							edgeReplaceInlineTLSBytes(t, certificate.CertificateChain, leaf.Certificate)
							edgeReplaceInlineTLSBytes(t, certificate.PrivateKey, leaf.Key)
							changedRoles[role] = true
						}
					}
				}
				if validation := secret.GetValidationContext(); validation != nil && material.Trust != nil {
					if !bytes.Equal(validation.GetTrustedCa().GetInlineBytes(), fixture.Authority.PEM) {
						t.Fatal("original native trust source differs")
					}
					edgeReplaceInlineTLSBytes(t, validation.TrustedCa, material.Trust)
					changedTrust++
				}
				if !proto.Equal(expected, resource) {
					changedSecrets++
				}
			}
			observed := proto.Clone(actuals[i])
			canonicalizeEdgeAny(t, expected.ProtoReflect())
			canonicalizeEdgeAny(t, observed.ProtoReflect())
			if !proto.Equal(expected, observed) {
				t.Fatalf("TLS generation changed another policy field kind=%s", kind)
			}
		}
	}
	if len(changedRoles) != len(material.Leaves) || changedSecrets == 0 || (material.Trust != nil && changedTrust != 4) {
		t.Fatal("TLS generation did not change exactly its referenced material")
	}
	t.Logf("envoy_gateway_tls_graph_delta certificate_roles=%d trust_secrets=%d changed_secrets=%d policy_equal=true", len(changedRoles), changedTrust, changedSecrets)
}

func TestPostgreSQLEnvoyGatewayTLSLifecycle(t *testing.T) {
	oidcIsolatedTLSCaseWithMarker(t, "envoy_gateway_tls_assertion=", func(t *testing.T) {
		for _, profile := range []string{"standard-routed", "hardened"} {
			t.Run(profile, func(t *testing.T) {
				edge := &translatedPublicEdge{lifecycle: &edgeTLSLifecycle{}}
				f := newPublicProjectionFixture(t, publicProjectionOptions{publicEdge: edge.factory(profile)})
				lifecycle, fixture := edge.lifecycle, edge.fixture
				original := fixture.Snapshot
				if profile == "hardened" {
					edge.assertTLSNegatives(t, f)
				}
				f.open(t, "tls-held", nil, "")
				publicWait(t, "real SDK held TLS stream heartbeat", func() bool { return f.snapshot(t, "tls-held").Heartbeats > 0 })
				publicLeaf := transporttest.Must(fixture.Authority.ValidLeaf("api.localhost", fixture.Leaves["public-api"].Parsed.URIs[0].String()))
				publicMaterial := edgeTLSMaterial{Leaves: map[string]transporttest.Leaf{"public-api": publicLeaf}}
				fixture.Control.publishSecrets(t, translateEdgeTLSGeneration(t, fixture, "frontend-renewal", publicMaterial))
				edge.assertPublicTLSLeaf(t, publicLeaf)
				if f.snapshot(t, "tls-held").Ended {
					t.Fatal("frontend certificate activation killed an admitted SDK stream")
				}
				var nextAuthority *transporttest.Authority
				var nextLeaves map[string]transporttest.Leaf
				if profile == "hardened" {
					nextAuthority = transporttest.Must(transporttest.NewAuthority("native-edge-next"))
					nextLeaves = map[string]transporttest.Leaf{}
					for _, role := range []string{"edge", "auth", "api", "event-stream", "git-proxy"} {
						old := fixture.Leaves[role]
						nextLeaves[role] = transporttest.Must(nextAuthority.ValidLeaf(old.Parsed.DNSNames[0], old.Parsed.URIs[0].String()))
					}
					overlap := append(append([]byte(nil), fixture.Authority.PEM...), nextAuthority.PEM...)
					for _, role := range []string{"auth", "api", "event-stream", "git-proxy"} {
						lifecycle.projectHTTP(t, role, "overlap-old", overlap, fixture.Leaves[role])
					}
					lifecycle.projectCheck(t, "overlap-old", overlap, fixture.Leaves["auth"])
					lifecycle.restartCheck(t)
					overlapMaterial := edgeTLSMaterial{Leaves: publicMaterial.Leaves, Trust: overlap}
					fixture.Control.publishSecrets(t, translateEdgeTLSGeneration(t, fixture, "native-overlap-old-leaves", overlapMaterial))
					for _, role := range []string{"auth", "api", "event-stream", "git-proxy"} {
						edge.assertTLSRole(t, f, role, edgeTLSRoleStatus(role), 1, fixture.Leaves["edge"].Parsed.SerialNumber.String())
					}
					if f.snapshot(t, "tls-held").Ended {
						t.Fatal("trust overlap killed an admitted SDK stream")
					}
					newMaterial := edgeTLSMaterial{Leaves: map[string]transporttest.Leaf{"public-api": publicLeaf, "edge": nextLeaves["edge"]}, Trust: overlap}
					fixture.Control.publishSecrets(t, translateEdgeTLSGeneration(t, fixture, "native-next-edge-leaf", newMaterial))
					lifecycle.projectCheck(t, "overlap-new", overlap, nextLeaves["auth"])
					lifecycle.restartCheck(t) // Force the new Check handshake; no retained-channel assumption.
					for _, role := range []string{"auth", "api", "event-stream", "git-proxy"} {
						lifecycle.projectHTTP(t, role, "overlap-new", overlap, nextLeaves[role])
						lifecycle.handOverHTTP(t, role)
						edge.assertTLSRole(t, f, role, edgeTLSRoleStatus(role), 1, nextLeaves["edge"].Parsed.SerialNumber.String())
					}
					if f.snapshot(t, "tls-held").Ended {
						t.Fatal("leaf renewal or graceful handover killed the held SDK stream")
					}
				}
				request := f.start(t, "", "")
				message := f.text(t, request, "actual TLS overlap retained this stream", false)
				end := f.end(t, request)
				seen := f.waitEvent(t, "tls-held", "span.model_request_end", 1)
				if seen.Ended || !edgeTLSEventPresent(seen.Events, message) || !edgeTLSEventPresent(seen.Events, end) {
					t.Fatal("held SDK stream did not retain its exact committed events through TLS activation")
				}
				var retired *translatedEnvoySnapshot
				if profile == "hardened" {
					retired = translateEdgeTLSGeneration(t, fixture, "native-retired-trust", edgeTLSMaterial{Leaves: map[string]transporttest.Leaf{"public-api": publicLeaf, "edge": nextLeaves["edge"]}, Trust: nextAuthority.PEM})
				} else {
					retired = translateEdgeTLSGeneration(t, fixture, "public-renewed-after-drain", publicMaterial)
				}
				fixture.Control.drain(t, func() {
					closed := decodePublicSnapshot(t, f.client.control(t, "close_viewer", map[string]any{"viewer": "tls-held"}))
					if !closed.Ended {
						t.Fatal("SDK stream cancellation did not join drain")
					}
					edge.client.CloseIdleConnections()
					for _, old := range lifecycle.retired {
						old.join(t)
					}
					// Session provisioning owns a separate SDK process and idle
					// pool. Join that finished owner before retiring edge trust;
					// the streaming SDK remains available for restart/history.
					fixture.Control.drainConnections(edge.ctx, t, "before-provisioning-sdk-join")
					f.sdk.stop(t)
					fixture.Control.drainConnections(edge.ctx, t, "after-provisioning-sdk-join")
				})
				for _, role := range []string{"auth", "api", "event-stream", "git-proxy"} {
					lifecycle.http[role].join(t)
				}
				lifecycle.stopCheck()
				if profile == "hardened" {
					for _, role := range []string{"auth", "api", "event-stream", "git-proxy"} {
						lifecycle.projectHTTP(t, role, "retired", nextAuthority.PEM, nextLeaves[role])
						lifecycle.restartHTTP(t, role)
					}
					lifecycle.projectCheck(t, "retired", nextAuthority.PEM, nextLeaves["auth"])
				} else {
					for _, role := range []string{"auth", "api", "event-stream", "git-proxy"} {
						lifecycle.restartHTTP(t, role)
					}
				}
				lifecycle.restartCheck(t)
				fixture.Snapshot = retired
				startEnvoyGatewayProxy(edge.ctx, t, fixture)
				fixture.Snapshot = original // All later generation comparisons retain the original reference graph.
				edgeAwaitReady(edge.ctx, t, fixture.Ports.Ready)
				edge.assertPublicTLSLeaf(t, publicLeaf)
				if profile == "hardened" {
					for _, role := range []string{"auth", "api", "event-stream", "git-proxy"} {
						edge.assertTLSRole(t, f, role, edgeTLSRoleStatus(role), 1, nextLeaves["edge"].Parsed.SerialNumber.String())
					}
					// A fresh old client cannot be hidden by an established pre-retirement channel.
					oldClient := translateEdgeTLSGeneration(t, fixture, "old-edge-after-retirement", edgeTLSMaterial{Leaves: map[string]transporttest.Leaf{"public-api": publicLeaf, "edge": fixture.Leaves["edge"]}, Trust: nextAuthority.PEM})
					fixture.Control.publishSecrets(t, oldClient)
					lifecycle.restartCheck(t)
					edge.assertTLSRole(t, f, "api", 503, 0, "")
					fixture.Control.publishSecrets(t, retired)
					lifecycle.restartCheck(t)
					// Deliberately serve a valid old peer: its owning loader must
					// trust its own leaf as well as the current edge. Envoy's
					// retired trust stays new-only and must reject that peer.
					oldPeerTrust := append(append([]byte(nil), fixture.Authority.PEM...), nextAuthority.PEM...)
					for _, role := range []string{"auth", "api", "event-stream", "git-proxy"} {
						lifecycle.projectHTTP(t, role, "old-server-after-retirement", oldPeerTrust, fixture.Leaves[role])
						lifecycle.restartHTTP(t, role)
						edge.assertTLSRole(t, f, role, 503, 0, "")
						lifecycle.projectHTTP(t, role, "current-server-after-retirement", nextAuthority.PEM, nextLeaves[role])
						lifecycle.restartHTTP(t, role)
						edge.assertTLSRole(t, f, role, edgeTLSRoleStatus(role), 1, nextLeaves["edge"].Parsed.SerialNumber.String())
					}
					lifecycle.projectCheck(t, "old-server-after-retirement", oldPeerTrust, fixture.Leaves["auth"])
					lifecycle.restartCheck(t)
					edge.assertTLSRole(t, f, "api", 503, 0, "")
					lifecycle.projectCheck(t, "current-server-after-retirement", nextAuthority.PEM, nextLeaves["auth"])
					lifecycle.restartCheck(t)
				}
				f.open(t, "tls-recovered", nil, "")
				page := ""
				foundMessage, foundEnd, complete := false, false, false
				for range 128 {
					history := f.list(t, "", "asc", page)
					for _, event := range history.Data {
						foundMessage = foundMessage || publicEventID(event) == message
						foundEnd = foundEnd || publicEventID(event) == end
					}
					if history.Next == nil {
						complete = true
						break
					}
					if *history.Next == "" || *history.Next == page {
						t.Fatal("TLS recovery list cursor did not advance")
					}
					page = *history.Next
				}
				if !complete || !foundMessage || !foundEnd {
					t.Fatal("real SDK list lost exact committed history after edge drain/restart")
				}
				fence := f.start(t, "", "")
				fenceEnd := f.end(t, fence)
				recovered := f.waitEvent(t, "tls-recovered", "span.model_request_end", 1)
				if !edgeTLSEventPresent(recovered.Events, fenceEnd) || edgeTLSEventPresent(recovered.Events, message) || edgeTLSEventPresent(recovered.Events, end) {
					t.Fatal("recovered TLS stream omitted new fence or replayed old history")
				}
				t.Logf("envoy_gateway_tls_assertion=actual_edge_lifecycle profile=%s frontend_reload=true held_sdk_stream=true explicit_drain=true native_retirement=%t passed=true", profile, profile == "hardened")
			})
		}
	})
}

func edgeTLSEventPresent(events []map[string]any, id string) bool {
	for _, event := range events {
		if publicEventID(event) == id {
			return true
		}
	}
	return false
}

func edgeTLSRoleStatus(role string) int {
	switch role {
	case "auth":
		return 400
	case "git-proxy":
		return 401
	default:
		return 200
	}
}

func (edge *translatedPublicEdge) assertTLSRole(t *testing.T, f *publicProjectionFixture, role string, want int, wantReceiver int64, wantPeer string) {
	t.Helper()
	receiver := edge.lifecycle.http[role]
	before, _ := receiver.receipt()
	checks := edgeAuthCheckCount(edge.ctx, t, edge.fixture.Ports.Admin)
	method, path, key, host := "GET", "/v1/sessions/"+f.session+"?beta=true", edge.key, "api.localhost"
	switch role {
	case "auth":
		method, path, key = "POST", "/v1/oauth/token", ""
	case "event-stream":
		path = "/v1/sessions/" + f.session + "/events/stream?beta=true"
	case "git-proxy":
		path, key, host = "/github.com/tetral-ai/public/info/refs?service=git-upload-pack", "", "git.localhost"
	}
	request := transporttest.Must(http.NewRequestWithContext(edge.ctx, method, fmt.Sprintf("https://%s:%d%s", host, edge.fixture.Ports.HTTPS, path), nil))
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("X-Api-Key", key)
	}
	response, err := edge.client.Do(request)
	if err != nil {
		t.Fatal("actual edge TLS role request failed before HTTP receipt")
	}
	_ = response.Body.Close()
	after, peer := receiver.receipt()
	checkDelta := edgeAuthCheckCount(edge.ctx, t, edge.fixture.Ports.Admin) - checks
	wantChecks := int64(0)
	if role == "api" || role == "event-stream" {
		wantChecks = 1
	}
	t.Logf("envoy_gateway_tls_role role=%s status=%d receiver_delta=%d check_delta=%d expected_peer=%t", role, response.StatusCode, after-before, checkDelta, wantPeer == "" || peer == wantPeer)
	if response.StatusCode != want || after-before != wantReceiver || checkDelta != wantChecks || (wantPeer != "" && peer != wantPeer) {
		t.Fatal("actual TLS role handshake/admission receipt differs")
	}
}

func (edge *translatedPublicEdge) assertPublicTLSLeaf(t *testing.T, leaf transporttest.Leaf) {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(edge.fixture.Authority.PEM)
	dialer := tls.Dialer{Config: &tls.Config{RootCAs: roots, ServerName: "api.localhost", MinVersion: tls.VersionTLS13}, NetDialer: &net.Dialer{Timeout: 3 * time.Second}}
	connection := transporttest.Must(dialer.DialContext(edge.ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", edge.fixture.Ports.HTTPS)))
	defer func() { _ = connection.Close() }()
	state := connection.(*tls.Conn).ConnectionState()
	if len(state.PeerCertificates) != 1 || state.PeerCertificates[0].SerialNumber.Cmp(leaf.Parsed.SerialNumber) != 0 {
		t.Fatal("fresh public TLS handshake did not select renewed leaf")
	}
}

func (edge *translatedPublicEdge) assertTLSNegatives(t *testing.T, f *publicProjectionFixture) {
	t.Helper()
	fixture, lifecycle := edge.fixture, edge.lifecycle
	unknown := transporttest.Must(transporttest.NewAuthority("untrusted-edge-negative"))
	for _, role := range []string{"auth", "api", "event-stream", "git-proxy"} {
		edge.assertTLSRole(t, f, role, edgeTLSRoleStatus(role), 1, fixture.Leaves["edge"].Parsed.SerialNumber.String())
		wrong := transporttest.Must(fixture.Authority.ValidLeaf("wrong-backend.edge.test", fixture.Leaves[role].Parsed.URIs[0].String()))
		lifecycle.projectHTTP(t, role, "wrong-san", fixture.Authority.PEM, wrong)
		lifecycle.restartHTTP(t, role)
		edge.assertTLSRole(t, f, role, 503, 0, "")
		lifecycle.projectHTTP(t, role, "restored-san", fixture.Authority.PEM, fixture.Leaves[role])
		lifecycle.restartHTTP(t, role)
		edge.assertTLSRole(t, f, role, edgeTLSRoleStatus(role), 1, fixture.Leaves["edge"].Parsed.SerialNumber.String())
		// The backend presents its correct DNS name and role from a root the edge
		// does not trust. The backend's own bundle includes that root only so its
		// loader activates the leaf; the edge client must reject the server.
		unknownServer := transporttest.Must(unknown.ValidLeaf(fixture.Leaves[role].Parsed.DNSNames[0], fixture.Leaves[role].Parsed.URIs[0].String()))
		lifecycle.projectHTTP(t, role, "unknown-ca", append(append([]byte(nil), fixture.Authority.PEM...), unknown.PEM...), unknownServer)
		lifecycle.restartHTTP(t, role)
		edge.assertTLSRole(t, f, role, 503, 0, "")
		lifecycle.projectHTTP(t, role, "restored-ca", fixture.Authority.PEM, fixture.Leaves[role])
		lifecycle.restartHTTP(t, role)
		edge.assertTLSRole(t, f, role, edgeTLSRoleStatus(role), 1, fixture.Leaves["edge"].Parsed.SerialNumber.String())
	}
	wrongCheck := transporttest.Must(fixture.Authority.ValidLeaf("wrong-check.edge.test", fixture.Leaves["auth"].Parsed.URIs[0].String()))
	lifecycle.projectCheck(t, "wrong-san", fixture.Authority.PEM, wrongCheck)
	lifecycle.restartCheck(t)
	edge.assertTLSRole(t, f, "api", 503, 0, "")
	lifecycle.projectCheck(t, "restored-san", fixture.Authority.PEM, fixture.Leaves["auth"])
	lifecycle.restartCheck(t)
	for _, negative := range []struct {
		name string
		leaf transporttest.Leaf
	}{
		{"wrong-role", transporttest.Must(fixture.Authority.ValidLeaf("edge.native.test", "spiffe://cluster.local/ns/envoy-gateway-system/sa/wrong-edge"))},
		{"unknown-ca", transporttest.Must(unknown.ValidLeaf("edge.native.test", fixture.Leaves["edge"].Parsed.URIs[0].String()))},
		{"expired", transporttest.Must(fixture.Authority.Issue("edge.native.test", fixture.Leaves["edge"].Parsed.URIs[0].String(), time.Now().Add(-time.Hour), time.Now().Add(-time.Minute)))},
	} {
		fixture.Control.publishSecrets(t, translateEdgeTLSGeneration(t, fixture, "client-negative-"+negative.name, edgeTLSMaterial{Leaves: map[string]transporttest.Leaf{"edge": negative.leaf}}))
		lifecycle.restartCheck(t)
		// Check fails before protected HTTP receivers. Bypass Auth HTTP and Git
		// independently prove their native client validation; API/Event native-role
		// validation is owned by service-local tests, not inferred from this503.
		for _, role := range []string{"auth", "git-proxy"} {
			lifecycle.restartHTTP(t, role)
			edge.assertTLSRole(t, f, role, 503, 0, "")
		}
		edge.assertTLSRole(t, f, "api", 503, 0, "")
		fixture.Control.publishSecrets(t, fixture.Snapshot)
		lifecycle.restartCheck(t)
		for _, role := range []string{"auth", "git-proxy"} {
			lifecycle.restartHTTP(t, role)
			edge.assertTLSRole(t, f, role, edgeTLSRoleStatus(role), 1, fixture.Leaves["edge"].Parsed.SerialNumber.String())
		}
		edge.assertTLSRole(t, f, "api", 200, 1, fixture.Leaves["edge"].Parsed.SerialNumber.String())
	}
}

func edgeReplaceInlineTLSBytes(t *testing.T, source *corev3.DataSource, value []byte) {
	t.Helper()
	bytes, ok := source.GetSpecifier().(*corev3.DataSource_InlineBytes)
	if !ok {
		t.Fatal("declared upstream TLS material changed DataSource representation")
	}
	bytes.InlineBytes = append([]byte(nil), value...)
}
