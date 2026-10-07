package testinfra

import (
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNATSRolePermissionsThroughActualBroker(t *testing.T) {
	fixture, err := LoadNATSFixture()
	if err != nil {
		t.Fatal(err)
	}
	connect := func(role NATSRole) (*nats.Conn, chan error) {
		user, err := os.ReadFile(role.UserPath)
		if err != nil {
			t.Fatal("read NATS role user")
		}
		password, err := os.ReadFile(role.PasswordPath)
		if err != nil {
			t.Fatal("read NATS role password")
		}
		denied := make(chan error, 4)
		connection, err := nats.Connect(fixture.Servers[0], nats.UserInfo(string(user), string(password)), nats.NoReconnect(), nats.Timeout(time.Second), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			select {
			case denied <- err:
			default:
			}
		}))
		if err != nil {
			t.Fatal("connect NATS authorized role")
		}
		t.Cleanup(connection.Close)
		return connection, denied
	}
	producer, publishErrors := connect(fixture.Publisher)
	consumer, subscribeErrors := connect(fixture.Subscriber)
	sub, err := consumer.SubscribeSync("preview.v1.acl.test")
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	check := func(marker string) {
		if err := producer.Publish("preview.v1.acl.test", []byte(marker)); err != nil {
			t.Fatal(err)
		}
		if err := producer.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		message, err := sub.NextMsg(time.Second)
		if err != nil || string(message.Data) != marker {
			t.Fatal("authorized positive control did not arrive in order")
		}
	}
	check("before")
	if _, err := producer.SubscribeSync("preview.v1.acl.test"); err != nil {
		t.Fatal(err)
	}
	if err := producer.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	awaitDenied := func(ch <-chan error) {
		select {
		case err := <-ch:
			if !errors.Is(err, nats.ErrPermissionViolation) {
				t.Fatal("broker rejection was not a subject permission denial")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("broker did not reject unauthorized operation")
		}
	}
	awaitDenied(publishErrors)
	if err := consumer.Publish("preview.v1.acl.test", []byte("unauthorized")); err != nil {
		t.Fatal(err)
	}
	if err := consumer.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	awaitDenied(subscribeErrors)
	check("after")
}

// Local brokers run the release's client policy. The expected roles come from
// the deployed contract: the publisher may only publish previews and the
// subscriber may only subscribe to them, each explicitly denying the other
// operation; the hardened client listener verifies clients with TLS first.
func TestNATSBrokerPolicyProjectsReleaseValues(t *testing.T) {
	policy, err := NATSBrokerPolicy(testRepositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	previews, deny := []any{"preview.v1.>"}, map[string]any{"deny": []any{">"}}
	if !reflect.DeepEqual(policy.PublisherPermissions, map[string]any{"publish": previews, "subscribe": deny}) ||
		!reflect.DeepEqual(policy.SubscriberPermissions, map[string]any{"publish": deny, "subscribe": previews}) {
		t.Fatalf("release roles projected onto the wrong fixture users: publisher=%v subscriber=%v", policy.PublisherPermissions, policy.SubscriberPermissions)
	}
	if policy.MaxPayload <= 0 || policy.ClientTLS["verify"] != true || policy.ClientTLS["handshake_first"] != true || policy.RouteTLS["verify"] != true {
		t.Fatalf("release payload ceiling or TLS options missing: %+v", policy)
	}
	authorization, err := policy.AuthorizationBlock("publisher-user", "publisher-password", "subscriber-user", "subscriber-password")
	if err != nil {
		t.Fatal(err)
	}
	// The NATS parser must receive the literal wildcard, not a JSON \u escape.
	if !strings.Contains(authorization, `"preview.v1.>"`) {
		t.Fatalf("authorization does not keep subject wildcards literal: %s", authorization)
	}
	var granted struct {
		Users []struct {
			User, Password string
			Permissions    map[string]any
		}
	}
	if err := json.Unmarshal([]byte(authorization), &granted); err != nil || len(granted.Users) != 2 ||
		granted.Users[0].User != "publisher-user" || granted.Users[0].Password != "publisher-password" || !reflect.DeepEqual(granted.Users[0].Permissions, policy.PublisherPermissions) ||
		granted.Users[1].User != "subscriber-user" || granted.Users[1].Password != "subscriber-password" || !reflect.DeepEqual(granted.Users[1].Permissions, policy.SubscriberPermissions) {
		t.Fatalf("fixture credentials lost their release role: %s", authorization)
	}
	route, err := policy.RouteTLSBlock(NATSFiles{CAPath: "/fixture/ca.crt", CertPath: "/fixture/route.crt", KeyPath: "/fixture/route.key"})
	if err != nil {
		t.Fatal(err)
	}
	var routeTLS map[string]any
	if err := json.Unmarshal([]byte(route), &routeTLS); err != nil || routeTLS["verify"] != true || routeTLS["cert_file"] != "/fixture/route.crt" || routeTLS["key_file"] != "/fixture/route.key" || routeTLS["ca_file"] != "/fixture/ca.crt" {
		t.Fatalf("route TLS lost release options or fixture files: %s", route)
	}
}

// Changing one side of the projection, the release values, must fail it
// rather than start brokers with a policy the release does not declare.
func TestNATSBrokerPolicyRejectsDriftedReleaseValues(t *testing.T) {
	source := filepath.Join(testRepositoryRoot(t), "deploy", "nats")
	release := map[string]string{}
	for _, name := range []string{"values.yaml", "values-hardened.yaml"} {
		data, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			t.Fatal(err)
		}
		release[name] = string(data)
	}
	for _, item := range []struct{ name, file, old, new string }{
		{"unchanged control", "values.yaml", "", ""},
		{"renamed subscriber placeholder", "values.yaml", "<< $NATS_SUBSCRIBER_USER >>", "<< $NATS_READER_USER >>"},
		{"subscriber role replaced by a second publisher", "values.yaml", "user: << $NATS_SUBSCRIBER_USER >>\n          password: << $NATS_SUBSCRIBER_PASSWORD >>", "user: << $NATS_PUBLISHER_USER >>\n          password: << $NATS_PUBLISHER_PASSWORD >>"},
		{"mismatched password placeholder", "values.yaml", "password: << $NATS_PUBLISHER_PASSWORD >>", "password: << $NATS_SUBSCRIBER_PASSWORD >>"},
		{"renamed payload ceiling", "values.yaml", "max_payload:", "max_payload_bytes:"},
		{"missing client TLS options", "values-hardened.yaml", "secretName: tetral-nats-server-tls\n      merge:", "secretName: tetral-nats-server-tls\n      options:"},
	} {
		t.Run(item.name, func(t *testing.T) {
			files := maps.Clone(release)
			if item.old != "" {
				if strings.Count(files[item.file], item.old) != 1 {
					t.Fatalf("mutation target %q is not unique in %s", item.old, item.file)
				}
				files[item.file] = strings.Replace(files[item.file], item.old, item.new, 1)
			}
			root := t.TempDir()
			directory := filepath.Join(root, "deploy", "nats")
			if err := os.MkdirAll(directory, 0o755); err != nil {
				t.Fatal(err)
			}
			for name, body := range files {
				writeTestFile(t, directory, name, body)
			}
			_, err := NATSBrokerPolicy(root)
			if item.old == "" && err != nil {
				t.Fatalf("unchanged release values did not project: %v", err)
			}
			if item.old != "" && err == nil {
				t.Fatal("drifted release values projected a broker policy")
			}
		})
	}
}
