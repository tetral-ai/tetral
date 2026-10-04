package testinfra

import (
	"errors"
	"os"
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
