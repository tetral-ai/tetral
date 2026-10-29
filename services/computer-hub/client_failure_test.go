package computerhub

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
)

func TestFirstClientMessageRejectsCrossRouteReplay(t *testing.T) {
	for _, route := range []string{"ready", "heartbeat"} {
		t.Run(route, func(t *testing.T) {
			f := newHubFixture(t, 720*time.Hour)
			cli := f.userInstance(time.Time{})
			stream := f.open(cli)
			const messageID = "shared_message"
			if route == "heartbeat" {
				f.acceptReady(cli, stream, messageID)
			} else {
				response, err := cli.ProtocolError(f.ctx, protocol.ProtocolErrorReport{ConnectionID: stream.connectionID, MessageID: messageID, Error: protocol.ProtocolError{Code: "invalid_message", Message: "invalid instruction"}})
				if err != nil || response.Status != http.StatusOK {
					t.Fatalf("setup = %d, %v", response.Status, err)
				}
			}
			before := f.connectionRow(stream.connectionID)
			ledger := f.count(`SELECT count(*) FROM connection_upstream_messages WHERE connection_id=$1`, stream.connectionID)
			var response enginetest.Response
			var err error
			if route == "ready" {
				response, err = cli.Ready(f.ctx, readyOf(stream.connectionID, messageID, "1.0.0"))
			} else {
				seq := int64(instance.HeartbeatSeqStride + 1)
				response, err = cli.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: stream.connectionID, MessageID: messageID, RequestMessageID: instance.HeartbeatMessageID(stream.connectionID, seq), HeartbeatSeq: seq})
			}
			requireClientError(t, response, err, protocol.ErrorMessageConflict)
			if after := f.connectionRow(stream.connectionID); !reflect.DeepEqual(before, after) {
				t.Fatalf("conflict changed connection: before=%v after=%v", before, after)
			}
			if got := f.count(`SELECT count(*) FROM connection_upstream_messages WHERE connection_id=$1`, stream.connectionID); got != ledger {
				t.Fatalf("ledger rows = %d; want %d", got, ledger)
			}
		})
	}
}

func TestClientDependencyFailureDoesNotAcceptFirstMessage(t *testing.T) {
	for _, tc := range []struct{ name, route, statement string }{
		{"ready authority", "ready", "FROM instance_credentials WHERE workspace_id=$1 AND instance_id=$2 AND id=$3"},
		{"ready replay ledger", "ready", "SELECT route,fingerprint,status,body FROM connection_upstream_messages"},
		{"heartbeat replay ledger", "heartbeat", "SELECT route,fingerprint,status,body FROM connection_upstream_messages"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newHubFixture(t, 720*time.Hour)
			cli := f.userInstance(time.Time{})
			stream := f.open(cli)
			if tc.route == "heartbeat" {
				f.acceptReady(cli, stream, "ready_before_failure")
			}
			if tc.route == "heartbeat" {
				f.clock.waitTimers(t, 2)
			} else {
				f.clock.waitTimers(t, 1)
			}
			before := f.connectionRow(stream.connectionID)
			ledger := f.count(`SELECT count(*) FROM connection_upstream_messages WHERE connection_id=$1`, stream.connectionID)
			f.failOn(tc.statement)
			var response enginetest.Response
			var err error
			if tc.route == "ready" {
				response, err = cli.Ready(f.ctx, readyOf(stream.connectionID, "fresh_ready", "1.0.0"))
			} else {
				seq := int64(instance.HeartbeatSeqStride + 1)
				response, err = cli.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: stream.connectionID, MessageID: "fresh_heartbeat", RequestMessageID: instance.HeartbeatMessageID(stream.connectionID, seq), HeartbeatSeq: seq})
			}
			if err != nil {
				t.Fatal(err)
			}
			if f.fail.Load() != nil {
				t.Fatal("request did not reach the selected dependency statement")
			}
			if response.Status != http.StatusServiceUnavailable {
				t.Fatalf("dependency response = %d %s; want 503", response.Status, response.Body)
			}
			var body struct {
				Type  string `json:"type"`
				Error struct {
					Type    string `json:"type"`
					Message string `json:"message"`
				} `json:"error"`
			}
			decodeJSON(t, response.Body, &body)
			if body.Type != "error" || body.Error.Type != "api_error" || body.Error.Message != "the connection store is unavailable" {
				t.Fatalf("dependency body = %s", response.Body)
			}
			if after := f.connectionRow(stream.connectionID); !reflect.DeepEqual(before, after) {
				t.Fatalf("dependency failure changed connection: before=%v after=%v", before, after)
			}
			if got := f.count(`SELECT count(*) FROM connection_upstream_messages WHERE connection_id=$1`, stream.connectionID); got != ledger {
				t.Fatalf("ledger rows = %d; want %d", got, ledger)
			}
		})
	}
}
