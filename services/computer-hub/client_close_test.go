package computerhub

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
)

func TestClosedReportStoresTerminalDiagnosticsOnce(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			f := newHubFixture(t, 720*time.Hour)
			cli := f.userInstance(time.Time{})
			const sessionID = "sesn_close"
			f.userComputer(cli, sessionID)
			stream := f.open(cli)
			f.acceptReady(cli, stream, "ready_close")
			prepare, payload := prepareOn(t, stream)
			acknowledged(t, f.report(cli, prepare, "prepared", prepared("/Users/me/shop")), "prepared")
			f.ownerTx(func(tx *dbconnect.Tx) error {
				_, err := computer.Detach(f.ctx, tx, f.ws, sessionID, "comp_"+sessionID, false, time.Now())
				return err
			})
			event := stream.next()
			if event.Envelope.Payload.MessageType() != "session.close" {
				t.Fatalf("close instruction: %s", event.Data)
			}
			closeID := event.Envelope.MessageID
			report := protocol.SessionStatus{ConnectionID: prepare.Envelope.ConnectionID, MessageID: "closed", RequestMessageID: closeID, SessionID: sessionID, BindingID: payload.BindingID, Status: "closed"}
			if failure {
				report.CleanupFailures = protocol.Some([]protocol.CleanupFailure{{Path: "/owned/repository", Message: "Permission denied"}})
				report.LostChanges = protocol.Some([]protocol.LostChange{{RequestMessageID: closeID, DriveID: "drive_example", Paths: []string{"notes.txt"}, PathsTotal: 1}, {RequestMessageID: prepare.Envelope.MessageID, DriveID: "drive_wrong", Paths: []string{"unrelated.txt"}, PathsTotal: 1}})
			}
			for _, mutate := range []func(*protocol.SessionStatus){
				func(s *protocol.SessionStatus) { s.RequestMessageID = prepare.Envelope.MessageID },
				func(s *protocol.SessionStatus) { s.SessionID = "sesn_wrong" },
				func(s *protocol.SessionStatus) { s.BindingID = "bind_0000000000000000" },
			} {
				invalid := report
				mutate(&invalid)
				response, err := cli.Post(f.ctx, "/v1/client/session-status", protocol.Encode(invalid))
				if err != nil {
					t.Fatal(err)
				}
				requireClientError(t, response, nil, protocol.ErrorUnknownInstruction)
				if f.bindingState(payload.BindingID) != computer.BindingClosing || len(f.sessionErrors(sessionID)) != 0 {
					t.Fatal("refused report mutated closure")
				}
			}
			if failure {
				// The report sent is exactly 65,536 bytes; one byte more is
				// refused and changes nothing.
				sized := func(size int) protocol.SessionStatus {
					padded := report
					failures := append([]protocol.CleanupFailure{}, report.CleanupFailures.Value...)
					for {
						failures = append(failures, protocol.CleanupFailure{Path: fmt.Sprintf("/owned/padding/%03d", len(failures))})
						padded.CleanupFailures = protocol.Some(failures)
						missing := size - len(protocol.Encode(padded))
						failures[len(failures)-1].Message = strings.Repeat("x", min(missing, 1024))
						if missing <= 1024 {
							return padded
						}
					}
				}
				over, err := cli.Invalid().Post(f.ctx, "/v1/client/session-status", protocol.Encode(sized(protocol.DefaultMessageJSONBytes+1)))
				if err != nil {
					t.Fatal(err)
				}
				requireClientError(t, over, nil, protocol.ErrorRequestTooLarge)
				if f.bindingState(payload.BindingID) != computer.BindingClosing || len(f.sessionErrors(sessionID)) != 0 {
					t.Fatal("a closed report over the limit changed the closure")
				}
				report = sized(protocol.DefaultMessageJSONBytes)
			}
			first, err := cli.Post(f.ctx, "/v1/client/session-status", protocol.Encode(report))
			if err != nil {
				t.Fatal(err)
			}
			acknowledged(t, first, "closed")
			replay, err := cli.Post(f.ctx, "/v1/client/session-status", protocol.Encode(report))
			if err != nil {
				t.Fatal(err)
			}
			if replay.Status != http.StatusOK || string(replay.Body) != string(first.Body) {
				t.Fatal("replay did not return saved ACK")
			}
			// A different upstream envelope for the same close, of the same
			// size, is also event-idempotent.
			report.MessageID = "second"
			again, err := cli.Post(f.ctx, "/v1/client/session-status", protocol.Encode(report))
			if err != nil {
				t.Fatal(err)
			}
			acknowledged(t, again, "second")
			if f.bindingState(payload.BindingID) != computer.BindingClosed || f.row(`SELECT execution_state FROM computers WHERE id=$1`, "comp_"+sessionID)["execution_state"] != "closed" {
				t.Fatal("binding/computer did not close")
			}
			events := f.sessionErrors(sessionID)
			if !failure {
				if len(events) != 0 {
					t.Fatalf("success events: %v", events)
				}
				return
			}
			if len(events) != 2 || events[0]["type"] != "computer_cleanup_error" || events[1]["type"] != "drive_changes_lost_error" {
				t.Fatalf("close events: %v", events)
			}
			for _, event := range events {
				if event["computer_id"] != "comp_"+sessionID || event["retry_status"].(map[string]any)["type"] != "terminal" {
					t.Fatalf("terminal close event: %v", event)
				}
			}
			waitUntil(t, func() bool { return strings.Contains(f.logs.String(), `"event":"client.lost_changes_ignored"`) })
			if strings.Count(f.logs.String(), `"event":"client.lost_changes_ignored"`) != 1 {
				t.Fatal("mismatched entry must be logged once, not on replay")
			}
			paths := events[0]["cleanup_failures"].([]any)
			if len(paths) != len(report.CleanupFailures.Value) || paths[0].(map[string]any)["path"] != "/owned/repository" || paths[0].(map[string]any)["message"] != "Permission denied" {
				t.Fatalf("cleanup diagnostics: %.200v", paths)
			}
		})
	}
}

func TestRequiredRepositoryPreparationFailureIsNotReady(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.cloudComputer("sesn_required_repository", true)
	stream := f.open(cli)
	f.acceptReady(cli, stream, "ready_repository")
	prepare, payload := prepareOn(t, stream)
	acknowledged(t, f.report(cli, prepare, "failed_repository", failed("repository_preparation_failed")), "failed_repository")
	if f.bindingState(payload.BindingID) != computer.BindingFailed {
		t.Fatal("required repository failure became ready")
	}
	events := f.sessionErrors(payload.SessionID)
	if len(events) != 1 || events[0]["type"] != "computer_preparation_error" || events[0]["code"] != "repository_preparation_failed" {
		t.Fatalf("preparation events: %v", events)
	}
}
