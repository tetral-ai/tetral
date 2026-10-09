package agentruntimebridge

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/sessioneventwrite"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// fencedFeedWriter holds the Session runtime-mutation fence in its own Bridge
// transaction until released, then optionally writes one event and commits or
// rolls back.
type fencedFeedWriter struct {
	locked   chan struct{}
	proceed  chan bool
	position chan int64
	done     chan error
}

func startFencedFeedWriter(t *testing.T, f contentDeclarationFixture, eventID string) *fencedFeedWriter {
	t.Helper()
	writer := &fencedFeedWriter{locked: make(chan struct{}), proceed: make(chan bool, 1), position: make(chan int64, 1), done: make(chan error, 1)}
	rollback := errors.New("roll back the fenced writer")
	go func() {
		err := f.store.Client.WithWorkspaceTx(f.ctx, f.scope.WorkspaceId, "agentruntimebridge.test_fenced_feed", func(tx *dbconnect.Tx) error {
			if err := runtimecontrol.LockRuntimeMutationSessionTx(f.ctx, tx, f.scope.WorkspaceId, f.scope.SessionId); err != nil {
				return err
			}
			close(writer.locked)
			commit := <-writer.proceed
			if eventID == "" {
				return nil
			}
			now := time.Now().UTC()
			sequence, err := runtimecontrol.NextSessionEventSequenceTx(f.ctx, tx, f.scope)
			if err != nil {
				return err
			}
			position, err := sessioneventwrite.InsertInitialTx(f.ctx, tx, sessioneventwrite.InitialEvent{
				WorkspaceID: f.scope.WorkspaceId, SessionID: f.scope.SessionId, SessionThreadID: f.scope.SessionThreadId,
				EventID: eventID, Sequence: sequence,
				Type: "session.status_running", PayloadJSON: `{"type":"session.status_running"}`,
				Visibility: "public", SessionVisible: true, CreatedAt: now, ProcessedAt: &now,
			})
			if err != nil {
				return err
			}
			writer.position <- position
			if !commit {
				return rollback
			}
			return nil
		})
		if errors.Is(err, rollback) {
			err = nil
		}
		writer.done <- err
	}()
	select {
	case <-writer.locked:
	case err := <-writer.done:
		t.Fatalf("fenced writer failed before holding the fence: %v", err)
	case <-f.ctx.Done():
		t.Fatal("fenced writer did not take the Session fence")
	}
	return writer
}

func (writer *fencedFeedWriter) finish(t *testing.T, commit bool) int64 {
	t.Helper()
	writer.proceed <- commit
	if err := <-writer.done; err != nil {
		t.Fatalf("fenced writer: %v", err)
	}
	select {
	case position := <-writer.position:
		return position
	default:
		return 0
	}
}

// waitForSessionFenceWaiter returns once another backend of this database is
// waiting on a lock, which here is only the Session fence held by the paused
// writer.
func waitForSessionFenceWaiter(t *testing.T, f contentDeclarationFixture) {
	t.Helper()
	for {
		var waiting int
		if err := f.admin.QueryRowContext(f.ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND state = 'active'`).Scan(&waiting); err != nil {
			t.Fatalf("observe Session fence waiters: %v", err)
		}
		if waiting > 0 {
			return
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("concurrent writer never waited on the Session fence")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func feedPositionsAfter(t *testing.T, f contentDeclarationFixture, sessionID string, cursor int64) []int64 {
	t.Helper()
	rows, err := f.admin.QueryContext(f.ctx, `SELECT stream_position FROM session_event_stream_changes
		WHERE workspace_id=$1 AND session_id=$2 AND stream_position > $3 ORDER BY stream_position`, f.scope.WorkspaceId, sessionID, cursor)
	if err != nil {
		t.Fatalf("read feed after %d: %v", cursor, err)
	}
	defer func() { _ = rows.Close() }()
	var positions []int64
	for rows.Next() {
		var position int64
		if err := rows.Scan(&position); err != nil {
			t.Fatal(err)
		}
		positions = append(positions, position)
	}
	return positions
}

func eventInsertPosition(t *testing.T, f contentDeclarationFixture, eventID string) int64 {
	t.Helper()
	var position int64
	if err := f.admin.QueryRowContext(f.ctx, `SELECT insert_stream_position FROM session_events WHERE event_id=$1`, eventID).Scan(&position); err != nil {
		t.Fatalf("read insert position of %s: %v", eventID, err)
	}
	return position
}

// A feed position is reserved only after the writer holds the Session fence:
// a writer on another Thread of the same Session waits and commits at a later
// position, so a reader that advanced through the first commit still sees
// the second. A rolled-back reservation leaves a legal gap. Other Sessions
// are not serialized behind the fence.
func TestPostgreSQLFeedPositionsFollowTheSessionFence(t *testing.T) {
	f := newContentDeclarationFixture(t)
	const childID = "thr_feed_fence_child"
	sessionfixture.SeedBridgeAPIChildThread(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, childID)
	childScope := runtimecontrol.ScopeForThread(f.scope, childID)
	writeChild := func(writeID string) (chan string, chan error) {
		eventIDs, errs := make(chan string, 1), make(chan error, 1)
		go func() {
			response, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{
				Scope: childScope, RuntimeWriteId: writeID, EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`,
			})
			if err == nil && response.GetCommitted() == nil {
				err = fmt.Errorf("child write %s = %#v; want committed", writeID, response)
			}
			if err != nil {
				errs <- err
				return
			}
			eventIDs <- response.GetCommitted().GetEventId()
		}()
		return eventIDs, errs
	}
	awaitChild := func(eventIDs chan string, errs chan error) string {
		t.Helper()
		select {
		case eventID := <-eventIDs:
			return eventID
		case err := <-errs:
			t.Fatalf("child write: %v", err)
		case <-f.ctx.Done():
			t.Fatal("child write did not finish")
		}
		return ""
	}

	first := startFencedFeedWriter(t, f, "evt_feed_fence_first")
	childIDs, childErrs := writeChild("feed-fence-child")
	waitForSessionFenceWaiter(t, f)
	firstPosition := first.finish(t, true)
	if seen := feedPositionsAfter(t, f, f.scope.SessionId, 0); len(seen) != 1 || seen[0] != firstPosition {
		t.Fatalf("reader before the waiting writer commits sees %v; want only %d", seen, firstPosition)
	}
	childPosition := eventInsertPosition(t, f, awaitChild(childIDs, childErrs))
	if childPosition <= firstPosition {
		t.Fatalf("waiting writer committed at %d, not after the fence holder's %d", childPosition, firstPosition)
	}
	if seen := feedPositionsAfter(t, f, f.scope.SessionId, firstPosition); len(seen) != 1 || seen[0] != childPosition {
		t.Fatalf("reader advanced through %d sees %v; want the waiting writer at %d", firstPosition, seen, childPosition)
	}

	rolledBack := startFencedFeedWriter(t, f, "evt_feed_fence_rolled_back")
	childIDs, childErrs = writeChild("feed-fence-child-after-rollback")
	waitForSessionFenceWaiter(t, f)
	reserved := rolledBack.finish(t, false)
	afterRollback := eventInsertPosition(t, f, awaitChild(childIDs, childErrs))
	var rolledBackRows int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT
		(SELECT count(*) FROM session_events WHERE event_id='evt_feed_fence_rolled_back') +
		(SELECT count(*) FROM session_event_stream_changes WHERE event_id='evt_feed_fence_rolled_back')`).Scan(&rolledBackRows); err != nil {
		t.Fatal(err)
	}
	if rolledBackRows != 0 || reserved <= childPosition || afterRollback <= reserved {
		t.Fatalf("rolled-back writer rows=%d reserved=%d; later writer at %d after %d; want no rows and a later position past the gap",
			rolledBackRows, reserved, afterRollback, childPosition)
	}

	other := seedContentDeclarationScope(t, f.admin, f.workload.DB, f.scope.WorkspaceId)
	held := startFencedFeedWriter(t, f, "")
	response, err := other.store.WriteEvent(other.ctx, &bridgev1.WriteEventRequest{
		Scope: other.scope, RuntimeWriteId: "feed-fence-other-session", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`,
	})
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("other Session write while the fence is held = %#v/%v", response, err)
	}
	held.finish(t, true)
}
