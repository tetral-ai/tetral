package computerhub

import (
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/protocol"
)

// A pass that takes a queued drive.changed with instructions released before
// and after it was queued writes it between them, in that order. A later
// notice of the same drive, queued after the second instruction, merges into
// it: one drive.changed at the earlier place with the higher seq.
func TestPassWritesADriveNoticeBetweenTheInstructionsAroundIt(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	_, stream := f.openOnline()
	issueSeq := func(messageID string) int64 {
		t.Helper()
		var seq int64
		if err := f.admin.QueryRowContext(f.ctx, `SELECT issue_seq FROM connection_instructions WHERE message_id=$1`, messageID).Scan(&seq); err != nil {
			t.Fatal(err)
		}
		return seq
	}
	f.streams.mu.Lock()
	held := f.streams.streams[stream.connectionID]
	f.streams.mu.Unlock()

	// Holding the stream's lock keeps the pod's handling of both
	// instructions' notices and the stream's next take until both are
	// queued, so one pass takes all three.
	held.mu.Lock()
	before := f.issueInstruction(f.ctx, stream.connectionID, "sess_before")
	held.bound = issueSeq(before)
	held.notices = mergeNotice(held.notices, driveNotice{driveID: "drive_between", seq: 7, after: held.bound})
	after := f.issueInstruction(f.ctx, stream.connectionID, "sess_after")
	held.bound = issueSeq(after)
	held.notices = mergeNotice(held.notices, driveNotice{driveID: "drive_between", seq: 9, after: held.bound})
	held.mu.Unlock()
	held.wakeOnly()

	f.requireInstructions(stream, before)
	if event := stream.next(); event.Envelope.Payload != (protocol.DriveChangedPayload{DriveID: "drive_between", Seq: 9}) {
		t.Fatalf("event %s; want drive.changed of drive_between at seq 9", event.Data)
	}
	f.requireInstructions(stream, after)
	requireNoEvent(t, stream)
}
