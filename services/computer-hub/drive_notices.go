package computerhub

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Drive change notices. Every pod listens on drive.NoticeChannel on the
// LISTEN connection of its connection notices (Run), so it receives both in
// the commit order of their transactions, and handles them one at a time. A
// commit's notice selects, among the streams the pod holds in its
// Workspace, those whose ready declared drives.mount and whose connection
// has a prepared or closing binding with the drive in its drive set
// (computer.DriveListeners), and queues drive.changed on each, behind the
// instructions the notices before it released (offerDrive). A queued,
// unsent notice of the same drive takes the higher seq and keeps its place.
// A stream joins this routing when the pod holds it, before connected.
//
// A stream's first pass, and each held stream's next pass each time the
// LISTEN connection is in place, writes, before any instruction,
// drive.changed with the last_seq of every drive with a commit in its
// connection's prepared or closing bindings' drive sets, read after the
// pass's instructions, so none of them comes before a commit whose notice
// was lost or not handled yet (resentDrives). A selection that fails asks
// the streams it chose among for the same.

// driveNotice is a drive.changed queued on a stream. after is the stream's
// bound when it was queued: it goes after the instructions up to that
// issue_seq and before every later one.
type driveNotice struct {
	driveID string
	seq     int64
	after   int64
}

// driveNotice handles one payload of drive.NoticeChannel; a malformed one is
// skipped.
func (s *Streams) driveNotice(ctx context.Context, payload string) {
	var notice drive.Notice
	if json.Unmarshal([]byte(payload), &notice) != nil || notice.WorkspaceID == "" || notice.DriveID == "" || notice.Seq <= 0 {
		return
	}
	ws := workspace.ID(notice.WorkspaceID)
	streams := map[string]*clientStream{}
	connections := []string{}
	for _, stream := range s.held() {
		if stream.workspace == ws {
			streams[stream.connection] = stream
			connections = append(connections, stream.connection)
		}
	}
	if len(connections) == 0 {
		return
	}
	var listeners []string
	routeCtx, cancel := context.WithTimeout(ctx, passDeadline)
	defer cancel()
	err := s.database.WithWorkspaceReadOnlyTx(routeCtx, string(ws), "computer_hub.drive_notice_route", func(tx *dbconnect.Tx) error {
		var err error
		listeners, err = computer.DriveListeners(routeCtx, tx, ws, notice.DriveID, connections)
		return err
	})
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		s.logger.WarnContext(ctx, "drive_notices.route_failed",
			slog.String("operation", "computer_hub.drive_notice_route"),
			slog.String("error.class", "dependency_unavailable"),
			slog.String("error.code", "drive_notice_route_failed"),
			slog.String("error.message_safe", "drive notice routing failed; the streams send their drives' last commits again"))
		for _, stream := range streams {
			stream.resendDrives()
		}
		return
	}
	for _, connectionID := range listeners {
		streams[connectionID].offerDrive(notice.DriveID, notice.Seq)
	}
}

// offerDrive queues drive.changed {driveID, seq} behind the instructions the
// stream's bound releases, or raises the seq of the unsent notice of driveID,
// which keeps its place, and wakes the stream.
func (c *clientStream) offerDrive(driveID string, seq int64) {
	c.mu.Lock()
	c.notices = mergeNotice(c.notices, driveNotice{driveID: driveID, seq: seq, after: c.bound})
	c.mu.Unlock()
	c.wakeOnly()
}

// resendDrives asks the stream's next pass to send the last commit of each
// drive its connection's bindings hold, and wakes it.
func (c *clientStream) resendDrives() {
	c.mu.Lock()
	c.resend = true
	c.mu.Unlock()
	c.wakeOnly()
}

// requeue puts back notices and the resend request a pass left, ahead of the
// notices queued since.
func (c *clientStream) requeue(notices []driveNotice, resend bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	merged := append([]driveNotice{}, notices...)
	for _, later := range c.notices {
		merged = mergeNotice(merged, later)
	}
	c.notices, c.resend = merged, c.resend || resend
}

// mergeNotice adds notice to notices, or raises the seq of the one of the
// same drive, which keeps its place.
func mergeNotice(notices []driveNotice, notice driveNotice) []driveNotice {
	for i := range notices {
		if notices[i].driveID == notice.driveID {
			notices[i].seq = max(notices[i].seq, notice.seq)
			return notices
		}
	}
	return append(notices, notice)
}

// releasedNotices splits notices into those a pass reading instructions up
// to upTo writes, and those it keeps queued, both in queue order.
func releasedNotices(notices []driveNotice, upTo int64) (released, kept []driveNotice) {
	for _, notice := range notices {
		if notice.after <= upTo {
			released = append(released, notice)
		} else {
			kept = append(kept, notice)
		}
	}
	return released, kept
}

// resentDrives is drive.changed with the last_seq of every drive with a
// commit in the drive sets of connection's prepared or closing bindings, when
// its ready declared drives.mount.
func resentDrives(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection) ([]protocol.DriveChangedPayload, error) {
	capabilities, err := protocol.DecodeCapabilities(connection.Capabilities)
	if err != nil || !capabilities.Drives.Mount {
		return nil, nil
	}
	drives, err := computer.ConnectionDrives(ctx, tx, ws, connection.ID)
	if err != nil {
		return nil, err
	}
	return drive.LastSeqs(ctx, tx, ws, drives)
}

// uncovered is notices without those whose seq a resent notice of the same
// drive reaches.
func uncovered(notices []driveNotice, resent []protocol.DriveChangedPayload) []driveNotice {
	covered := map[string]int64{}
	for _, notice := range resent {
		covered[notice.DriveID] = notice.Seq
	}
	var kept []driveNotice
	for _, notice := range notices {
		if seq, found := covered[notice.driveID]; !found || notice.seq > seq {
			kept = append(kept, notice)
		}
	}
	return kept
}
