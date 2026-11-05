package integration

import (
	"bytes"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

// Tests of POST /v1/client/drive-sync: drive sync reports from a test CLI
// over real HTTP to computer-hub, stored with their drive_sync input in
// PostgreSQL.

// rejectedWrite is a drive_sync of one rejected write, compact as Engine
// stores it.
const rejectedWrite = `{"committed":0,"deleted":0,"rejected":[{"path":"/tetral/drives/drv_notes/a.txt","change":"write","reason":"limit","rule":"max_bytes"}],"not_synced":[]}`

// driveSyncReport is the JSON of a drive sync report on connectionID for
// sessionID's bindingID, of taskID when set, with members extra after
// drive_sync.
func driveSyncReport(connectionID, messageID, sessionID, bindingID, taskID, driveSync, extra string) []byte {
	task := ""
	if taskID != "" {
		task = `"task_id":` + quoteJSON(taskID) + `,`
	}
	return []byte(`{"protocol_version":1,"connection_id":` + quoteJSON(connectionID) + `,"message_id":` + quoteJSON(messageID) +
		`,"session_id":` + quoteJSON(sessionID) + `,"binding_id":` + quoteJSON(bindingID) + `,` + task + `"drive_sync":` + driveSync + extra + `}`)
}

// bindingFor is the binding prepared for s on the stream's connection.
func (c *cliStream) bindingFor(s *toolSession) string {
	c.f.t.Helper()
	binding, _ := c.f.row(`SELECT binding_id FROM computer_bindings WHERE connection_id=$1 AND session_id=$2 AND state='prepared'`, c.connectionID, s.id)["binding_id"].(string)
	if binding == "" {
		c.f.t.Fatalf("no binding prepared for %s on %s", s.id, c.connectionID)
	}
	return binding
}

func (c *cliStream) driveSync(body []byte) enginetest.Response {
	c.f.t.Helper()
	response, err := c.cli.Post(c.f.ctx, "/v1/client/drive-sync", body)
	if err != nil {
		c.f.t.Fatal(err)
	}
	return response
}

// driveSyncAcked posts body and requires its ACK for messageID.
func (c *cliStream) driveSyncAcked(body []byte, messageID string) enginetest.Response {
	c.f.t.Helper()
	response := c.driveSync(body)
	if ack, err := response.Acknowledgement(); err != nil || ack.MessageID != messageID || ack.ConnectionID != c.connectionID {
		c.f.t.Fatalf("drive sync report %s = %d %s, %v; want its ACK", messageID, response.Status, response.Body, err)
	}
	return response
}

// driveSyncRefused posts body and requires status and code.
func (c *cliStream) driveSyncRefused(body []byte, status int, code protocol.ErrorCode) {
	c.f.t.Helper()
	response := c.driveSync(body)
	if failure, err := response.Error(); response.Status != status || err != nil || failure.Code != code {
		c.f.t.Fatalf("drive sync report = %d %s; want %d %s", response.Status, response.Body, status, code)
	}
}

// driveSyncs lists, for each stored drive sync report of sessionID in
// message_id order, its message_id, the thread and status of its input and
// the number of its active jobs, then the Session's drive_sync inputs and
// their active jobs.
func (f *machineTools) driveSyncs(sessionID string) []string {
	f.t.Helper()
	rows, err := f.admin.QueryContext(f.ctx, `SELECT r.message_id, inbox.session_thread_id, inbox.status,
		(SELECT count(*) FROM queue_jobs job WHERE job.kind='runtime_input' AND job.status IN ('pending','leased')
		   AND job.payload_json::jsonb->>'runtime_input_id'=r.runtime_input_id)
		FROM session_drive_sync_reports r JOIN session_runtime_inbox inbox ON inbox.workspace_id=r.workspace_id AND inbox.runtime_input_id=r.runtime_input_id
		WHERE r.session_id=$1 ORDER BY r.message_id`, sessionID)
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var listed []string
	for rows.Next() {
		var messageID, threadID, status string
		var jobs int
		if err := rows.Scan(&messageID, &threadID, &status, &jobs); err != nil {
			f.t.Fatal(err)
		}
		listed = append(listed, fmt.Sprintf("%s %s %s %d", messageID, threadID, status, jobs))
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	inputs := f.count(`SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1 AND input_kind='drive_sync'`, sessionID)
	jobs := f.count(`SELECT count(*) FROM queue_jobs WHERE kind='runtime_input' AND status IN ('pending','leased')
		AND payload_json::jsonb->>'session_id'=$1 AND payload_json::jsonb->>'input_kind'='drive_sync'`, sessionID)
	return append(listed, fmt.Sprintf("inputs %d jobs %d", inputs, jobs))
}

// A report sent again with its message_id after a lost ACK gets the
// identical ACK and stores nothing more: one report, one drive_sync input of
// the main thread and one job. The message_id with other content gets 409
// message_conflict and changes nothing.
func TestADriveSyncReportSentAgainGetsItsFirstAcknowledgement(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	binding := stream.bindingFor(s)
	body := driveSyncReport(stream.connectionID, "dsync_1", s.id, binding, "", rejectedWrite, "")
	first := stream.driveSyncAcked(body, "dsync_1")
	again := stream.driveSyncAcked(body, "dsync_1")
	if !bytes.Equal(first.Body, again.Body) {
		t.Fatalf("second ACK %s; want the first %s", again.Body, first.Body)
	}
	want := []string{"dsync_1 " + s.thread + " queued 1", "inputs 1 jobs 1"}
	if got := f.driveSyncs(s.id); !reflect.DeepEqual(got, want) {
		t.Fatalf("drive syncs = %q; want %q", got, want)
	}
	stored := f.row(`SELECT computer_id, binding_id, task_id, drive_sync_json FROM session_drive_sync_reports WHERE message_id='dsync_1'`)
	if stored["computer_id"] != computerID || stored["binding_id"] != binding || stored["task_id"] != nil || stored["drive_sync_json"] != rejectedWrite {
		t.Fatalf("stored report = %v; want the binding's computer and the drive_sync as sent", stored)
	}

	changed := strings.Replace(rejectedWrite, `"committed":0`, `"committed":1`, 1)
	stream.driveSyncRefused(driveSyncReport(stream.connectionID, "dsync_1", s.id, binding, "", changed, ""), http.StatusConflict, protocol.ErrorMessageConflict)
	if got := f.driveSyncs(s.id); !reflect.DeepEqual(got, want) {
		t.Fatalf("drive syncs after the conflict = %q; want %q", got, want)
	}
}

// A report is at most 131,072 bytes, whitespace included, and its
// drive_sync at most 65,536, with no downloads; a refused report stores
// nothing.
func TestDriveSyncReportsHaveTheirOwnBounds(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	_, stream := s.preparedUserComputer("laptop")
	binding := stream.bindingFor(s)
	sized := func(messageID string, size int) []byte {
		unpadded := driveSyncReport(stream.connectionID, messageID, s.id, binding, "", rejectedWrite, `,"padding":""`)
		return driveSyncReport(stream.connectionID, messageID, s.id, binding, "", rejectedWrite, `,"padding":"`+strings.Repeat("x", size-len(unpadded))+`"`)
	}
	var rejections []string
	for i := range 100 {
		rejections = append(rejections, `{"path":"/tetral/drives/drv_notes/`+fmt.Sprintf("%03d", i)+strings.Repeat("p", 640)+`","change":"write","reason":"limit","rule":"max_bytes"}`)
	}
	for _, c := range []struct {
		name   string
		body   []byte
		status int
		code   protocol.ErrorCode
	}{
		{"exactly 131,072 bytes", sized("dsync_largest", protocol.DriveSyncReportJSONBytes), http.StatusOK, ""},
		{"one byte of whitespace more", append(sized("dsync_over", protocol.DriveSyncReportJSONBytes), ' '), http.StatusRequestEntityTooLarge, protocol.ErrorRequestTooLarge},
		{"a download", driveSyncReport(stream.connectionID, "dsync_download", s.id, binding, "",
			strings.Replace(rejectedWrite, `"not_synced":[]`, `"not_synced":[],"downloading":[{"path":"/tetral/drives/drv_notes/b.bin","downloaded_bytes":1,"size_bytes":2}]`, 1), ""),
			http.StatusBadRequest, protocol.ErrorInvalidMessage},
		{"a drive_sync over 65,536 bytes", driveSyncReport(stream.connectionID, "dsync_long", s.id, binding, "",
			`{"committed":0,"deleted":0,"rejected":[`+strings.Join(rejections, ",")+`],"not_synced":[]}`, ""), http.StatusBadRequest, protocol.ErrorInvalidMessage},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.status == http.StatusOK {
				if len(c.body) != protocol.DriveSyncReportJSONBytes {
					t.Fatalf("the report is %d bytes", len(c.body))
				}
				stream.driveSyncAcked(c.body, "dsync_largest")
				return
			}
			stream.driveSyncRefused(c.body, c.status, c.code)
		})
	}
	if got, want := f.driveSyncs(s.id), []string{"dsync_largest " + s.thread + " queued 1", "inputs 1 jobs 1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("drive syncs = %q; want only the accepted report's %q", got, want)
	}
}

// binding_id must name a binding issued on the report's connection and
// task_id a task issued for the Session, on any connection: a report rebuilt
// on a new connection for a task of the ended one is accepted for the
// thread that made the call.
func TestADriveSyncReportNamesItsConnectionsBindingAndASessionTask(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	_, desktop := s.preparedUserComputer("desktop")
	other := f.session()
	otherComputer, otherStream := other.preparedUserComputer("server")
	_, otherTask := other.admitted(otherComputer, "true")
	otherStream.execute()
	_, task := s.admitted(computerID, "true")
	stream.execute()

	stream.driveSyncRefused(driveSyncReport(stream.connectionID, "dsync_binding", s.id, desktop.bindingFor(s), "", rejectedWrite, ""),
		http.StatusConflict, protocol.ErrorUnknownInstruction)
	stream.driveSyncRefused(driveSyncReport(stream.connectionID, "dsync_task", s.id, stream.bindingFor(s), otherTask, rejectedWrite, ""),
		http.StatusConflict, protocol.ErrorUnknownInstruction)

	f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, stream.connectionID)
	stream.close()
	fresh := f.connect(stream.cli, everyTool)
	fresh.prepare("")
	fresh.driveSyncAcked(driveSyncReport(fresh.connectionID, "dsync_rebuilt", s.id, fresh.bindingFor(s), task, rejectedWrite, ""), "dsync_rebuilt")
	if got, want := f.driveSyncs(s.id), []string{"dsync_rebuilt " + s.thread + " queued 1", "inputs 1 jobs 1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("drive syncs = %q; want only the rebuilt report's %q", got, want)
	}
	if stored := f.row(`SELECT task_id, connection_id FROM session_drive_sync_reports WHERE message_id='dsync_rebuilt'`); stored["task_id"] != task || stored["connection_id"] != fresh.connectionID {
		t.Fatalf("stored report = %v; want the task on the new connection", stored)
	}
}

// A report of a task goes to the thread that made the call while that
// thread is open, and to the main thread once it is closed or terminated; a
// report without task_id goes to the main thread.
func TestADriveSyncReportGoesToTheThreadThatMadeTheCall(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	binding := stream.bindingFor(s)
	tasks := map[string]string{}
	for _, child := range []string{"open", "closed", "terminated"} {
		thread := "sthr_" + child + "_" + s.id
		sessionfixture.SeedBridgeAPIChildThread(t, f.admin, string(f.ws), s.id, s.thread, thread)
		_, tasks[child] = s.in(thread).admitted(computerID, "true")
		stream.execute()
	}
	f.exec(`UPDATE session_threads SET status='closed_for_runtime' WHERE id=$1`, "sthr_closed_"+s.id)
	f.exec(`UPDATE session_threads SET status='terminated' WHERE id=$1`, "sthr_terminated_"+s.id)
	for _, report := range [][2]string{{"dsync_a_open", tasks["open"]}, {"dsync_b_closed", tasks["closed"]}, {"dsync_c_terminated", tasks["terminated"]}, {"dsync_d_none", ""}} {
		stream.driveSyncAcked(driveSyncReport(stream.connectionID, report[0], s.id, binding, report[1], rejectedWrite, ""), report[0])
	}
	want := []string{"dsync_a_open sthr_open_" + s.id + " queued 1", "dsync_b_closed " + s.thread + " queued 1",
		"dsync_c_terminated " + s.thread + " queued 1", "dsync_d_none " + s.thread + " queued 1", "inputs 4 jobs 4"}
	if got := f.driveSyncs(s.id); !reflect.DeepEqual(got, want) {
		t.Fatalf("drive syncs = %q; want %q", got, want)
	}
}
