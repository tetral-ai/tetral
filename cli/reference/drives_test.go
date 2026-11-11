package reference

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// driveClient sends drive transfer requests for inst_a's binding bind_1.
type driveClient struct {
	ts           *testServer
	connectionID string
}

// mounted brings inst_a online with drive mounting declared and stores the
// prepared report of bind_1 of sess_1, which holds drive_1 read_write and
// drive_ro read_only.
func (ts *testServer) mounted() (*eventStream, *driveClient) {
	ts.t.Helper()
	stream := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
	connectionID := stream.expect("connected")["connection_id"].(string)
	ready := readyBody(connectionID, "ready_1")
	ready["capabilities"] = map[string]any{"session_preparation": true, "drives": map[string]any{"mount": true}}
	if answer := ts.post("ready", "tetral_ck_a", ready); answer.status != http.StatusOK {
		ts.t.Fatalf("ready: %d %s", answer.status, answer.body)
	}
	prepare := preparePayload("sess_1", "bind_1")
	prepare["drives"] = []any{
		map[string]any{"drive_id": "drive_1", "access": "read_write"},
		map[string]any{"drive_id": "drive_ro", "access": "read_only"},
	}
	var issued struct {
		MessageID string `json:"message_id"`
	}
	ts.call("prepare", map[string]any{"instance_id": "inst_a", "payload": prepare}, &issued)
	stream.expect("session.prepare")
	report := preparedReport(connectionID, "report_prepared", issued.MessageID, "sess_1", "bind_1")
	if answer := ts.post("session-status", "tetral_ck_a", report); answer.status != http.StatusOK {
		ts.t.Fatalf("prepared report: %d %s", answer.status, answer.body)
	}
	return stream, &driveClient{ts: ts, connectionID: connectionID}
}

func (client *driveClient) query() url.Values {
	return url.Values{"protocol_version": {"1"}, "connection_id": {client.connectionID}, "binding_id": {"bind_1"}}
}

// request sends one drive transfer request for drive_1 and reads its whole
// answer.
func (client *driveClient) request(method, path string, query url.Values, header map[string]string, body []byte) reply {
	target := client.ts.api.URL + "/v1/client/drives/drive_1/" + path
	if query != nil {
		target += "?" + query.Encode()
	}
	request, _ := http.NewRequest(method, target, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer tetral_ck_a")
	for name, value := range header {
		request.Header.Set(name, value)
	}
	// A connection of its own: the client sends a request again by itself
	// only on a connection it has used before.
	single := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	response, err := single.Do(request)
	if err != nil {
		return reply{err: err}
	}
	defer func() { _ = response.Body.Close() }()
	answer, err := io.ReadAll(response.Body)
	return reply{status: response.StatusCode, header: response.Header, body: answer, err: err}
}

func hashOf(content string) string {
	digest := sha256.Sum256([]byte(content))
	return hex.EncodeToString(digest[:])
}

func (client *driveClient) upload(content string) reply {
	return client.request(http.MethodPut, "content/"+hashOf(content), client.query(),
		map[string]string{"Content-Type": "application/octet-stream"}, []byte(content))
}

// commit commits the writes of new files, each path with its content.
func (client *driveClient) commit(files map[string]string) reply {
	changes := []any{}
	for path, content := range files {
		changes = append(changes, map[string]any{
			"change": "write", "path": path, "base_sha256": nil, "content_sha256": hashOf(content), "executable": false,
		})
	}
	body, _ := json.Marshal(map[string]any{
		"protocol_version": 1, "connection_id": client.connectionID, "binding_id": "bind_1", "changes": changes,
	})
	return client.request(http.MethodPost, "commits", nil, map[string]string{"Content-Type": "application/json"}, body)
}

func (client *driveClient) download(hash, byteRange string) reply {
	header := map[string]string{}
	if byteRange != "" {
		header["Range"] = byteRange
	}
	return client.request(http.MethodGet, "content/"+hash, client.query(), header, nil)
}

// otherWriter commits changes to drive_1 as another writer and returns the
// control's result.
func (ts *testServer) otherWriter(changes ...map[string]any) (result struct {
	Seq     *int64 `json:"seq"`
	Changes []struct {
		Path          string `json:"path"`
		VersionID     string `json:"version_id"`
		ContentSha256 string `json:"content_sha256"`
		SizeBytes     int64  `json:"size_bytes"`
	} `json:"changes"`
}) {
	ts.t.Helper()
	ts.call("drive_commit", map[string]any{"drive_id": "drive_1", "changes": changes}, &result)
	return result
}

func TestEveryPayloadTakingControlRefusesAnInvalidUnmarkedPayload(t *testing.T) {
	instruction := func(payload map[string]any) map[string]any {
		return map[string]any{"instance_id": "inst_a", "payload": payload}
	}
	event := map[string]any{
		"protocol_version": 1, "connection_id": "conn_1", "message_id": "msg_sent", "type": "heartbeat",
		"payload": map[string]any{},
	}
	rows := []struct {
		control string
		input   map[string]any
		// refusal is part of the decoder's error for the payload.
		refusal string
		// marked is set where outside_contract lets the same payload through.
		marked bool
	}{
		{"prepare", instruction(map[string]any{"session_id": "sess_1", "binding_id": "bind_1"}), `missing member "cwd"`, true},
		{"close", instruction(map[string]any{"session_id": "sess_1"}), `missing member "binding_id"`, true},
		{"drives", instruction(map[string]any{"session_id": "sess_1", "binding_id": "bind_1", "drives": []any{
			map[string]any{"drive_id": "drive_1", "access": "read_only"}, map[string]any{"drive_id": "drive_1", "access": "read_write"},
		}}), "drive_id twice in drives", true},
		{"execute", instruction(map[string]any{}), "missing member", true},
		{"stdin", instruction(map[string]any{}), "missing member", true},
		{"cancel", instruction(map[string]any{}), "missing member", true},
		{"mcp_call", instruction(map[string]any{}), "missing member", true},
		{"send", map[string]any{"instance_id": "inst_a", "event": event}, `missing member "heartbeat_seq"`, true},
		{"send", map[string]any{"instance_id": "inst_a", "data": `{"protocol_version":1`}, "invalid JSON", true},
		{"dispatch_before_ready_ack", map[string]any{"instance_id": "inst_b", "instructions": []any{
			map[string]any{"type": "session.close", "payload": map[string]any{"session_id": "sess_1"}},
		}}, `missing member "binding_id"`, false},
		{"drive_commit", map[string]any{"drive_id": "drive_1", "changes": []any{
			map[string]any{"path": "a//b", "text": "x"},
		}}, "drive path segments", false},
		{"drive_commit", map[string]any{"drive_id": "", "changes": []any{}}, "/drive_id", false},
		{"drive_limits", map[string]any{"drive_id": ""}, "/drive_id", false},
	}
	for _, row := range rows {
		t.Run(row.control, func(t *testing.T) {
			ts := startServer(t)
			ts.online()
			status, answer := ts.callStatus(row.control, row.input)
			if status != http.StatusBadRequest || !strings.Contains(string(answer), row.refusal) {
				t.Errorf("unmarked: %d %s; want 400 with %q", status, answer, row.refusal)
			}
			if !row.marked {
				return
			}
			row.input["outside_contract"] = true
			if status, answer := ts.callStatus(row.control, row.input); status != http.StatusOK {
				t.Errorf("marked outside_contract: %d %s; want it sent", status, answer)
			}
		})
	}
}

func TestAnswerFaultsReachTheDriveTransferEndpoints(t *testing.T) {
	const held = "held content"
	endpoints := []struct {
		name string
		// send sends one request of the endpoint that the store answers 200,
		// or 206 for the download.
		send func(client *driveClient) reply
		json bool
	}{
		{"list_files", func(client *driveClient) reply {
			return client.request(http.MethodGet, "files", client.query(), nil, nil)
		}, true},
		{"list_changes", func(client *driveClient) reply {
			query := client.query()
			query.Set("after_seq", "0")
			return client.request(http.MethodGet, "changes", query, nil, nil)
		}, true},
		{"upload_content", func(client *driveClient) reply { return client.upload("uploaded content") }, true},
		{"commit", func(client *driveClient) reply { return client.commit(map[string]string{"new.txt": held}) }, true},
		{"download_content", func(client *driveClient) reply { return client.download(hashOf(held), "bytes=0-3") }, false},
	}
	// served counts the endpoint's requests that the store answered itself.
	served := func(ts *testServer, endpoint string) (count int) {
		for _, entry := range ts.state().Requests {
			if entry.Endpoint == endpoint && entry.Status/100 == 2 {
				count++
			}
		}
		return count
	}
	for _, endpoint := range endpoints {
		start := func(t *testing.T, fault map[string]any) (*testServer, <-chan reply) {
			ts := startServer(t)
			_, client := ts.mounted()
			ts.otherWriter(map[string]any{"path": "held.txt", "text": held})
			if answer := client.upload(held); answer.status != http.StatusOK {
				t.Fatalf("upload: %d %s", answer.status, answer.body)
			}
			fault["endpoint"] = endpoint.name
			ts.call("answer", fault, nil)
			before := served(ts, endpoint.name)
			done := make(chan reply, 1)
			go func() { done <- endpoint.send(client) }()
			t.Cleanup(func() {
				if count := served(ts, endpoint.name) - before; count > 1 {
					t.Errorf("the store answered %d requests; want at most the one sent", count)
				}
			})
			return ts, done
		}
		t.Run(endpoint.name+"/error", func(t *testing.T) {
			ts, done := start(t, map[string]any{"action": "error", "code": "unavailable", "status": 503})
			if answer := receive(t, done); answer.status != http.StatusServiceUnavailable || answer.code() != "unavailable" {
				t.Errorf("answer %d %s; want the injected 503", answer.status, answer.body)
			}
			if count := served(ts, endpoint.name); count != boolCount(endpoint.name == "upload_content") {
				t.Errorf("the store answered %d requests of its own after the refused one", count)
			}
		})
		t.Run(endpoint.name+"/delay", func(t *testing.T) {
			ts, done := start(t, map[string]any{"action": "delay", "delay_ms": 5000})
			waitFor(t, func() bool { return len(ts.state().Held) == 1 })
			ts.advance(4999 * time.Millisecond)
			select {
			case answer := <-done:
				t.Fatalf("answered before its delay: %+v", answer)
			default:
			}
			ts.advance(time.Millisecond)
			if answer := receive(t, done); answer.status/100 != 2 {
				t.Errorf("delayed answer %d %s", answer.status, answer.body)
			}
		})
		t.Run(endpoint.name+"/withhold", func(t *testing.T) {
			ts, done := start(t, map[string]any{"action": "withhold"})
			waitFor(t, func() bool { return len(ts.state().Held) == 1 })
			ts.advance(time.Minute)
			select {
			case answer := <-done:
				t.Fatalf("a withheld answer arrived: %+v", answer)
			default:
			}
			ts.call("answer", map[string]any{"action": "release", "endpoint": endpoint.name}, nil)
			if answer := receive(t, done); answer.status/100 != 2 {
				t.Errorf("released answer %d %s", answer.status, answer.body)
			}
		})
		t.Run(endpoint.name+"/drop", func(t *testing.T) {
			ts, done := start(t, map[string]any{"action": "drop"})
			if answer := receive(t, done); answer.err == nil {
				t.Errorf("answer %d %s; want the connection cut", answer.status, answer.body)
			}
			if endpoint.name == "commit" && ts.driveSeq("drive_1") != 2 {
				t.Error("the dropped commit was not applied")
			}
		})
		t.Run(endpoint.name+"/hold_request", func(t *testing.T) {
			ts, done := start(t, map[string]any{"action": "hold_request"})
			waitFor(t, func() bool { return len(ts.state().Held) == 1 })
			ts.call("credential", map[string]any{"action": "revoke", "credential_id": "cred_a"}, nil)
			ts.call("answer", map[string]any{"action": "release"}, nil)
			if answer := receive(t, done); answer.status != http.StatusUnauthorized || answer.code() != "credential_revoked" {
				t.Errorf("released answer %d %s; want it authenticated at its release", answer.status, answer.body)
			}
		})
		if endpoint.json {
			t.Run(endpoint.name+"/schema_fail", func(t *testing.T) {
				_, done := start(t, map[string]any{"action": "schema_fail"})
				if answer := receive(t, done); answer.status != http.StatusOK || string(answer.body) != "{}" {
					t.Errorf("answer %d %s; want 200 {}", answer.status, answer.body)
				}
			})
		}
	}
	t.Run("a fault that fits no drive transfer request is refused", func(t *testing.T) {
		ts := startServer(t)
		for _, fault := range []map[string]any{
			{"endpoint": "commit", "action": "drop", "message_id": "msg_1"},
			{"endpoint": "commit", "action": "wrong_identity"},
			{"endpoint": "upload_content", "action": "without_accepted"},
			{"endpoint": "download_content", "action": "schema_fail"},
			{"endpoint": "list_files", "action": "throttle", "bytes_per_second": 1},
		} {
			if status, answer := ts.callStatus("answer", fault); status != http.StatusBadRequest {
				t.Errorf("%v: %d %s; want it refused", fault, status, answer)
			}
		}
	})
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

// driveSeq is the seq of a drive's latest commit, as drive_state reports it.
func (ts *testServer) driveSeq(driveID string) int64 {
	ts.t.Helper()
	var state struct {
		Seq int64 `json:"seq"`
	}
	ts.call("drive_state", map[string]any{"drive_id": driveID}, &state)
	return state.Seq
}

func TestThrottleMovesADriveBodyAtTheClocksPace(t *testing.T) {
	content := strings.Repeat("0123456789abcdef", 16)
	rows := []struct {
		endpoint string
		send     func(client *driveClient) reply
	}{
		{"upload_content", func(client *driveClient) reply { return client.upload(content) }},
		{"download_content", func(client *driveClient) reply { return client.download(hashOf(content), "") }},
	}
	for _, row := range rows {
		t.Run(row.endpoint, func(t *testing.T) {
			ts := startServer(t)
			_, client := ts.mounted()
			ts.otherWriter(map[string]any{"path": "held.txt", "text": content})
			ts.call("answer", map[string]any{"endpoint": row.endpoint, "action": "throttle", "bytes_per_second": 64}, nil)
			done := make(chan reply, 1)
			go func() { done <- row.send(client) }()
			waitFor(t, func() bool { return len(ts.state().Throttled) == 1 })
			size := int64(len(content))
			for moved := int64(64); moved < size; moved += 64 {
				ts.advance(time.Second)
				waitFor(t, func() bool { return ts.state().Throttled[0].Read == moved })
				select {
				case answer := <-done:
					t.Fatalf("answered with %d of %d bytes moved: %+v", moved, size, answer)
				default:
				}
			}
			ts.advance(time.Second)
			answer := receive(t, done)
			if answer.status != http.StatusOK || answer.err != nil {
				t.Fatalf("throttled answer %d %s %v", answer.status, answer.body, answer.err)
			}
			if row.endpoint == "download_content" && string(answer.body) != content {
				t.Errorf("downloaded %q", answer.body)
			}
		})
	}
}

func TestGeneratedContentGivesTheSameBytesToEveryRange(t *testing.T) {
	ts := startServer(t)
	_, client := ts.mounted()
	const size = 200_000
	written := ts.otherWriter(map[string]any{"path": "big.bin", "generated": map[string]any{"size_bytes": size, "seed": "seed-a"}})
	hash := written.Changes[0].ContentSha256
	whole := client.download(hash, "")
	if whole.status != http.StatusOK || len(whole.body) != size || hashOf(string(whole.body)) != hash {
		t.Fatalf("the whole content: %d, %d bytes with hash %s; want %d bytes with the hash %s that the control gave",
			whole.status, len(whole.body), hashOf(string(whole.body)), size, hash)
	}
	// Starts on and off the cipher's 16-byte blocks and the copy buffer's edge.
	for _, bounds := range [][2]int{{0, 0}, {1, 16}, {15, 17}, {16, 31}, {65_535, 65_537}, {199_999, 199_999}} {
		part := client.download(hash, fmt.Sprintf("bytes=%d-%d", bounds[0], bounds[1]))
		if part.status != http.StatusPartialContent || !bytes.Equal(part.body, whole.body[bounds[0]:bounds[1]+1]) {
			t.Errorf("range %v: %d with %d bytes; want the same bytes of the whole content", bounds, part.status, len(part.body))
		}
	}
	other := ts.otherWriter(map[string]any{"path": "other.bin", "generated": map[string]any{"size_bytes": size, "seed": "seed-b"}})
	if other.Changes[0].ContentSha256 == hash {
		t.Error("another seed gave the same content")
	}
}
