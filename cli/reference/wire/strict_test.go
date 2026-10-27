package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// Input that two JSON readers could read two ways is refused before any type
// sees it.
func TestTheStrictReaderRefusesAmbiguousJSON(t *testing.T) {
	rows := []struct {
		name    string
		input   string
		refused StrictErrorKind
	}{
		{"repeated key at depth 1", `{"a":1,"a":1}`, DuplicateMember},
		{"repeated key at depth 3", `{"a":{"b":{"c":1,"c":2}}}`, DuplicateMember},
		{"repeated key spelled with an escape", `{"a":1,"\u0061":2}`, DuplicateMember},
		{"lone high surrogate", `"\ud800"`, LoneSurrogate},
		{"lone low surrogate", `"\udc00"`, LoneSurrogate},
		{"escaped surrogate pair", `"\ud83d\ude00"`, 0},
		{"number beyond a 64-bit float", `1e400`, NonFiniteNumber},
		{"integer above the signed 64-bit range", `9223372036854775808`, IntegerOutOfRange},
		{"integer below the signed 64-bit range", `-9223372036854775809`, IntegerOutOfRange},
		{"largest signed 64-bit integer", `9223372036854775807`, 0},
		{"smallest signed 64-bit integer", `-9223372036854775808`, 0},
		{"127 levels of nesting", strings.Repeat("[", 127) + strings.Repeat("]", 127), 0},
		{"128 levels of nesting", strings.Repeat("[", 128) + strings.Repeat("]", 128), TooDeep},
	}
	for _, row := range rows {
		_, err := readStrict([]byte(row.input))
		var strict *StrictError
		switch {
		case row.refused == 0 && err != nil:
			t.Errorf("%s: refused: %v", row.name, err)
		case row.refused != 0 && (!errors.As(err, &strict) || strict.Kind != row.refused):
			t.Errorf("%s: got %v, want %s", row.name, err, row.refused)
		}
	}
}

func TestUnknownFieldsShareTheEnvelopesNestingBudget(t *testing.T) {
	for _, arrays := range []int{126, 127} {
		nested := strings.Repeat("[", arrays) + "0" + strings.Repeat("]", arrays)
		message := []byte(`{"protocol_version":1,"connection_id":"c","message_id":"m","request_message_id":"r","heartbeat_seq":1,"extension":` + nested + `}`)
		decoded, err := Decode[Heartbeat](message)
		if arrays == 126 {
			if err != nil || decoded.HeartbeatSeq != 1 {
				t.Fatalf("127 total levels: %+v, %v", decoded, err)
			}
		} else {
			var refused *DecodeError
			if !errors.As(err, &refused) || refused.Kind != Strict || refused.Strict.Kind != TooDeep {
				t.Fatalf("128 total levels: %v", err)
			}
		}
	}
}

func TestTheDecoderReadsThroughTheStrictPassAndKeepsIntegersExact(t *testing.T) {
	heartbeat := func(seq string) []byte {
		return []byte(`{"protocol_version":1,"connection_id":"c","message_id":"m","request_message_id":"r","heartbeat_seq":` + seq + `}`)
	}
	// 2^53 + 1 is the first integer that a 64-bit float cannot hold.
	for _, row := range []struct {
		input string
		seq   int64
	}{
		{"9007199254740993", 1<<53 + 1},
		{"9223372036854775807", 1<<63 - 1},
	} {
		decoded, err := Decode[Heartbeat](heartbeat(row.input))
		if err != nil || decoded.HeartbeatSeq != row.seq {
			t.Errorf("%s: got %d, %v", row.input, decoded.HeartbeatSeq, err)
		}
	}
	listing, err := Decode[ListFilesResponse]([]byte(`{"files":[],"has_more":false,"start_seq":-0}`))
	if err != nil || !listing.StartSeq.Set || listing.StartSeq.Value != 0 {
		t.Errorf("the integer -0: got %+v, %v", listing.StartSeq, err)
	}
	var refused *DecodeError
	_, err = Decode[Heartbeat](heartbeat(`1,"heartbeat_seq":2`))
	if !errors.As(err, &refused) || refused.Kind != Strict || refused.Strict.Kind != DuplicateMember {
		t.Errorf("a repeated member: %v", err)
	}
}

// fixturesByName reads the fixture files, by file and fixture name.
func fixturesByName(t *testing.T) map[string]map[string]fixture {
	t.Helper()
	files := map[string]map[string]fixture{}
	for _, file := range fixtureFiles {
		var fixtures []fixture
		if err := json.Unmarshal(readProtocolFile(t, file.name+".fixtures.json"), &fixtures); err != nil {
			t.Fatal(err)
		}
		files[file.name] = map[string]fixture{}
		for _, item := range fixtures {
			files[file.name][item.Name] = item
		}
	}
	return files
}

// decodeEdited decodes tree in place of fixture name's value, with the
// status, endpoint and paired fixtures that name gives.
func decodeEdited(t *testing.T, files map[string]map[string]fixture, file, name string, tree any) error {
	t.Helper()
	_, err := reencodeEdited(t, files, file, name, tree)
	return err
}

// reencodeEdited decodes tree as decodeEdited does and gives its encoding.
func reencodeEdited(t *testing.T, files map[string]map[string]fixture, file, name string, tree any) ([]byte, error) {
	t.Helper()
	item := files[file][name]
	encoded, err := json.Marshal(tree)
	if err != nil {
		t.Fatal(err)
	}
	item.Value = encoded
	return decodeFixture(t, file, item, files[file])
}

// put sets the member or item at pointer, adding a member that is not there.
func put(t *testing.T, tree any, pointer string, value any) {
	t.Helper()
	parent, last := splitPointer(t, tree, pointer)
	switch node := parent.(type) {
	case map[string]any:
		node[last] = value
	case []any:
		index, err := strconv.Atoi(last)
		if err != nil {
			t.Fatal(err)
		}
		node[index] = value
	default:
		t.Fatalf("no object or array above %s", pointer)
	}
}

func splitPointer(t *testing.T, tree any, pointer string) (any, string) {
	t.Helper()
	parts := strings.Split(pointer, "/")[1:]
	node := tree
	for _, part := range parts[:len(parts)-1] {
		switch current := node.(type) {
		case map[string]any:
			node = current[part]
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index >= len(current) {
				t.Fatalf("no item %s in %s", part, pointer)
			}
			node = current[index]
		default:
			t.Fatalf("nothing at %s", pointer)
		}
	}
	return node, parts[len(parts)-1]
}

// refusedNaming reports whether typed decoding refused the message for the
// reason that word names, rather than for another one.
func refusedNaming(err error, word string) bool {
	var refused *DecodeError
	return errors.As(err, &refused) && refused.Kind == Shape && strings.Contains(refused.Detail, word)
}

// The Rust test of the same name checks each row against the schema: the
// schema refuses the null, and lets the member be absent exactly in the
// optional rows.
func TestAMemberSentAsNullWhereItsSchemaRefusesNullIsRefused(t *testing.T) {
	rows := []struct{ presence, file, name, member string }{
		{"required", "tool-execution-v1", "ack", "/message_id"},
		{"required", "tool-execution-v1", "ack", "/accepted"},
		{"required", "tool-execution-v1", "protocol_error_without_request_id", "/error/code"},
		{"required", "tool-execution-v1", "execute_edit", "/payload/tool_use_event_id"},
		{"required", "tool-execution-v1", "execute_edit", "/payload/input"},
		{"required", "tool-execution-v1", "execute_edit", "/payload/sandbox_policy/filesystem/rules/0/path"},
		{"required", "tool-execution-v1", "drive_sync_report", "/drive_sync/committed"},
		{"required", "connection-v1", "heartbeat_report", "/heartbeat_seq"},
		{"required", "connection-v1", "connected_resumed", "/type"},
		{"required", "connection-v1", "connected_resumed", "/payload"},
		{"required", "connection-v1", "drive_changed", "/payload/drive_id"},
		{"required", "connection-v1", "events_query_new", "/protocol_version"},
		{"required", "connection-v1", "error_rate_limited", "/code"},
		{"required", "connection-v1", "ready_no_tools", "/capabilities/session_preparation"},
		{"required", "connection-v1", "mcp_tools_empty_listing", "/server_info/name"},
		{"required", "connection-v1", "closed_report", "/status"},
		{"required", "drive-transfer-v1", "rate_limited_headers", "/Retry-After"},
		{"required", "drive-transfer-v1", "commit_request_delete_of_absent_file", "/changes/0/path"},
		{"required", "drive-transfer-v1", "list_changes_page_from_beginning", "/records/0/version_id"},
		{"required", "drive-transfer-v1", "error_drive_cursor_expired", "/message"},
		{"required", "drive-transfer-v1", "list_files_empty_drive", "/has_more"},
		{"optional", "tool-execution-v1", "execute_with_traceparent", "/traceparent"},
		{"optional", "tool-execution-v1", "drive_sync_report_with_task", "/task_id"},
		{"optional", "tool-execution-v1", "ready_with_local_mcp", "/local_mcp"},
		{"optional", "tool-execution-v1", "partial_stdin_error_preserves_result", "/error"},
		{"optional", "tool-execution-v1", "partial_stdin_error_preserves_result", "/result"},
		{"optional", "tool-execution-v1", "not_synced_uploading", "/result/drive_sync"},
		{"optional", "tool-execution-v1", "mcp_cancel_reply", "/result/result/cancelled"},
		{"optional", "tool-execution-v1", "mcp_cancel_reply", "/result/result/was_running"},
		{"optional", "tool-execution-v1", "mcp_terminal", "/result/result/isError"},
		{"optional", "tool-execution-v1", "mcp_terminal", "/result/result/structuredContent"},
		{"optional", "connection-v1", "events_query_resume", "/previous_connection_id"},
		{"optional", "connection-v1", "events_query_resume", "/resume"},
		{"optional", "connection-v1", "session_close_with_traceparent", "/traceparent"},
		{"optional", "connection-v1", "protocol_error_report", "/request_message_id"},
		{"optional", "connection-v1", "ready_with_startup", "/startup"},
		{"optional", "connection-v1", "ready_local_mcp_without_tools", "/capabilities/local_mcp"},
		{"optional", "connection-v1", "prepared_report", "/repository_failures"},
		{"optional", "connection-v1", "applied_report", "/drive_mount_failures"},
		{"optional", "connection-v1", "applied_report", "/lost_changes"},
		{"optional", "connection-v1", "mcp_tools_listing", "/tools/0/description"},
		{"optional", "connection-v1", "mcp_tools_listing", "/tools/0/annotations"},
		{"optional", "connection-v1", "mcp_tools_listing", "/tools/1/title"},
		{"optional", "connection-v1", "mcp_tools_listing", "/tools/1/outputSchema"},
		{"optional", "drive-transfer-v1", "list_files_next_page_query", "/after_path"},
		{"optional", "drive-transfer-v1", "list_changes_query_inside_commit", "/after_index"},
		{"optional", "drive-transfer-v1", "list_files_empty_drive", "/start_seq"},
		{"optional", "drive-transfer-v1", "upload_headers_empty_file", "/Content-Length"},
		{"optional", "drive-transfer-v1", "download_headers_open_range", "/Range"},
		{"optional", "drive-transfer-v1", "commit_response", "/results/2/current"},
	}
	files := fixturesByName(t)
	for _, row := range rows {
		item, ok := files[row.file][row.name]
		if !ok || !item.Valid {
			t.Fatalf("no valid fixture %s %s", row.file, row.name)
		}
		tree := readTree(t, compact(t, item.Value))
		put(t, tree, row.member, nil)
		if err := decodeEdited(t, files, row.file, row.name, tree); !refusedNaming(err, "null") {
			t.Errorf("%s %s, %s: null decoded with %v", row.name, row.member, row.presence, err)
		}
	}
}

// The optional members whose schema allows null and that no valid fixture
// sends as null. The Rust test of the same name checks each row against the
// schema.
func TestAnOptionalMemberWhoseSchemaAllowsNullKeepsItThroughARoundTrip(t *testing.T) {
	limits := map[string]string{"/payload/limits": "{}"}
	rows := []struct {
		name   string
		edits  map[string]string
		member string
	}{
		{"execute_exec_command", nil, "/payload/input/cwd"},
		{"execute_exec_command", nil, "/payload/input/env"},
		{"execute_exec_command", nil, "/payload/input/wait_ms"},
		{"execute_exec_command", nil, "/payload/input/on_wait_expiry"},
		{"execute_exec_command", nil, "/payload/input/task_lifetime_ms"},
		{"execute_exec_command", nil, "/payload/input/task_id"},
		{"execute_edit", nil, "/payload/input/new_string"},
		{"execute_edit", nil, "/payload/input/replace_all"},
		{"control_stdin_read", nil, "/payload/wait_ms"},
		{"control_cancel", nil, "/payload/reason"},
		{"control_stdin_read", limits, "/payload/limits/visible_bytes"},
		{"control_stdin_read", limits, "/payload/limits/visible_lines"},
	}
	files := fixturesByName(t)
	for _, row := range rows {
		item, ok := files["tool-execution-v1"][row.name]
		if !ok || !item.Valid {
			t.Fatalf("no valid fixture %s", row.name)
		}
		tree := readTree(t, compact(t, item.Value))
		for pointer, edit := range row.edits {
			put(t, tree, pointer, readTree(t, []byte(edit)))
		}
		put(t, tree, row.member, nil)
		got, err := reencodeEdited(t, files, "tool-execution-v1", row.name, tree)
		if err != nil {
			t.Errorf("%s %s: %v", row.name, row.member, err)
		} else if !reflect.DeepEqual(readTree(t, got), tree) {
			t.Errorf("%s %s: re-encoded as %s", row.name, row.member, got)
		}
	}
}

// The Rust test of the same name checks that the schema refuses each edit.
func TestAnIdentityThatIsEmptyOrHoldsNULIsRefused(t *testing.T) {
	rows := []struct{ file, name, member string }{
		{"connection-v1", "connected_resumed", "/message_id"},
		{"connection-v1", "events_query_new", "/instance_id"},
		{"tool-execution-v1", "ack", "/message_id"},
		{"tool-execution-v1", "execute_edit", "/payload/tool_use_event_id"},
		{"drive-transfer-v1", "list_files_next_page_query", "/binding_id"},
		{"drive-transfer-v1", "list_changes_page_from_beginning", "/records/0/version_id"},
	}
	files := fixturesByName(t)
	for _, row := range rows {
		item, ok := files[row.file][row.name]
		if !ok || !item.Valid {
			t.Fatalf("no valid fixture %s %s", row.file, row.name)
		}
		for _, identity := range []string{"", "id\x00tail"} {
			tree := readTree(t, compact(t, item.Value))
			put(t, tree, row.member, identity)
			if err := decodeEdited(t, files, row.file, row.name, tree); !refusedNaming(err, "a nonempty identity without U+0000") {
				t.Errorf("%s %s %q: decoded with %v", row.name, row.member, identity, err)
			}
		}
	}
}

// closedTables are closed definitions that no message holds, so no message
// can carry an unknown member in them: the drive transfer limits and
// endpoint tables.
var closedTables = []string{
	"drive-transfer-v1#/$defs/DriveTransferLimits",
	"drive-transfer-v1#/$defs/DriveTransferEndpoints",
}

// walkSchemas returns the schema locations with `additionalProperties:
// false`, and the targets of every `$ref`, as file#pointer.
func walkSchemas(t *testing.T) (closed, targets map[string]bool) {
	t.Helper()
	closed, targets = map[string]bool{}, map[string]bool{}
	var walk func(file string, node any, pointer string)
	walk = func(file string, node any, pointer string) {
		switch current := node.(type) {
		case map[string]any:
			if current["additionalProperties"] == false {
				closed[file+"#"+pointer] = true
			}
			if target, ok := current["$ref"].(string); ok {
				targetFile, targetPointer, _ := strings.Cut(target, "#")
				targetFile, ok = strings.CutSuffix(targetFile, ".schema.json")
				if !ok {
					targetFile = file
				}
				targets[targetFile+"#"+targetPointer] = true
			}
			for name, member := range current {
				walk(file, member, pointer+"/"+name)
			}
		case []any:
			for index, item := range current {
				walk(file, item, fmt.Sprintf("%s/%d", pointer, index))
			}
		}
	}
	for _, file := range fixtureFiles {
		walk(file.name, readTree(t, readProtocolFile(t, file.name+".schema.json")), "")
	}
	return closed, targets
}

// Each row names a closed schema location, the valid fixture that holds an
// object of it, edits that put one there where no fixture does, and the
// object's pointer. The Rust test of the same name checks that each object
// is one of its row's location. The network modes `disabled` and `direct`
// are two types, so `NetworkPolicy/oneOf/0` has a row for each.
func TestAnUnknownMemberInEachClosedObjectIsRefused(t *testing.T) {
	type edit struct{ pointer, value string }
	rows := []struct {
		location, name string
		edits          []edit
		object         string
	}{
		{"tool-execution-v1#/$defs/SandboxPolicy", "execute_edit", nil, "/payload/sandbox_policy"},
		{"tool-execution-v1#/$defs/FilesystemPolicy", "execute_edit", nil, "/payload/sandbox_policy/filesystem"},
		{"tool-execution-v1#/$defs/FilesystemRule", "execute_edit", nil, "/payload/sandbox_policy/filesystem/rules/0"},
		{"tool-execution-v1#/$defs/NetworkPolicy/oneOf/0", "execute_edit", nil, "/payload/sandbox_policy/network"},
		{"tool-execution-v1#/$defs/NetworkPolicy/oneOf/0", "execute_edit", []edit{
			{"/payload/sandbox_policy/network", `{"mode":"direct"}`},
		}, "/payload/sandbox_policy/network"},
		{"tool-execution-v1#/$defs/NetworkPolicy/oneOf/1", "managed_explicit_rules", nil, "/network"},
		{"tool-execution-v1#/$defs/ManagedNetworkRules", "managed_explicit_rules", nil, "/network/rules"},
		{"tool-execution-v1#/$defs/NetworkRule", "managed_explicit_rules", nil, "/network/rules/allow/0"},
		{"tool-execution-v1#/$defs/NetworkDestination/oneOf/0", "managed_explicit_rules", nil, "/network/rules/allow/0/destination"},
		{"tool-execution-v1#/$defs/NetworkDestination/oneOf/1", "managed_explicit_rules", []edit{
			{"/network/rules/allow/0/destination", `{"kind":"cidr","value":"10.0.0.0/8"}`},
		}, "/network/rules/allow/0/destination"},
		{"tool-execution-v1#/$defs/PortRange", "managed_explicit_rules", nil, "/network/rules/allow/0/ports/0"},
		{"tool-execution-v1#/$defs/NetworkListen", "managed_explicit_rules", []edit{
			{"/network/rules/listen", `[{"address":"127.0.0.1","transport":"tcp","ports":[{"from":8080,"to":8080}]}]`},
		}, "/network/rules/listen/0"},
		{"tool-execution-v1#/$defs/TransportLimits", "ready_direct_and_disabled", nil, "/tool_execution/limits"},
		{"tool-execution-v1#/$defs/ManagedNetworkCapability", "ready_direct_and_disabled", []edit{
			{"/tool_execution/network_modes", `["disabled","direct","managed"]`},
			{"/tool_execution/managed_network", `{"transports":["tcp"],"listen":true}`},
		}, "/tool_execution/managed_network"},
		{"tool-execution-v1#/$defs/DriveSyncOutcome", "drive_sync_report", nil, "/drive_sync"},
		{"tool-execution-v1#/$defs/DriveRejection", "drive_sync_report", nil, "/drive_sync/rejected/0"},
		{"tool-execution-v1#/$defs/DriveNotSynced", "not_synced_uploading", nil, "/result/drive_sync/not_synced/0"},
		{"tool-execution-v1#/$defs/DriveDownload", "drive_sync_report", []edit{
			{"/drive_sync/downloading", `[{"path":"/tetral/sessions/id_session_id/drives/id_drive_id/data/input.csv","downloaded_bytes":10,"size_bytes":20}]`},
		}, "/drive_sync/downloading/0"},
		{"tool-execution-v1#/$defs/DriveMountFailure", "applied_report", nil, "/drive_mount_failures/0"},
		{"tool-execution-v1#/$defs/DriveWriter/oneOf/0", "commit_response_rejections", nil, "/results/0/current/writer"},
		{"tool-execution-v1#/$defs/DriveWriter/oneOf/1", "drive_sync_report", nil, "/drive_sync/rejected/1/current_writer"},
		{"tool-execution-v1#/$defs/DriveWriter/oneOf/2", "drive_sync_report", []edit{
			{"/drive_sync/rejected/0/current_writer", `{"type":"service_account_actor","service_account_id":"id_service_account_id"}`},
		}, "/drive_sync/rejected/0/current_writer"},
		{"tool-execution-v1#/$defs/DriveWriter/oneOf/3", "commit_response", nil, "/results/2/current/writer"},
		{"connection-v1#/$defs/McpTool", "mcp_tools_2048_tools", nil, "/tools/0"},
		{"drive-transfer-v1#/$defs/DrivePathParams", "drive_path_params", nil, ""},
		{"drive-transfer-v1#/$defs/ContentPathParams", "content_path_params", nil, ""},
		{"drive-transfer-v1#/$defs/CommitRequest", "commit_request", nil, ""},
		{"drive-transfer-v1#/$defs/DeleteChange", "commit_request", nil, "/changes/0"},
		{"drive-transfer-v1#/$defs/WriteChange", "commit_request", nil, "/changes/1"},
	}
	files := fixturesByName(t)
	listed, seen := map[string]bool{}, map[string]bool{}
	for _, row := range rows {
		listed[row.location] = true
		if key := fmt.Sprint(row); seen[key] {
			t.Errorf("%s: the row repeats", row.location)
		} else {
			seen[key] = true
		}
		var homes []string
		for file, fixtures := range files {
			if item, ok := fixtures[row.name]; ok && item.Valid {
				homes = append(homes, file)
			}
		}
		if len(homes) != 1 {
			t.Fatalf("%s: valid fixture %s in %v", row.location, row.name, homes)
		}
		home := homes[0]
		tree := readTree(t, compact(t, files[home][row.name].Value))
		for _, change := range row.edits {
			put(t, tree, change.pointer, readTree(t, []byte(change.value)))
		}
		if err := decodeEdited(t, files, home, row.name, tree); err != nil {
			t.Errorf("%s: the edited fixture is refused: %v", row.location, err)
			continue
		}
		put(t, tree, row.object+"/unexpected_member", json.Number("1"))
		if err := decodeEdited(t, files, home, row.name, tree); !refusedNaming(err, "unexpected_member") {
			t.Errorf("%s: decoded with %v", row.location, err)
		}
	}
	closed, targets := walkSchemas(t)
	for _, table := range closedTables {
		for target := range targets {
			if strings.HasPrefix(target, table) {
				t.Errorf("%s is held by a message: %s", table, target)
			}
		}
		listed[table] = true
	}
	if got, want := slices.Sorted(maps.Keys(listed)), slices.Sorted(maps.Keys(closed)); !slices.Equal(got, want) {
		t.Errorf("rows %v, closed locations %v", got, want)
	}
}
