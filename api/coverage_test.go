// Layer 1: the coverage record's arithmetic over the OpenAPI file.
package api

import (
	"slices"
	"strings"
	"testing"
)

const twoOperations = `{"paths": {
	"/v1/drives": {"get": {"operationId": "drives.list", "responses": {
		"200": {},
		"400": {"x-error-types": ["invalid_request_error"]}}}},
	"/v1/drives/{drive_id}/changes/stream": {"get": {"operationId": "drive_changes.stream", "responses": {
		"200": {"content": {"text/event-stream": {"x-events": {"drive.changed": {}}}}},
		"404": {"x-error-types": ["not_found_error"]}}}}}}`

// Unreached lists what the file declares and no test checked, less the
// list's entries, and each entry of the list that is not true.
func TestUnreachedListsWhatNoTestChecked(t *testing.T) {
	all := []Checked{
		{Operation: "drives.list", Status: "200"},
		{Operation: "drives.list", Status: "400", Error: "invalid_request_error"},
		{Operation: "drive_changes.stream", Status: "200"},
		{Operation: "drive_changes.stream", Status: "404", Error: "not_found_error"},
		{Operation: "drive_changes.stream", Event: "drive.changed"},
	}
	without := func(index int) []Checked { return slices.Delete(slices.Clone(all), index, index+1) }
	rows := []struct {
		name    string
		checked []Checked
		list    string
		want    []string
	}{
		{name: "everything checked", checked: all, list: `{}`},
		{name: "an operation with only an error answer", checked: without(0), list: `{}`,
			want: []string{"operation drives.list: no test checked a success answer of it"}},
		{name: "an error type on no operation", checked: without(3), list: `{}`,
			want: []string{"error type not_found_error: no test checked an answer with it"}},
		{name: "an event on no stream", checked: without(4), list: `{}`,
			want: []string{"stream event drive.changed: no test checked it on a stream"}},
		{name: "the same three, each with its reason", checked: []Checked{all[1], all[2]},
			list: `{"operations": {"drives.list": "r"}, "errors": {"not_found_error": "r"}, "events": {"drive.changed": "r"}}`},
		{name: "an entry a test checked", checked: all, list: `{"errors": {"not_found_error": "r"}}`,
			want: []string{"error type not_found_error: unreachable.json lists it and a test checked it"}},
		{name: "an entry the file does not declare", checked: all, list: `{"events": {"drive.removed": "r"}}`,
			want: []string{"stream event drive.removed: unreachable.json lists it and the file does not declare it"}},
		{name: "an entry without a reason", checked: without(4), list: `{"events": {"drive.changed": " "}}`,
			want: []string{"stream event drive.changed: unreachable.json gives no reason"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			declared, unreachable, err := parse([]byte(twoOperations), []byte(row.list))
			if err != nil {
				t.Fatal(err)
			}
			if got := unreached(declared, unreachable, row.checked); !slices.Equal(got, row.want) {
				t.Errorf("unreached = %q; want %q", got, row.want)
			}
		})
	}
}

// What Required lists, checked, leaves nothing unreached, and less any one
// line leaves that line's name.
func TestRequiredIsWhatLeavesNothingUnreached(t *testing.T) {
	required, err := Required()
	if err != nil {
		t.Fatal(err)
	}
	if lines, err := Unreached(required); err != nil || len(lines) != 0 {
		t.Fatalf("with every required line checked, unreached = %q, %v", lines, err)
	}
	for index, line := range required {
		_, name := line.kind()
		lines, err := Unreached(slices.Delete(slices.Clone(required), index, index+1))
		if err != nil || len(lines) != 1 || !strings.Contains(lines[0], " "+name+": no test checked") {
			t.Errorf("without %+v, unreached = %q, %v", line, lines, err)
		}
	}
}
