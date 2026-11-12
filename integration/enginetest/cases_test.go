// Layer 1: the case matcher in process.
package enginetest

import (
	"strings"
	"testing"
)

// The matcher decides whether a case holds, so each of its rules is held to
// one value that matches and one that does not.
func TestACaseMatchesOnlyWhatItStates(t *testing.T) {
	hello := "hello\n"
	for _, row := range []struct {
		name, want, got string
		// refusal is part of the mismatch's message, "" for a match.
		refusal string
	}{
		{"an object matches one with more members", `{"a":1}`, `{"a":1,"b":2}`, ""},
		{"a missing member", `{"a":1}`, `{"b":2}`, "/a: missing"},
		{"an unequal member", `{"a":{"b":"x"}}`, `{"a":{"b":"y"}}`, `/a/b: got "y"`},
		{"an array has the stated length", `[1]`, `[1,2]`, "want an array of 1"},
		{"an array matches element by element", `[{"a":1},{"a":2}]`, `[{"a":1},{"a":3}]`, "/1/a: got 3"},
		{"null matches only null", `{"a":null}`, `{"a":0}`, "/a: got 0"},
		{"an absent member", `{"a":{"$absent":true}}`, `{"b":1}`, ""},
		{"an absent member that is present", `{"a":{"$absent":true}}`, `{"a":null}`, "want no such member"},
		{"a captured value is held to its later use", `[{"$capture":"n"},{"$value":"n"}]`, `[7,7]`, ""},
		{"a captured value that differs later", `[{"$capture":"n"},{"$value":"n"}]`, `[7,8]`, "/1: got 8"},
		{"a captured string inside a string", `[{"$capture":"id"},"x_${id}"]`, `["a","x_b"]`, `/1: got "x_b"`},
		{"a symbol is bound by its first appearance", `["${drive:D}","${drive:D}"]`, `["drive_1","drive_2"]`, `/1: got "drive_2"`},
		{"a content's hash and size", `["${sha256:hello}",{"$size":"hello"}]`, `["5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",6]`, ""},
		{"a content's size that differs", `{"$size":"hello"}`, `7`, "got 7"},
		{"a number within its bounds", `{"$between":[1,60]}`, `60`, ""},
		{"a number outside its bounds", `{"$between":[1,60]}`, `61`, "want 1 to 60"},
		{"a repeated element", `{"$repeat":{"count":2,"each":{"path":"f${index}"}}}`, `[{"path":"f00000"},{"path":"f00001"}]`, ""},
		{"a repeated element out of order", `{"$repeat":{"count":2,"each":{"path":"f${index}"}}}`, `[{"path":"f00001"},{"path":"f00000"}]`, `/0/path: got "f00001"`},
		{"a repeated element one short", `{"$repeat":{"count":2,"each":{"path":"f${index}"}}}`, `[{"path":"f00000"}]`, "want an array of 2"},
	} {
		want, err := DecodeJSON([]byte(row.want))
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeJSON([]byte(row.got))
		if err != nil {
			t.Fatal(err)
		}
		err = NewBindings(map[string]Content{"hello": {Text: &hello}}).Match(want, got)
		switch {
		case row.refusal == "" && err != nil:
			t.Errorf("%s: %v; want a match", row.name, err)
		case row.refusal != "" && (err == nil || !strings.Contains(err.Error(), row.refusal)):
			t.Errorf("%s: %v; want a mismatch naming %q", row.name, err, row.refusal)
		}
	}
}
