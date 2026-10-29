package protocol

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
)

func TestPolicyObjectsRejectUnknownMembers(t *testing.T) {
	// Managed rules are reserved protocol structure; this test does not
	// advertise a managed-network capability for personal computers.
	const control = `{"schema_version":1,"filesystem":{"rules":[{"path":"/project","access":"read"}]},"network":{"mode":"managed","rules":{"allow":[{"destination":{"kind":"host","value":"example.com"},"transport":"tcp","ports":[{"from":443,"to":443}]}],"deny":[],"listen":[{"address":"127.0.0.1","transport":"tcp","ports":[{"from":8080,"to":8080}]}]}}}`
	if _, err := DecodeSandboxPolicy([]byte(control)); err != nil {
		t.Fatalf("valid control: %v", err)
	}
	for _, test := range []struct{ name, path string }{
		{"sandboxPolicy", ""},
		{"filesystemPolicy", "/filesystem"},
		{"filesystemRule", "/filesystem/rules/0"},
		{"networkPolicy", "/network"},
		{"managedNetworkRules", "/network/rules"},
		{"networkRule", "/network/rules/allow/0"},
		{"networkDestination", "/network/rules/allow/0/destination"},
		{"networkListen", "/network/rules/listen/0"},
		{"portRange", "/network/rules/allow/0/ports/0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var root any
			if err := json.Unmarshal([]byte(control), &root); err != nil {
				t.Fatal(err)
			}
			object := root
			if test.path != "" {
				for _, segment := range strings.Split(test.path[1:], "/") {
					switch current := object.(type) {
					case map[string]any:
						object = current[segment]
					case []any:
						index, err := strconv.Atoi(segment)
						if err != nil {
							t.Fatal(err)
						}
						object = current[index]
					default:
						t.Fatalf("invalid test path %q", test.path)
					}
				}
			}
			object.(map[string]any)["unexpected"] = true
			body, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			_, err = DecodeSandboxPolicy(body)
			var failure *DecodeError
			if !errors.As(err, &failure) || failure.Pointer != test.path+"/unexpected" || failure.Reason != `unknown member "unexpected"` {
				t.Fatalf("decode = %v; want unknown member at %s/unexpected", err, test.path)
			}
		})
	}
}
