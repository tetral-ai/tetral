package instance

import "testing"

func TestFingerprintIgnoresLayoutAndKeepsValuesAsGiven(t *testing.T) {
	for _, tc := range []struct {
		name  string
		a, b  string
		equal bool
	}{
		{"key order and whitespace, nested", `{"b":1,"a":[1,{"d":2,"c":"x"}]}`, ` { "a" : [ 1 , { "c" : "x" , "d" : 2 } ] , "b" : 1 } `, true},
		{"structural characters inside strings", `{"a":"x,y","b":"}{"}`, `{"b":"}{","a":"x,y"}`, true},
		{"omitted field and explicit null", `{}`, `{"expires_at":null}`, false},
		{"numbers as given", `{"n":1}`, `{"n":1.0}`, false},
		{"strings as given", `{"s":"A"}`, `{"s":"\u0041"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, okA := Fingerprint([]byte(tc.a))
			b, okB := Fingerprint([]byte(tc.b))
			if !okA || !okB || (a == b) != tc.equal {
				t.Fatalf("equal = %v (ok %v %v); want %v", a == b, okA, okB, tc.equal)
			}
		})
	}
	for _, body := range []string{`{"a":1,"a":1}`, `{"a":{"b":1,"b":2}}`, `{"a":1} x`, ``} {
		if _, ok := Fingerprint([]byte(body)); ok {
			t.Errorf("Fingerprint(%q) ok; want refused", body)
		}
	}
}
