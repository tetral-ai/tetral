// Layer 1: the case file of protocol/behaviour-v1.cases.json as the runner of
// the Engine-direction cases reads it, with its references and matchers.
package reference

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The format of protocol/behaviour-v1.cases.json as protocol/README.md,
// "Behaviour cases", states it: the file, what a case's symbols and names
// stand for, and how an expected value matches a received one.

type caseFile struct {
	Version  int                    `json:"version"`
	States   map[string]caseNamed   `json:"states"`
	Stimuli  map[string]caseNamed   `json:"stimuli"`
	Contents map[string]caseContent `json:"contents"`
	Cases    []behaviourCase        `json:"cases"`
}

// caseNamed is one state or stimulus of the file's closed lists.
type caseNamed struct {
	With  []string `json:"with"`
	Means string   `json:"means"`
}

type caseContent struct {
	Text  *string `json:"text"`
	Byte  *string `json:"byte"`
	Count int     `json:"count"`
}

func (content caseContent) bytes() []byte {
	if content.Text != nil {
		return []byte(*content.Text)
	}
	return bytes.Repeat([]byte(*content.Byte), content.Count)
}

type behaviourCase struct {
	Name      string      `json:"name"`
	Direction string      `json:"direction"`
	Section   string      `json:"section"`
	Given     []caseGiven `json:"given"`
	Steps     []caseStep  `json:"steps"`
}

type caseGiven struct {
	State string         `json:"state"`
	With  map[string]any `json:"with"`
}

type caseStep struct {
	Request  *caseRequest   `json:"request"`
	Answer   *caseAnswer    `json:"answer"`
	Expect   *caseExpect    `json:"expect"`
	Stimulus string         `json:"stimulus"`
	With     map[string]any `json:"with"`
}

type caseRequest struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   map[string]string `json:"query"`
	Headers map[string]string `json:"headers"`
	// Body is the JSON body, empty when the request has none.
	Body          json.RawMessage `json:"body"`
	Content       string          `json:"content"`
	Credential    string          `json:"credential"`
	DeclaresBytes *int64          `json:"declares_bytes"`
	// Invalid marks a request outside the contract. This runner sends every
	// request as written, so it reads the mark and does nothing with it.
	Invalid bool `json:"invalid"`
}

type caseAnswer struct {
	Status  int             `json:"status"`
	Headers map[string]any  `json:"headers"`
	Body    json.RawMessage `json:"body"`
	Content *caseRange      `json:"content"`
}

// caseRange is the bytes From to To of content Of, both included; the whole
// content without them.
type caseRange struct {
	Of   string `json:"of"`
	From *int   `json:"from"`
	To   *int   `json:"to"`
}

type caseExpect struct {
	Event   json.RawMessage `json:"event"`
	Request json.RawMessage `json:"request"`
	End     bool            `json:"end"`
}

// loadCases reads the case file and holds it to its form: no member the
// format lacks, each step one request with its answer, one expectation or
// one stimulus, each state and stimulus of the file's lists with the members
// it takes.
func loadCases() (caseFile, error) {
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", "behaviour-v1.cases.json"))
	if err != nil {
		return caseFile{}, err
	}
	var file caseFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&file); err != nil {
		return caseFile{}, err
	}
	if file.Version != 1 {
		return caseFile{}, fmt.Errorf("version %d", file.Version)
	}
	names := map[string]bool{}
	for _, c := range file.Cases {
		if c.Name == "" || names[c.Name] || c.Section == "" || (c.Direction != "engine" && c.Direction != "cli") {
			return caseFile{}, fmt.Errorf("case %q needs a name of its own, a section and a direction", c.Name)
		}
		names[c.Name] = true
		for _, given := range c.Given {
			if err := takes(file.States, given.State, given.With); err != nil {
				return caseFile{}, fmt.Errorf("case %q: state %w", c.Name, err)
			}
		}
		for index, step := range c.Steps {
			kinds := 0
			for _, present := range []bool{step.Request != nil, step.Expect != nil, step.Stimulus != ""} {
				if present {
					kinds++
				}
			}
			if kinds != 1 || (step.Request != nil) != (step.Answer != nil) || (step.With != nil && step.Stimulus == "") {
				return caseFile{}, fmt.Errorf("case %q: step %d is not one request with its answer, one expectation or one stimulus", c.Name, index+1)
			}
			if step.Stimulus != "" {
				if err := takes(file.Stimuli, step.Stimulus, step.With); err != nil {
					return caseFile{}, fmt.Errorf("case %q: stimulus %w", c.Name, err)
				}
			}
		}
	}
	return file, nil
}

func takes(list map[string]caseNamed, name string, with map[string]any) error {
	named, found := list[name]
	if !found {
		return fmt.Errorf("%q is not in the file's list", name)
	}
	for member := range with {
		if !slices.Contains(named.With, member) {
			return fmt.Errorf("%q takes no %q", name, member)
		}
	}
	return nil
}

// decodeCaseValue reads JSON as the value a match compares: its numbers
// stay as written.
func decodeCaseValue(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, errors.New("data after the JSON value")
	}
	return value, nil
}

// caseBindings is what one run of a case has bound: each symbol's identity,
// each captured value and the connection of the latest `connected`.
type caseBindings struct {
	contents   map[string]caseContent
	symbols    map[string]string
	values     map[string]any
	connection string
	// index is the copy a $repeat is at.
	index *int
	// expecting is set while an expectation is expanded: a $value then
	// stays, for the match to compare.
	expecting bool
}

var (
	caseReference = regexp.MustCompile(`\$\{([a-z0-9_]+)(?::([A-Za-z0-9_.-]+))?\}`)
	errUnbound    = errors.New("not bound")
	// canonicalDecimal is the Decimal of the drive transfer schema.
	canonicalDecimal = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
)

// identityPrefix is the prefix of the identity this runner gives a symbol
// of each kind.
var identityPrefix = map[string]string{
	"instance": "inst_", "credential": "ccred_", "session": "sess_", "binding": "bind_", "drive": "drive_",
}

// bind gives the symbol of kind its identity, as the state or stimulus that
// creates the thing does, and returns it.
func (bound *caseBindings) bind(kind, symbol string) string {
	identity := identityPrefix[kind] + strings.ToLower(symbol)
	bound.symbols[kind+":"+symbol] = identity
	return identity
}

func (bound *caseBindings) content(name string) ([]byte, error) {
	content, found := bound.contents[name]
	if !found {
		return nil, fmt.Errorf("no content %q", name)
	}
	return content.bytes(), nil
}

// numberText is a JSON number as its decimal text: one read from the case
// file or an answer (json.Number), or from an event (float64).
func numberText(value any) (string, bool) {
	switch number := value.(type) {
	case json.Number:
		return number.String(), true
	case float64:
		return strconv.FormatFloat(number, 'f', -1, 64), true
	}
	return "", false
}

// lookup resolves one ${kind:name} or ${name}.
func (bound *caseBindings) lookup(kind, name string) (string, error) {
	switch {
	case name == "" && kind == "connection":
		if bound.connection == "" {
			return "", fmt.Errorf("${connection}: %w", errUnbound)
		}
		return bound.connection, nil
	case name == "" && kind == "index":
		if bound.index == nil {
			return "", errors.New("${index} outside a $repeat")
		}
		return fmt.Sprintf("%05d", *bound.index), nil
	case name == "":
		value, found := bound.values[kind]
		if !found {
			return "", fmt.Errorf("${%s}: %w", kind, errUnbound)
		}
		if text, isString := value.(string); isString {
			return text, nil
		}
		if text, isNumber := numberText(value); isNumber {
			return text, nil
		}
		return "", fmt.Errorf("${%s} is neither a string nor a number", kind)
	case kind == "sha256":
		content, err := bound.content(name)
		if err != nil {
			return "", err
		}
		digest := sha256.Sum256(content)
		return hex.EncodeToString(digest[:]), nil
	}
	if _, isKind := identityPrefix[kind]; !isKind {
		return "", fmt.Errorf("${%s:%s}: no such kind of symbol", kind, name)
	}
	identity, found := bound.symbols[kind+":"+name]
	if !found {
		return "", fmt.Errorf("${%s:%s}: %w", kind, name, errUnbound)
	}
	return identity, nil
}

// expandString replaces every reference of text.
func (bound *caseBindings) expandString(text string) (string, error) {
	var failed error
	expanded := caseReference.ReplaceAllStringFunc(text, func(match string) string {
		parts := caseReference.FindStringSubmatch(match)
		value, err := bound.lookup(parts[1], parts[2])
		if err != nil && failed == nil {
			failed = err
		}
		return value
	})
	return expanded, failed
}

// operand is the name a one-member object such as {"$size": "hello"} gives
// its operator.
func operand(object map[string]any, operator string) (string, bool) {
	name, isString := object[operator].(string)
	return name, isString && len(object) == 1
}

// expand returns value, as the case file holds it, with every reference,
// $size, $value and $repeat replaced: what a request or a stimulus is
// given. A matcher that only an expectation may hold fails it.
func (bound *caseBindings) expand(value any) (any, error) {
	switch given := value.(type) {
	case string:
		return bound.expandString(given)
	case []any:
		expanded := make([]any, len(given))
		for index, item := range given {
			var err error
			if expanded[index], err = bound.expand(item); err != nil {
				return nil, err
			}
		}
		return expanded, nil
	case map[string]any:
		if name, is := operand(given, "$size"); is {
			content, err := bound.content(name)
			return json.Number(strconv.Itoa(len(content))), err
		}
		if name, is := operand(given, "$value"); is {
			if bound.expecting {
				return given, nil
			}
			captured, found := bound.values[name]
			if !found {
				return nil, fmt.Errorf("$value %s: %w", name, errUnbound)
			}
			return captured, nil
		}
		if repeat, is := given["$repeat"]; is {
			return bound.repeat(given, repeat)
		}
		expanded := make(map[string]any, len(given))
		for name, member := range given {
			if strings.HasPrefix(name, "$") {
				return nil, fmt.Errorf("%s stands in no request and no stimulus", name)
			}
			var err error
			if expanded[name], err = bound.expand(member); err != nil {
				return nil, err
			}
		}
		return expanded, nil
	}
	return value, nil
}

// repeat returns the array a $repeat stands for.
func (bound *caseBindings) repeat(object map[string]any, repeat any) (any, error) {
	members, _ := repeat.(map[string]any)
	number, _ := members["count"].(json.Number)
	count, err := number.Int64()
	each, hasEach := members["each"]
	if len(object) != 1 || len(members) != 2 || err != nil || count < 0 || !hasEach {
		return nil, errors.New("$repeat is {count, each}")
	}
	items := make([]any, count)
	defer func() { bound.index = nil }()
	for index := range items {
		bound.index = &index
		if items[index], err = bound.expand(each); err != nil {
			return nil, err
		}
	}
	return items, nil
}

// match holds got, a received JSON value, to want, as the case file holds
// it, and binds what want names: a $capture, and a string that is one
// reference to a symbol or to the connection not bound before. A name is
// bound by its $capture only, so that a misspelled one fails.
func (bound *caseBindings) match(at string, want, got any) error {
	switch expected := want.(type) {
	case string:
		text, isString := got.(string)
		expanded, err := bound.expandString(expected)
		if errors.Is(err, errUnbound) && isString {
			parts := caseReference.FindStringSubmatch(expected)
			switch {
			case parts == nil || parts[0] != expected:
			case parts[2] != "":
				bound.symbols[parts[1]+":"+parts[2]] = text
				return nil
			case parts[1] == "connection":
				bound.connection = text
				return nil
			}
		}
		if err != nil {
			return fmt.Errorf("%s: %w", at, err)
		}
		if !isString || text != expanded {
			return fmt.Errorf("%s: got %s, want %q", at, shown(got), expanded)
		}
		return nil
	case []any:
		items, isArray := got.([]any)
		if !isArray || len(items) != len(expected) {
			return fmt.Errorf("%s: got %s, want an array of %d", at, shown(got), len(expected))
		}
		for index := range expected {
			if err := bound.match(at+"/"+strconv.Itoa(index), expected[index], items[index]); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		return bound.matchObject(at, expected, got)
	case json.Number:
		text, isNumber := numberText(got)
		if !isNumber || text != expected.String() {
			return fmt.Errorf("%s: got %s, want %s", at, shown(got), expected)
		}
		return nil
	case bool, nil:
		if want != got {
			return fmt.Errorf("%s: got %s, want %s", at, shown(got), shown(want))
		}
		return nil
	}
	return fmt.Errorf("%s: an expected value of type %T", at, want)
}

func (bound *caseBindings) matchObject(at string, expected map[string]any, got any) error {
	if name, is := operand(expected, "$capture"); is {
		bound.values[name] = got
		return nil
	}
	if limits, is := expected["$between"]; is {
		return between(at, expected, limits, got)
	}
	if name, is := operand(expected, "$value"); is {
		// A captured value is compared as it was received, not as an
		// expectation that an object with more members would meet.
		captured, found := bound.values[name]
		if !found {
			return fmt.Errorf("%s: $value %s: %w", at, name, errUnbound)
		}
		if !sameValue(captured, got) {
			return fmt.Errorf("%s: got %s, want %s", at, shown(got), shown(captured))
		}
		return nil
	}
	_, isSize := operand(expected, "$size")
	if _, isRepeat := expected["$repeat"]; isSize || isRepeat {
		bound.expecting = true
		expanded, err := bound.expand(expected)
		bound.expecting = false
		if err != nil {
			return fmt.Errorf("%s: %w", at, err)
		}
		return bound.match(at, expanded, got)
	}
	object, isObject := got.(map[string]any)
	if !isObject {
		return fmt.Errorf("%s: got %s, want an object", at, shown(got))
	}
	for _, name := range slices.Sorted(maps.Keys(expected)) {
		if strings.HasPrefix(name, "$") {
			return fmt.Errorf("%s: matcher %s is not implemented", at, name)
		}
		member, present := object[name]
		if absent, is := expected[name].(map[string]any); is && len(absent) == 1 && absent["$absent"] == true {
			if present {
				return fmt.Errorf("%s/%s: got %s, want no such member", at, name, shown(member))
			}
			continue
		}
		if !present {
			return fmt.Errorf("%s/%s: missing in %s", at, name, shown(got))
		}
		if err := bound.match(at+"/"+name, expected[name], member); err != nil {
			return err
		}
	}
	return nil
}

// matchHeader holds an answer's header to want. A $between takes the
// header's value as the decimal it must be; a header the answer lacks
// matches nothing.
func (bound *caseBindings) matchHeader(at string, want any, values []string) error {
	if len(values) != 1 {
		return fmt.Errorf("%s: got %q, want one value", at, values)
	}
	if object, isObject := want.(map[string]any); isObject && object["$between"] != nil {
		if !canonicalDecimal.MatchString(values[0]) {
			return fmt.Errorf("%s: got %q, want a decimal", at, values[0])
		}
		return bound.match(at, want, json.Number(values[0]))
	}
	return bound.match(at, want, values[0])
}

// sameValue compares two received JSON values, whose numbers may have come
// from an answer or from an event.
func sameValue(a, b any) bool {
	if textA, isNumber := numberText(a); isNumber {
		textB, alsoNumber := numberText(b)
		return alsoNumber && textA == textB
	}
	encodedA, errA := json.Marshal(a)
	encodedB, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(encodedA, encodedB)
}

// between holds got, a number, to the two limits, both included.
func between(at string, object map[string]any, limits, got any) error {
	pair, _ := limits.([]any)
	if len(object) != 1 || len(pair) != 2 {
		return fmt.Errorf("%s: $between is [low, high]", at)
	}
	low, lowErr := integer(pair[0])
	high, highErr := integer(pair[1])
	if lowErr != nil || highErr != nil {
		return fmt.Errorf("%s: $between is [low, high]", at)
	}
	value, err := integer(got)
	if err != nil || value < low || value > high {
		return fmt.Errorf("%s: got %s, want %d to %d", at, shown(got), low, high)
	}
	return nil
}

// integer is a JSON number that is an integer.
func integer(value any) (int64, error) {
	text, isNumber := numberText(value)
	if !isNumber {
		return 0, fmt.Errorf("%s is no number", shown(value))
	}
	return strconv.ParseInt(text, 10, 64)
}

// shown is a value as a failure message prints it, cut to a line.
func shown(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	if len(encoded) > 400 {
		return string(encoded[:400]) + "…"
	}
	return string(encoded)
}

func TestACaseExpectationFailsOnWhatItDoesNotHold(t *testing.T) {
	value := func(text string) any {
		decoded, err := decodeCaseValue(json.RawMessage(text))
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		return decoded
	}
	bound := &caseBindings{symbols: map[string]string{}, values: map[string]any{"actor": value(`{"type":"api_actor"}`)}}
	for _, row := range []struct {
		want, got string
		// header is set where got is an answer header's values.
		header  []string
		failure string
	}{
		{want: `{"a":{"$between":[0,45000]}}`, got: `{"a":30000}`},
		{want: `{"a":{"$between":[0,45000]}}`, got: `{"a":"30000"}`, failure: `body/a: got "30000", want 0 to 45000`},
		{want: `{"$between":[1,60]}`, header: []string{"30"}},
		{want: `{"$between":[1,60]}`, header: []string{"+30"}, failure: `body: got "+30", want a decimal`},
		{want: `{"$between":[1,60]}`, header: []string{"030"}, failure: `body: got "030", want a decimal`},
		{want: `{"$capture":"tag"}`, header: nil, failure: `body: got [], want one value`},
		{want: `{"a":"${connection}"}`, got: `{"a":"conn_1"}`},
		{want: `{"a":"${connection}"}`, got: `{"a":"conn_2"}`, failure: `body/a: got "conn_2", want "conn_1"`},
		{want: `{"a":"${secnd}"}`, got: `{"a":"msg_1"}`, failure: `body/a: ${secnd}: not bound`},
		{want: `{"$repeat":{"count":1,"each":{"$value":"actor"}}}`, got: `[{"type":"api_actor"}]`},
		{
			want: `{"$repeat":{"count":1,"each":{"$value":"actor"}}}`, got: `[{"type":"api_actor","extra":1}]`,
			failure: `body/0: got {"extra":1,"type":"api_actor"}, want {"type":"api_actor"}`,
		},
	} {
		var err error
		if row.got == "" {
			err = bound.matchHeader("body", value(row.want), row.header)
		} else {
			err = bound.match("body", value(row.want), value(row.got))
		}
		if failure := fmt.Sprint(err); (err == nil) != (row.failure == "") || err != nil && failure != row.failure {
			t.Errorf("%s against %s%q: %v; want %q", row.want, row.got, row.header, err, row.failure)
		}
	}
}
