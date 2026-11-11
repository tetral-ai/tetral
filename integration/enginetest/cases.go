package enginetest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"

	contractfiles "github.com/tetral-ai/tetral/protocol"
)

// BehaviourCases is protocol/behaviour-v1.cases.json (protocol/README.md,
// "Behaviour cases").
type BehaviourCases struct {
	Version  int                `json:"version"`
	States   map[string]Named   `json:"states"`
	Stimuli  map[string]Named   `json:"stimuli"`
	Contents map[string]Content `json:"contents"`
	Cases    []Case             `json:"cases"`
}

// Named is one state or stimulus of the closed lists.
type Named struct {
	With  []string `json:"with"`
	Means string   `json:"means"`
}

// Content is one named file content.
type Content struct {
	Text  *string `json:"text,omitempty"`
	Byte  *string `json:"byte,omitempty"`
	Count int     `json:"count,omitempty"`
}

// Bytes returns the content.
func (c Content) Bytes() []byte {
	if c.Text != nil {
		return []byte(*c.Text)
	}
	return bytes.Repeat([]byte(*c.Byte), c.Count)
}

// Case is one behaviour case.
type Case struct {
	Name      string  `json:"name"`
	Direction string  `json:"direction"`
	Section   string  `json:"section"`
	Given     []Given `json:"given"`
	Steps     []Step  `json:"steps"`
}

// Given is one state a case starts from.
type Given struct {
	State string         `json:"state"`
	With  map[string]any `json:"with"`
}

// Step is one step: a request with its answer, an expectation or a
// stimulus.
type Step struct {
	Request  *CaseRequest   `json:"request,omitempty"`
	Answer   *CaseAnswer    `json:"answer,omitempty"`
	Expect   *Expectation   `json:"expect,omitempty"`
	Stimulus string         `json:"stimulus,omitempty"`
	With     map[string]any `json:"with,omitempty"`
}

// CaseRequest is a request a case sends.
type CaseRequest struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   map[string]string `json:"query,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    any               `json:"body,omitempty"`
	Content string            `json:"content,omitempty"`
	// DeclaresBytes is the Content-Length of a request that sends no byte
	// of its body.
	DeclaresBytes *int64 `json:"declares_bytes,omitempty"`
	// Invalid marks a request the case puts outside the contract on
	// purpose.
	Invalid bool `json:"invalid,omitempty"`
}

// CaseAnswer is what the contract requires of a request's answer.
type CaseAnswer struct {
	Status  int            `json:"status"`
	Headers map[string]any `json:"headers,omitempty"`
	Body    any            `json:"body,omitempty"`
	Content *ContentRange  `json:"content,omitempty"`
}

// ContentRange is the bytes From to To of content Of; the whole content
// without them.
type ContentRange struct {
	Of   string `json:"of"`
	From *int   `json:"from,omitempty"`
	To   *int   `json:"to,omitempty"`
}

// Expectation is the next event of the case's stream, the CLI's next
// request to a path, or the stream's end.
type Expectation struct {
	Event   any              `json:"event,omitempty"`
	Request *ExpectedRequest `json:"request,omitempty"`
	End     bool             `json:"end,omitempty"`
}

// ExpectedRequest is a request the CLI must send.
type ExpectedRequest struct {
	Path string `json:"path"`
	Body any    `json:"body"`
}

// LoadBehaviourCases reads the case file and checks its form: the keys the
// format has, one kind per step, states and stimuli of the closed lists
// with the members they take.
func LoadBehaviourCases() (BehaviourCases, error) {
	data, err := contractfiles.Files.ReadFile("behaviour-v1.cases.json")
	if err != nil {
		return BehaviourCases{}, err
	}
	var file BehaviourCases
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&file); err != nil {
		return BehaviourCases{}, fmt.Errorf("behaviour cases: %w", err)
	}
	if file.Version != 1 {
		return BehaviourCases{}, fmt.Errorf("behaviour cases: version %d", file.Version)
	}
	names := map[string]bool{}
	for _, c := range file.Cases {
		if c.Name == "" || names[c.Name] || c.Section == "" || (c.Direction != "engine" && c.Direction != "cli") {
			return BehaviourCases{}, fmt.Errorf("behaviour case %q: needs a name of its own, a section and a direction", c.Name)
		}
		names[c.Name] = true
		for _, given := range c.Given {
			if err := takes(file.States, given.State, given.With); err != nil {
				return BehaviourCases{}, fmt.Errorf("behaviour case %q: state %w", c.Name, err)
			}
		}
		for i, step := range c.Steps {
			kinds := 0
			for _, present := range []bool{step.Request != nil, step.Expect != nil, step.Stimulus != ""} {
				if present {
					kinds++
				}
			}
			if kinds != 1 || (step.Request != nil) != (step.Answer != nil) || (step.With != nil && step.Stimulus == "") {
				return BehaviourCases{}, fmt.Errorf("behaviour case %q: step %d is not one request with its answer, one expectation or one stimulus", c.Name, i+1)
			}
			if step.Stimulus != "" {
				if err := takes(file.Stimuli, step.Stimulus, step.With); err != nil {
					return BehaviourCases{}, fmt.Errorf("behaviour case %q: stimulus %w", c.Name, err)
				}
			}
		}
	}
	return file, nil
}

func takes(list map[string]Named, name string, with map[string]any) error {
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

// Direction returns the names of the cases of direction, in file order.
func (f BehaviourCases) Direction(direction string) []string {
	var names []string
	for _, c := range f.Cases {
		if c.Direction == direction {
			names = append(names, c.Name)
		}
	}
	return names
}

// Bindings is what one run of a case has bound: each symbol's identity,
// each captured value and the current connection.
type Bindings struct {
	contents   map[string]Content
	symbols    map[string]string
	values     map[string]any
	Connection string
	// index is the element a $repeat is at.
	index *int
}

// NewBindings returns the bindings of a run over contents.
func NewBindings(contents map[string]Content) *Bindings {
	return &Bindings{contents: contents, symbols: map[string]string{}, values: map[string]any{}}
}

// Bind gives symbol of kind (instance, credential, session, binding or
// drive) its identity.
func (b *Bindings) Bind(kind, symbol, identity string) { b.symbols[kind+":"+symbol] = identity }

// Identity returns the identity of symbol of kind.
func (b *Bindings) Identity(kind, symbol string) (string, bool) {
	identity, bound := b.symbols[kind+":"+symbol]
	return identity, bound
}

// Content returns content name.
func (b *Bindings) Content(name string) ([]byte, error) {
	content, found := b.contents[name]
	if !found {
		return nil, fmt.Errorf("no content %q", name)
	}
	return content.Bytes(), nil
}

var reference = regexp.MustCompile(`\$\{([a-z0-9_]+)(?::([A-Za-z0-9_.-]+))?\}`)

var errUnbound = errors.New("unbound")

// lookup resolves one ${kind:name} or ${name}.
func (b *Bindings) lookup(kind, name string) (string, error) {
	switch {
	case name == "" && kind == "connection":
		if b.Connection == "" {
			return "", fmt.Errorf("${connection}: %w", errUnbound)
		}
		return b.Connection, nil
	case name == "" && kind == "index":
		if b.index == nil {
			return "", errors.New("${index} outside a $repeat")
		}
		return fmt.Sprintf("%05d", *b.index), nil
	case name == "":
		value, bound := b.values[kind]
		if !bound {
			return "", fmt.Errorf("${%s}: %w", kind, errUnbound)
		}
		switch v := value.(type) {
		case string:
			return v, nil
		case json.Number:
			return v.String(), nil
		}
		return "", fmt.Errorf("${%s} is neither a string nor a number", kind)
	case kind == "sha256":
		content, err := b.Content(name)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(content)
		return hex.EncodeToString(sum[:]), nil
	}
	identity, bound := b.symbols[kind+":"+name]
	if !bound {
		return "", fmt.Errorf("${%s:%s}: %w", kind, name, errUnbound)
	}
	return identity, nil
}

// ExpandString replaces every reference of text.
func (b *Bindings) ExpandString(text string) (string, error) {
	var failed error
	expanded := reference.ReplaceAllStringFunc(text, func(match string) string {
		parts := reference.FindStringSubmatch(match)
		value, err := b.lookup(parts[1], parts[2])
		if err != nil && failed == nil {
			failed = err
		}
		return value
	})
	return expanded, failed
}

// Expand returns value, as the case file holds it, with every reference
// replaced: what a request or a stimulus is given.
func (b *Bindings) Expand(value any) (any, error) {
	switch v := value.(type) {
	case string:
		return b.ExpandString(v)
	case []any:
		expanded := make([]any, len(v))
		for i, item := range v {
			var err error
			if expanded[i], err = b.Expand(item); err != nil {
				return nil, err
			}
		}
		return expanded, nil
	case map[string]any:
		if name, ok := operand(v, "$size"); ok {
			content, err := b.Content(name)
			return json.Number(strconv.Itoa(len(content))), err
		}
		if name, ok := operand(v, "$value"); ok {
			bound, found := b.values[name]
			if !found {
				return nil, fmt.Errorf("$value %s: %w", name, errUnbound)
			}
			return bound, nil
		}
		if repeat, ok := v["$repeat"].(map[string]any); ok && len(v) == 1 {
			return b.repeat(repeat)
		}
		expanded := make(map[string]any, len(v))
		for name, member := range v {
			var err error
			if expanded[name], err = b.Expand(member); err != nil {
				return nil, err
			}
		}
		return expanded, nil
	}
	return value, nil
}

// repeat returns the array a $repeat stands for: count copies of each, in
// which ${index} is the copy's number from 0 as five digits.
func (b *Bindings) repeat(repeat map[string]any) (any, error) {
	number, _ := repeat["count"].(json.Number)
	count, err := number.Int64()
	if err != nil || len(repeat) != 2 || repeat["each"] == nil {
		return nil, errors.New("$repeat is {count, each}")
	}
	items := make([]any, count)
	defer func() { b.index = nil }()
	for i := range items {
		b.index = &i
		if items[i], err = b.Expand(repeat["each"]); err != nil {
			return nil, err
		}
	}
	return items, nil
}

func operand(object map[string]any, operator string) (string, bool) {
	name, ok := object[operator].(string)
	return name, ok && len(object) == 1
}

// Match compares got, a decoded JSON value read with json.Number, with
// want, as the case file holds it, and binds what want captures: a
// $capture, and a string that is one reference to a symbol or name not yet
// bound.
func (b *Bindings) Match(want, got any) error { return b.match("", want, got) }

func (b *Bindings) match(at string, want, got any) error {
	switch w := want.(type) {
	case string:
		text, isString := got.(string)
		expanded, err := b.ExpandString(w)
		if errors.Is(err, errUnbound) && isString {
			if parts := reference.FindStringSubmatch(w); parts != nil && parts[0] == w {
				b.capture(parts[1], parts[2], text)
				return nil
			}
		}
		if err != nil {
			return fmt.Errorf("%s: %w", at, err)
		}
		if !isString || text != expanded {
			return fmt.Errorf("%s: got %s, want %q", at, show(got), expanded)
		}
		return nil
	case []any:
		items, ok := got.([]any)
		if !ok || len(items) != len(w) {
			return fmt.Errorf("%s: got %s, want an array of %d", at, show(got), len(w))
		}
		for i := range w {
			if err := b.match(at+"/"+strconv.Itoa(i), w[i], items[i]); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		if name, ok := operand(w, "$capture"); ok {
			b.values[name] = got
			return nil
		}
		if bounds, ok := w["$between"].([]any); ok && len(w) == 1 {
			return between(at, bounds, got)
		}
		_, size := operand(w, "$size")
		_, repeat := w["$repeat"].(map[string]any)
		if _, value := operand(w, "$value"); size || value || (repeat && len(w) == 1) {
			expanded, err := b.Expand(w)
			if err != nil {
				return fmt.Errorf("%s: %w", at, err)
			}
			return b.match(at, expanded, got)
		}
		object, ok := got.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: got %s, want an object", at, show(got))
		}
		for _, name := range slices.Sorted(maps.Keys(w)) {
			member, present := object[name]
			if absent, ok := w[name].(map[string]any); ok && absent["$absent"] == true && len(absent) == 1 {
				if present {
					return fmt.Errorf("%s/%s: got %s, want no such member", at, name, show(member))
				}
				continue
			}
			if !present {
				return fmt.Errorf("%s/%s: missing", at, name)
			}
			if err := b.match(at+"/"+name, w[name], member); err != nil {
				return err
			}
		}
		return nil
	case json.Number:
		if number, ok := got.(json.Number); !ok || number.String() != w.String() {
			return fmt.Errorf("%s: got %s, want %s", at, show(got), w)
		}
		return nil
	}
	if want != got {
		return fmt.Errorf("%s: got %s, want %s", at, show(got), show(want))
	}
	return nil
}

// between holds got, a number or a string of decimal digits, to the two
// bounds, both included.
func between(at string, bounds []any, got any) error {
	var value int64
	var err error
	switch g := got.(type) {
	case json.Number:
		value, err = g.Int64()
	case string:
		value, err = strconv.ParseInt(g, 10, 64)
	default:
		err = errors.New("not a number")
	}
	low, _ := bounds[0].(json.Number)
	high, _ := bounds[len(bounds)-1].(json.Number)
	from, lowErr := low.Int64()
	to, highErr := high.Int64()
	if len(bounds) != 2 || lowErr != nil || highErr != nil {
		return fmt.Errorf("%s: $between is [low, high]", at)
	}
	if err != nil || value < from || value > to {
		return fmt.Errorf("%s: got %s, want %d to %d", at, show(got), from, to)
	}
	return nil
}

func (b *Bindings) capture(kind, name, text string) {
	switch {
	case name == "" && kind == "connection":
		b.Connection = text
	case name == "":
		b.values[kind] = text
	default:
		b.symbols[kind+":"+name] = text
	}
}

func show(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return abbreviate(data)
}

// DecodeJSON reads data as the value Match compares.
func DecodeJSON(data []byte) (any, error) {
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

// WithString returns member name of a state's or stimulus's with, a
// string, with its references replaced.
func (b *Bindings) WithString(with map[string]any, name string) (string, error) {
	text, ok := with[name].(string)
	if !ok {
		return "", fmt.Errorf("%q is not a string: %v", name, with[name])
	}
	return b.ExpandString(text)
}
