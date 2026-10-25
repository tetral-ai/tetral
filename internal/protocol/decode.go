package protocol

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// kind is the JSON type of one parsed value.
type kind uint8

const (
	kindNull kind = iota
	kindBool
	kindNumber
	kindString
	kindArray
	kindObject
)

func (k kind) String() string {
	return [...]string{"null", "boolean", "number", "string", "array", "object"}[k]
}

// value is one JSON value as the token pass read it: object members keep
// their order and numbers keep their exact text, so that no integer passes
// through float64 and an opaque object is carried unchanged.
type value struct {
	kind    kind
	boolean bool
	text    string // a string's unescaped text, or a number's exact text
	size    int    // a parsed string's length as written, quotes and escapes included
	items   []*value
	members []member
}

type member struct {
	key     string
	keySize int // the key's length as written
	value   *value
}

func (v *value) get(key string) (*value, bool) {
	for _, m := range v.members {
		if m.key == key {
			return m.value, true
		}
	}
	return nil, false
}

// maxNesting bounds the depth of arrays and objects the token pass accepts, so
// that a body of nested brackets cannot exhaust the stack of the recursive
// descent; no protocol message comes near it.
const maxNesting = 10000

// parse is the token pass: it reads one UTF-8 JSON text and rejects invalid
// UTF-8, an escaped lone surrogate, a duplicate object key at any depth (equal
// after unescaping), a nonstandard token such as NaN, a fraction or exponent
// beyond the float64 range, and trailing data.
func parse(data []byte) (*value, error) {
	p := parser{data: data}
	p.space()
	v, err := p.value("", 0)
	if err != nil {
		return nil, err
	}
	p.space()
	if p.pos != len(p.data) {
		return nil, invalid("", "data after the JSON value")
	}
	return v, nil
}

type parser struct {
	data []byte
	pos  int
}

func (p *parser) space() {
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *parser) value(ptr string, depth int) (*value, error) {
	if p.pos >= len(p.data) {
		return nil, invalid(ptr, "unexpected end of JSON")
	}
	switch c := p.data[p.pos]; {
	case c == '{':
		return p.object(ptr, depth+1)
	case c == '[':
		return p.array(ptr, depth+1)
	case c == '"':
		start := p.pos
		text, err := p.string(ptr)
		if err != nil {
			return nil, err
		}
		return &value{kind: kindString, text: text, size: p.pos - start}, nil
	case c == '-' || ('0' <= c && c <= '9'):
		return p.number(ptr)
	case p.literal("true"):
		return &value{kind: kindBool, boolean: true}, nil
	case p.literal("false"):
		return &value{kind: kindBool}, nil
	case p.literal("null"):
		return &value{kind: kindNull}, nil
	default:
		return nil, invalid(ptr, "invalid JSON token")
	}
}

func (p *parser) literal(word string) bool {
	if !strings.HasPrefix(string(p.data[p.pos:min(len(p.data), p.pos+len(word))]), word) {
		return false
	}
	p.pos += len(word)
	return true
}

func (p *parser) object(ptr string, depth int) (*value, error) {
	if depth > maxNesting {
		return nil, invalid(ptr, "JSON nested too deeply")
	}
	p.pos++
	v := &value{kind: kindObject}
	var keys map[string]struct{}
	p.space()
	if p.pos < len(p.data) && p.data[p.pos] == '}' {
		p.pos++
		return v, nil
	}
	for {
		p.space()
		if p.pos >= len(p.data) || p.data[p.pos] != '"' {
			return nil, invalid(ptr, "expected an object key")
		}
		keyStart := p.pos
		key, err := p.string(ptr)
		if err != nil {
			return nil, err
		}
		keySize := p.pos - keyStart
		child := pointer(ptr, key)
		if keys == nil {
			keys = make(map[string]struct{})
		}
		if _, ok := keys[key]; ok {
			return nil, invalid(child, "duplicate object key")
		}
		keys[key] = struct{}{}
		p.space()
		if p.pos >= len(p.data) || p.data[p.pos] != ':' {
			return nil, invalid(child, "expected ':' after an object key")
		}
		p.pos++
		p.space()
		item, err := p.value(child, depth)
		if err != nil {
			return nil, err
		}
		v.members = append(v.members, member{key: key, keySize: keySize, value: item})
		p.space()
		if p.pos < len(p.data) && p.data[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.pos < len(p.data) && p.data[p.pos] == '}' {
			p.pos++
			return v, nil
		}
		return nil, invalid(ptr, "expected ',' or '}' in an object")
	}
}

func (p *parser) array(ptr string, depth int) (*value, error) {
	if depth > maxNesting {
		return nil, invalid(ptr, "JSON nested too deeply")
	}
	p.pos++
	v := &value{kind: kindArray}
	p.space()
	if p.pos < len(p.data) && p.data[p.pos] == ']' {
		p.pos++
		return v, nil
	}
	for {
		p.space()
		item, err := p.value(pointer(ptr, strconv.Itoa(len(v.items))), depth)
		if err != nil {
			return nil, err
		}
		v.items = append(v.items, item)
		p.space()
		if p.pos < len(p.data) && p.data[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.pos < len(p.data) && p.data[p.pos] == ']' {
			p.pos++
			return v, nil
		}
		return nil, invalid(ptr, "expected ',' or ']' in an array")
	}
}

// string reads a JSON string at p.pos and returns its unescaped text.
func (p *parser) string(ptr string) (string, error) {
	p.pos++
	var out []byte
	start := p.pos
	for {
		if p.pos >= len(p.data) {
			return "", invalid(ptr, "unterminated string")
		}
		c := p.data[p.pos]
		switch {
		case c == '"':
			out = append(out, p.data[start:p.pos]...)
			p.pos++
			return string(out), nil
		case c == '\\':
			out = append(out, p.data[start:p.pos]...)
			r, err := p.escape(ptr)
			if err != nil {
				return "", err
			}
			out = utf8.AppendRune(out, r)
			start = p.pos
		case c < 0x20:
			return "", invalid(ptr, "unescaped control character in a string")
		case c < utf8.RuneSelf:
			p.pos++
		default:
			r, size := utf8.DecodeRune(p.data[p.pos:])
			if r == utf8.RuneError && size == 1 {
				return "", invalid(ptr, "invalid UTF-8")
			}
			p.pos += size
		}
	}
}

// escape reads one escape sequence; a \u escape of a surrogate must be the
// high half of a pair whose low half follows at once.
func (p *parser) escape(ptr string) (rune, error) {
	if p.pos+1 >= len(p.data) {
		return 0, invalid(ptr, "unterminated string")
	}
	c := p.data[p.pos+1]
	p.pos += 2
	switch c {
	case '"', '\\', '/':
		return rune(c), nil
	case 'b':
		return '\b', nil
	case 'f':
		return '\f', nil
	case 'n':
		return '\n', nil
	case 'r':
		return '\r', nil
	case 't':
		return '\t', nil
	case 'u':
	default:
		return 0, invalid(ptr, "invalid escape in a string")
	}
	r, ok := p.hex4()
	if !ok {
		return 0, invalid(ptr, "invalid \\u escape in a string")
	}
	if !utf16.IsSurrogate(r) {
		return r, nil
	}
	if r < 0xdc00 && p.pos+1 < len(p.data) && p.data[p.pos] == '\\' && p.data[p.pos+1] == 'u' {
		p.pos += 2
		low, ok := p.hex4()
		if ok {
			if pair := utf16.DecodeRune(r, low); pair != utf8.RuneError {
				return pair, nil
			}
		}
	}
	return 0, invalid(ptr, "lone surrogate escape in a string")
}

func (p *parser) hex4() (rune, bool) {
	if p.pos+4 > len(p.data) {
		return 0, false
	}
	n, err := strconv.ParseUint(string(p.data[p.pos:p.pos+4]), 16, 32)
	if err != nil {
		return 0, false
	}
	p.pos += 4
	return rune(n), true
}

func (p *parser) number(ptr string) (*value, error) {
	start := p.pos
	if p.data[p.pos] == '-' {
		p.pos++
	}
	switch {
	case p.pos < len(p.data) && p.data[p.pos] == '0':
		p.pos++
	case p.digits() == 0:
		return nil, invalid(ptr, "invalid number")
	}
	if p.pos < len(p.data) && p.data[p.pos] == '.' {
		p.pos++
		if p.digits() == 0 {
			return nil, invalid(ptr, "invalid number")
		}
	}
	if p.pos < len(p.data) && (p.data[p.pos] == 'e' || p.data[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.data) && (p.data[p.pos] == '+' || p.data[p.pos] == '-') {
			p.pos++
		}
		if p.digits() == 0 {
			return nil, invalid(ptr, "invalid number")
		}
	}
	text := string(p.data[start:p.pos])
	// A fraction or an exponent reads as a float64; one beyond its range
	// would be infinite, and the protocol has no nonfinite number. An
	// integer keeps its exact text whatever its length.
	if strings.ContainsAny(text, ".eE") {
		if f, _ := strconv.ParseFloat(text, 64); math.IsInf(f, 0) {
			return nil, invalid(ptr, "number beyond the float64 range")
		}
	}
	return &value{kind: kindNumber, text: text}, nil
}

func (p *parser) digits() int {
	start := p.pos
	for p.pos < len(p.data) && '0' <= p.data[p.pos] && p.data[p.pos] <= '9' {
		p.pos++
	}
	return p.pos - start
}

// pointer appends one reference token to a JSON pointer (RFC 6901).
func pointer(ptr, token string) string {
	token = strings.ReplaceAll(token, "~", "~0")
	return ptr + "/" + strings.ReplaceAll(token, "/", "~1")
}

// compactSize is the length of v's compact UTF-8 encoding, the measure of
// every byte limit of the three schema files: no whitespace, members as read,
// and every string and number token as written, escapes included. A string
// made in code, which has no written form, counts with the escaping of
// stringSize.
func compactSize(v *value) int {
	switch v.kind {
	case kindNull:
		return 4
	case kindBool:
		if v.boolean {
			return 4
		}
		return 5
	case kindNumber:
		return len(v.text)
	case kindString:
		return writtenSize(v.size, v.text)
	case kindArray:
		n := 2 + max(0, len(v.items)-1)
		for _, item := range v.items {
			n += compactSize(item)
		}
		return n
	default:
		n := 2 + max(0, len(v.members)-1)
		for _, m := range v.members {
			n += writtenSize(m.keySize, m.key) + 1 + compactSize(m.value)
		}
		return n
	}
}

func writtenSize(size int, text string) int {
	if size > 0 {
		return size
	}
	return stringSize(text)
}

func stringSize(s string) int {
	n := 2
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"' || c == '\\' || c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t':
			n += 2
		case c < 0x20:
			n += 6
		default:
			n++
		}
	}
	return n
}

// decoder runs the schema rules over a parsed value. It keeps the first
// failure, after which every step is a no-op.
type decoder struct {
	err error
	// unreadable holds the query parameters given more than once or with a
	// value that is not UTF-8: reading one fails, and one that the definition
	// does not model is never read.
	unreadable map[*value]string
	// unreadableErr is the first such read, recorded even after another
	// failure: that rule comes first, so its failure is the one returned.
	unreadableErr error
}

func (d *decoder) failed() bool { return d.err != nil }

func (d *decoder) fail(ptr, format string, args ...any) {
	if d.err == nil {
		d.err = invalid(ptr, fmt.Sprintf(format, args...))
	}
}

// decodeFunc reads one value at ptr. It returns the zero value once the
// decoder has failed.
type decodeFunc[T any] func(d *decoder, ptr string, v *value) T

// object is the view of one JSON object being decoded: each member a decoder
// reads is marked, and open or closed then accepts or rejects the rest.
type object struct {
	d    *decoder
	ptr  string
	v    *value
	used []bool
}

func (d *decoder) object(ptr string, v *value) *object {
	o := &object{d: d, ptr: ptr}
	if d.failed() {
		return o
	}
	if v.kind != kindObject {
		d.fail(ptr, "expected an object, got %s", v.kind)
		return o
	}
	o.v = v
	o.used = make([]bool, len(v.members))
	return o
}

// member returns the named member and its pointer, or nil when it is absent
// or the decoder has failed.
func (o *object) member(name string) (*value, string) {
	if o.v == nil {
		return nil, ""
	}
	for i, m := range o.v.members {
		if m.key != name {
			continue
		}
		if reason, ok := o.d.unreadable[m.value]; ok {
			if o.d.unreadableErr == nil {
				o.d.unreadableErr = invalid(pointer(o.ptr, name), reason)
			}
			o.d.fail(pointer(o.ptr, name), "%s", reason)
			return nil, ""
		}
		if o.d.failed() {
			return nil, ""
		}
		o.used[i] = true
		return m.value, pointer(o.ptr, name)
	}
	return nil, ""
}

func (o *object) has(name string) bool {
	if o.d.failed() {
		return false
	}
	_, ok := o.v.get(name)
	return ok
}

// absent rejects the members a definition marks as never carried.
func (o *object) absent(names ...string) {
	for _, name := range names {
		if _, ptr := o.member(name); ptr != "" {
			o.d.fail(ptr, "member %q is not allowed here", name)
		}
	}
}

// open ends an object whose definition allows unknown members: every member
// no decoder read is skipped.
func (o *object) open() {}

// closed rejects the first member no decoder read.
func (o *object) closed() {
	if o.d.failed() {
		return
	}
	for i, m := range o.v.members {
		if !o.used[i] {
			o.d.fail(pointer(o.ptr, m.key), "unknown member %q", m.key)
			return
		}
	}
}

func required[T any](o *object, name string, f decodeFunc[T]) T {
	var zero T
	v, ptr := o.member(name)
	if o.d.failed() {
		return zero
	}
	if v == nil {
		o.d.fail(o.ptr, "missing required member %q", name)
		return zero
	}
	return f(o.d, ptr, v)
}

func optional[T any](o *object, name string, f decodeFunc[T]) Optional[T] {
	v, ptr := o.member(name)
	if v == nil {
		return Optional[T]{}
	}
	return Optional[T]{Value: f(o.d, ptr, v), Set: true}
}

// nullable reads a member that may be absent, null or a value.
func nullable[T any](o *object, name string, f decodeFunc[T]) Nullable[T] {
	v, ptr := o.member(name)
	switch {
	case v == nil:
		return Nullable[T]{}
	case v.kind == kindNull:
		return Nullable[T]{Null: true}
	default:
		return Nullable[T]{Value: f(o.d, ptr, v), Present: true}
	}
}

// requiredNullable reads a member that must be present as null or a value.
func requiredNullable[T any](o *object, name string, f decodeFunc[T]) Nullable[T] {
	if !o.d.failed() && !o.has(name) {
		o.d.fail(o.ptr, "missing required member %q", name)
	}
	return nullable(o, name, f)
}

func anyString(d *decoder, ptr string, v *value) string {
	if d.failed() {
		return ""
	}
	if v.kind != kindString {
		d.fail(ptr, "expected a string, got %s", v.kind)
		return ""
	}
	return v.text
}

// stringCheck returns why a string breaks a schema constraint, or "".
type stringCheck func(string) string

func stringWith(checks ...stringCheck) decodeFunc[string] {
	return func(d *decoder, ptr string, v *value) string {
		s := anyString(d, ptr, v)
		for _, check := range checks {
			if d.failed() {
				break
			}
			if reason := check(s); reason != "" {
				d.fail(ptr, "%s", reason)
			}
		}
		return s
	}
}

// minLength and maxLength count code points, as JSON Schema does.
func minLength(n int) stringCheck {
	return func(s string) string {
		if utf8.RuneCountInString(s) < n {
			return fmt.Sprintf("shorter than %d characters", n)
		}
		return ""
	}
}

func maxLength(n int) stringCheck {
	return func(s string) string {
		if utf8.RuneCountInString(s) > n {
			return fmt.Sprintf("longer than %d characters", n)
		}
		return ""
	}
}

// noNUL is the pattern ^[^\u0000]+$ less its nonempty part.
func noNUL(s string) string {
	if strings.ContainsRune(s, 0) {
		return "contains U+0000"
	}
	return ""
}

// identity is the Id definition: nonempty and without U+0000.
var identity = stringWith(minLength(1), noNUL)

// absolutePath is the pattern ^/[^\u0000]*$.
func absolutePath(s string) string {
	if !strings.HasPrefix(s, "/") {
		return "not an absolute path"
	}
	return noNUL(s)
}

func lowerHex(n int) stringCheck {
	return func(s string) string {
		if len(s) != n || strings.Trim(s, "0123456789abcdef") != "" {
			return fmt.Sprintf("not %d lowercase hexadecimal digits", n)
		}
		return ""
	}
}

var sha256Hex = stringWith(lowerHex(64))

// isECMASpace is \s of the ECMAScript regular expressions that JSON Schema
// patterns use.
func isECMASpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
		return true
	}
	return 0x2000 <= r && r <= 0x200a
}

func enum(values ...string) decodeFunc[string] {
	return func(d *decoder, ptr string, v *value) string {
		s := anyString(d, ptr, v)
		if d.failed() || slices.Contains(values, s) {
			return s
		}
		d.fail(ptr, "%q is not one of %q", s, values)
		return s
	}
}

// checkEnum applies an enum to a string already read as a wider type.
func checkEnum(d *decoder, ptr, s string, values ...string) {
	if !d.failed() && !slices.Contains(values, s) {
		d.fail(ptr, "%q is not one of %q", s, values)
	}
}

func constString(want string) decodeFunc[string] { return enum(want) }

func boolean(d *decoder, ptr string, v *value) bool {
	if d.failed() {
		return false
	}
	if v.kind != kindBool {
		d.fail(ptr, "expected a boolean, got %s", v.kind)
		return false
	}
	return v.boolean
}

func constBool(want bool) decodeFunc[bool] {
	return func(d *decoder, ptr string, v *value) bool {
		b := boolean(d, ptr, v)
		if !d.failed() && b != want {
			d.fail(ptr, "expected %t", want)
		}
		return b
	}
}

// integerText parses an integer token from its exact decimal text; a
// fraction, an exponent or a value outside the signed 64-bit range fails.
func integerText(d *decoder, ptr string, v *value) int64 {
	if d.failed() {
		return 0
	}
	if v.kind != kindNumber {
		d.fail(ptr, "expected an integer, got %s", v.kind)
		return 0
	}
	if strings.ContainsAny(v.text, ".eE") {
		d.fail(ptr, "expected an integer, got %s", v.text)
		return 0
	}
	n, err := strconv.ParseInt(v.text, 10, 64)
	if err != nil {
		d.fail(ptr, "integer %s is outside the signed 64-bit range", v.text)
		return 0
	}
	return n
}

func integer(minimum, maximum int64) decodeFunc[int64] {
	return func(d *decoder, ptr string, v *value) int64 {
		n := integerText(d, ptr, v)
		if !d.failed() && (n < minimum || n > maximum) {
			d.fail(ptr, "integer %d is outside %d to %d", n, minimum, maximum)
		}
		return n
	}
}

var (
	anyInteger      = integer(math.MinInt64, math.MaxInt64)
	nonNegative     = integer(0, math.MaxInt64)
	positiveInteger = integer(1, math.MaxInt64)
)

func constInteger(want int64) decodeFunc[int64] { return integer(want, want) }

// arrayOf reads an array whose items f decodes, with JSON Schema's minItems
// and maxItems; maxItems < 0 is unbounded.
func arrayOf[T any](f decodeFunc[T], minItems, maxItems int) decodeFunc[[]T] {
	return func(d *decoder, ptr string, v *value) []T {
		if d.failed() {
			return nil
		}
		if v.kind != kindArray {
			d.fail(ptr, "expected an array, got %s", v.kind)
			return nil
		}
		if len(v.items) < minItems {
			d.fail(ptr, "fewer than %d items", minItems)
			return nil
		}
		if maxItems >= 0 && len(v.items) > maxItems {
			d.fail(ptr, "more than %d items", maxItems)
			return nil
		}
		items := make([]T, 0, len(v.items))
		for i, item := range v.items {
			items = append(items, f(d, pointer(ptr, strconv.Itoa(i)), item))
		}
		return items
	}
}

// uniqueStrings is uniqueItems for an array of strings.
func uniqueStrings(f decodeFunc[[]string]) decodeFunc[[]string] {
	return func(d *decoder, ptr string, v *value) []string {
		items := f(d, ptr, v)
		seen := make(map[string]bool, len(items))
		for i, item := range items {
			if seen[item] {
				d.fail(pointer(ptr, strconv.Itoa(i)), "%q appears twice", item)
				break
			}
			seen[item] = true
		}
		return items
	}
}

func rawObject(d *decoder, ptr string, v *value) RawObject {
	if d.failed() {
		return nil
	}
	if v.kind != kindObject {
		d.fail(ptr, "expected an object, got %s", v.kind)
		return nil
	}
	return RawObject(appendValue(nil, v))
}

func stringMap(d *decoder, ptr string, v *value) map[string]string {
	o := d.object(ptr, v)
	if d.failed() {
		return nil
	}
	m := make(map[string]string, len(v.members))
	for _, member := range v.members {
		m[member.key] = required(o, member.key, anyString)
	}
	return m
}

// decodeJSON runs the token pass and then f over the whole value, returning
// the decoded value and the parsed tree for the relations that follow.
func decodeJSON[T any](data []byte, f decodeFunc[T]) (T, *value, error) {
	var zero T
	v, err := parse(data)
	if err != nil {
		return zero, nil, err
	}
	d := &decoder{}
	out := f(d, "", v)
	if d.err != nil {
		return zero, nil, d.err
	}
	return out, v, nil
}

// checkBodySize answers a request body over its complete limit.
func checkBodySize(v *value, limit int, reason string) error {
	if compactSize(v) > limit {
		return tooLarge("", reason)
	}
	return nil
}

// decodeChecked decodes data with f and then applies check, the relations
// beyond the schema.
func decodeChecked[T any](data []byte, f decodeFunc[T], check func(T, *value) error) (T, error) {
	out, v, err := decodeJSON(data, f)
	if err == nil && check != nil {
		err = check(out, v)
	}
	if err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// bodySizeCheck is checkBodySize for a body with no other relation.
func bodySizeCheck[T any](limit int, reason string) func(T, *value) error {
	return func(_ T, v *value) error { return checkBodySize(v, limit, reason) }
}

// messageSizeCheck rejects a message that Engine sends or answers over its
// limit; no request of Engine's answers it.
func messageSizeCheck[T any](limit int, reason string) func(T, *value) error {
	return func(_ T, v *value) error {
		if compactSize(v) > limit {
			return invalid("", reason)
		}
		return nil
	}
}

// integerTokens runs check after requiring each named member of the top-level
// object, when present, to be an integer token, also where the definition
// does not model the member.
func integerTokens[T any](check func(T, *value) error, names ...string) func(T, *value) error {
	return func(out T, v *value) error {
		for _, name := range names {
			if m, ok := v.get(name); ok && (m.kind != kindNumber || strings.ContainsAny(m.text, ".eE")) {
				return invalid(pointer("", name), "not an integer token")
			}
		}
		if check == nil {
			return nil
		}
		return check(out, v)
	}
}

// decodeParameters decodes query parameters as the object of strings the
// schema's definition models.
func decodeParameters[T any](params map[string][]string, f decodeFunc[T]) (T, error) {
	var zero T
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}
	slices.Sort(names)
	v := &value{kind: kindObject}
	d := &decoder{unreadable: map[*value]string{}}
	for _, name := range names {
		values := params[name]
		if len(values) == 0 {
			continue
		}
		item := &value{kind: kindString, text: values[0]}
		if len(values) > 1 {
			d.unreadable[item] = "given more than once"
		} else if !utf8.ValidString(values[0]) {
			d.unreadable[item] = "invalid UTF-8"
		}
		v.members = append(v.members, member{key: name, value: item})
	}
	out := f(d, "", v)
	if d.unreadableErr != nil {
		return zero, d.unreadableErr
	}
	if d.err != nil {
		return zero, d.err
	}
	return out, nil
}

// singleValues gives each parameter of params its one value.
func singleValues(params map[string]string) map[string][]string {
	values := make(map[string][]string, len(params))
	for name, v := range params {
		values[name] = []string{v}
	}
	return values
}
