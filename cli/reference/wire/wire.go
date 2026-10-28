package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"
)

// ProtocolVersion is the connection protocol version every envelope carries.
const ProtocolVersion = 1

// Message is a protocol message: one JSON value with a size limit.
type Message interface {
	// MaxBytes is the largest size of the message as received.
	MaxBytes() int
}

// Received is a message as read, unknown members included.
type Received struct {
	Tree  any
	Bytes int
}

// receivedChecker is a message whose rules read it as received: its size,
// or members that no type keeps.
type receivedChecker interface {
	checkReceived(Received) error
}

// checker is a type with rules beyond its members' types: relations between
// members, or the rule of a string. The decoder applies it to every value it
// reads, at every depth.
type checker interface {
	check() error
}

// Answer is a response that its request constrains.
type Answer[R any] interface {
	Message
	checkAnswer(request R) error
}

// DecodeErrorKind says which pass refused a message.
type DecodeErrorKind int

const (
	TooLarge DecodeErrorKind = iota + 1
	Strict
	Shape
	Rule
)

// DecodeError is a refused message.
type DecodeError struct {
	Kind   DecodeErrorKind
	Detail string
	Strict *StrictError
}

func (e *DecodeError) Error() string {
	switch e.Kind {
	case TooLarge:
		return "the message is over its limit: " + e.Detail
	case Strict:
		return e.Strict.Error()
	case Shape:
		return "the message does not match its schema: " + e.Detail
	default:
		return "the message breaks a protocol rule: " + e.Detail
	}
}

// ruleError is a broken rule beyond the schema.
type ruleError string

func (e ruleError) Error() string { return string(e) }

func rule(holds bool, broken string) error {
	if holds {
		return nil
	}
	return ruleError(broken)
}

// Decode reads one message: its size, the strict pass, the members by their
// exact names, then the rules of every value.
func Decode[M any, P interface {
	*M
	Message
}](input []byte) (M, error) {
	var message M
	if limit := P(&message).MaxBytes(); len(input) > limit {
		return message, &DecodeError{Kind: TooLarge, Detail: fmt.Sprintf("%d bytes over %d", len(input), limit)}
	}
	tree, err := readStrict(input)
	if err != nil {
		var strict *StrictError
		errors.As(err, &strict)
		return message, &DecodeError{Kind: Strict, Strict: strict}
	}
	if err := decodeValue(reflect.ValueOf(&message).Elem(), tree, "", tags{}); err != nil {
		return message, classify(err)
	}
	if received, ok := any(P(&message)).(receivedChecker); ok {
		if err := received.checkReceived(Received{Tree: tree, Bytes: len(input)}); err != nil {
			return message, classify(err)
		}
	}
	return message, nil
}

// DecodeAnswer reads a response as Decode does, then checks it against the
// request it answers.
func DecodeAnswer[R any, M any, P interface {
	*M
	Answer[R]
}](input []byte, request R) (M, error) {
	message, err := Decode[M, P](input)
	if err != nil {
		return message, err
	}
	if err := P(&message).checkAnswer(request); err != nil {
		return message, classify(err)
	}
	return message, nil
}

// Encode writes a message as compact UTF-8 JSON.
func Encode(message any) ([]byte, error) {
	var buffer strings.Builder
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(message); err != nil {
		return nil, err
	}
	return []byte(strings.TrimSuffix(buffer.String(), "\n")), nil
}

func classify(err error) *DecodeError {
	var broken ruleError
	if errors.As(err, &broken) {
		return &DecodeError{Kind: Rule, Detail: err.Error()}
	}
	return &DecodeError{Kind: Shape, Detail: err.Error()}
}

// treeDecoder is a type that reads itself from the tree.
type treeDecoder interface {
	decodeTree(tree any) error
}

// fieldPresence is a member type that may be absent or null: Opt, Maybe,
// Nullable and Absent.
type fieldPresence interface {
	decodeMember(tree any, present bool, path string, options tags) error
}

// Closed marks a struct whose members the protocol fixes: an unknown member
// is refused rather than ignored.
type Closed struct{}

var closedType = reflect.TypeFor[Closed]()

// tags are the constraints of the `wire` struct tag: `min` and `max` bound an
// integer or the length of an array, `unique` refuses an array with two equal
// items and `const` fixes a value.
type tags struct {
	min, max   *int64
	unique     bool
	constValue *string
}

func parseTags(tag string) tags {
	var options tags
	for _, part := range strings.Split(tag, ",") {
		name, value, _ := strings.Cut(part, "=")
		switch name {
		case "min", "max":
			number, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				panic("wire tag " + part)
			}
			if name == "min" {
				options.min = &number
			} else {
				options.max = &number
			}
		case "unique":
			options.unique = true
		case "const":
			options.constValue = &value
		case "":
		default:
			panic("wire tag " + part)
		}
	}
	return options
}

func (options tags) bound(value int64, what string) error {
	if options.min != nil && value < *options.min {
		return fmt.Errorf("%s %d below %d", what, value, *options.min)
	}
	if options.max != nil && value > *options.max {
		return fmt.Errorf("%s %d above %d", what, value, *options.max)
	}
	return nil
}

func decodeValue(value reflect.Value, tree any, path string, options tags) error {
	if decoder, ok := value.Addr().Interface().(treeDecoder); ok {
		if err := decoder.decodeTree(tree); err != nil {
			return prefixed(path, err)
		}
		return checked(value, path)
	}
	switch value.Kind() {
	case reflect.Struct:
		if err := decodeStruct(value, tree, path); err != nil {
			return err
		}
	case reflect.String:
		text, ok := tree.(string)
		if !ok {
			return fmt.Errorf("%s: want a string", where(path))
		}
		if options.constValue != nil && text != *options.constValue {
			return fmt.Errorf("%s: want %q", where(path), *options.constValue)
		}
		value.SetString(text)
	case reflect.Bool:
		flag, ok := tree.(bool)
		if !ok {
			return fmt.Errorf("%s: want a boolean", where(path))
		}
		if options.constValue != nil && strconv.FormatBool(flag) != *options.constValue {
			return fmt.Errorf("%s: want %s", where(path), *options.constValue)
		}
		value.SetBool(flag)
	case reflect.Int64:
		number, ok := tree.(json.Number)
		if !ok {
			return fmt.Errorf("%s: want an integer", where(path))
		}
		// An integer token only: 7.0 and 7e0 are refused, and nothing is
		// read through a float.
		integer, err := strconv.ParseInt(number.String(), 10, 64)
		if err != nil {
			return fmt.Errorf("%s: want an integer, found %s", where(path), number)
		}
		if options.constValue != nil && number.String() != *options.constValue {
			return fmt.Errorf("%s: want %s", where(path), *options.constValue)
		}
		if err := options.bound(integer, "value"); err != nil {
			return fmt.Errorf("%s: %w", where(path), err)
		}
		value.SetInt(integer)
	case reflect.Slice:
		items, ok := tree.([]any)
		if !ok {
			return fmt.Errorf("%s: want an array", where(path))
		}
		if err := options.bound(int64(len(items)), "item count"); err != nil {
			return fmt.Errorf("%s: %w", where(path), err)
		}
		if options.unique && !uniqueTrees(items) {
			return fmt.Errorf("%s: an item repeats", where(path))
		}
		slice := reflect.MakeSlice(value.Type(), len(items), len(items))
		for index, item := range items {
			if err := decodeValue(slice.Index(index), item, fmt.Sprintf("%s/%d", path, index), tags{}); err != nil {
				return err
			}
		}
		value.Set(slice)
	case reflect.Map:
		members, ok := tree.(map[string]any)
		if !ok || value.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("%s: want an object", where(path))
		}
		decoded := reflect.MakeMapWithSize(value.Type(), len(members))
		for name, member := range members {
			item := reflect.New(value.Type().Elem()).Elem()
			if err := decodeValue(item, member, path+"/"+name, tags{}); err != nil {
				return err
			}
			decoded.SetMapIndex(reflect.ValueOf(name).Convert(value.Type().Key()), item)
		}
		value.Set(decoded)
	case reflect.Interface:
		value.Set(reflect.ValueOf(&tree).Elem())
	default:
		return fmt.Errorf("%s: no decoder for %s", where(path), value.Type())
	}
	return checked(value, path)
}

func decodeStruct(value reflect.Value, tree any, path string) error {
	members, ok := tree.(map[string]any)
	if !ok {
		return fmt.Errorf("%s: want an object", where(path))
	}
	known := map[string]bool{}
	closed, err := decodeMembers(value, members, path, known)
	if err != nil {
		return err
	}
	if closed {
		for name := range members {
			if !known[name] {
				return fmt.Errorf("%s: unknown member %q", where(path), name)
			}
		}
	}
	return nil
}

// decodeMembers reads the members of one object into a struct's fields, and
// those of an embedded struct as its own, as encoding/json writes them. It
// records each member name it knows and reports whether the struct is
// Closed.
func decodeMembers(value reflect.Value, members map[string]any, path string, known map[string]bool) (bool, error) {
	closed := false
	for index := range value.NumField() {
		field := value.Type().Field(index)
		if field.Type == closedType {
			closed = true
			continue
		}
		if field.Anonymous && field.Tag.Get("json") == "" && field.Type.Kind() == reflect.Struct {
			embedded := value.Field(index)
			if _, err := decodeMembers(embedded, members, path, known); err != nil {
				return false, err
			}
			if err := checked(embedded, path); err != nil {
				return false, err
			}
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			continue
		}
		known[name] = true
		member, present := members[name]
		options := parseTags(field.Tag.Get("wire"))
		memberPath := path + "/" + name
		target := value.Field(index)
		if presence, ok := target.Addr().Interface().(fieldPresence); ok {
			if err := presence.decodeMember(member, present, memberPath, options); err != nil {
				return false, err
			}
			continue
		}
		if !present {
			return false, fmt.Errorf("%s: missing member %q", where(path), name)
		}
		if member == nil {
			return false, fmt.Errorf("%s: null", memberPath)
		}
		if err := decodeValue(target, member, memberPath, options); err != nil {
			return false, err
		}
	}
	return closed, nil
}

// decodeAt reads a tree into target, a pointer, reporting errors under path.
func decodeAt(target any, tree any, path string) error {
	return decodeValue(reflect.ValueOf(target).Elem(), tree, path, tags{})
}

func checked(value reflect.Value, path string) error {
	if rules, ok := value.Addr().Interface().(checker); ok {
		if err := rules.check(); err != nil {
			return prefixed(path, err)
		}
	}
	return nil
}

func prefixed(path string, err error) error {
	var broken ruleError
	if errors.As(err, &broken) {
		return ruleError(fmt.Sprintf("%s: %s", where(path), broken))
	}
	return fmt.Errorf("%s: %w", where(path), err)
}

func where(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

func uniqueTrees(items []any) bool {
	seen := map[string]bool{}
	for _, item := range items {
		// Encoding sorts object members, so equal values encode equally.
		key, err := json.Marshal(item)
		if err != nil || seen[string(key)] {
			return false
		}
		seen[string(key)] = true
	}
	return true
}

// variant is one shape of a tagged union: an object whose member, the
// union's tag, names the shape.
type variant interface{ variantName() string }

// decodeVariant reads a union's object: the tag picks one of variants, which
// reads the other members.
func decodeVariant[V variant](tree any, tag string, variants ...V) (V, error) {
	var none V
	members, ok := tree.(map[string]any)
	if !ok {
		return none, errors.New("want an object")
	}
	name, ok := members[tag].(string)
	if !ok {
		return none, fmt.Errorf("want the string member %q", tag)
	}
	for _, candidate := range variants {
		if candidate.variantName() == name {
			rest := maps.Clone(members)
			delete(rest, tag)
			return candidate, decodeAt(candidate, rest, "")
		}
	}
	return none, fmt.Errorf("unknown %s %q", tag, name)
}

// encodeVariant writes a variant with its tag as the first member.
func encodeVariant(tag string, value variant) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	head, err := json.Marshal(map[string]string{tag: value.variantName()})
	if err != nil {
		return nil, err
	}
	if string(body) == "{}" {
		return head, nil
	}
	return append(append(head[:len(head)-1], ','), body[1:]...), nil
}

// Object is a JSON object the protocol leaves open, such as a tool's input,
// kept as read.
type Object map[string]any

// Opt is a member that may be absent and is never null.
type Opt[T any] struct {
	Value T
	Set   bool
}

// Some is a present Opt.
func Some[T any](value T) Opt[T] { return Opt[T]{Value: value, Set: true} }

func (o Opt[T]) IsZero() bool { return !o.Set }

func (o Opt[T]) MarshalJSON() ([]byte, error) { return json.Marshal(o.Value) }

func (o *Opt[T]) decodeMember(tree any, present bool, path string, options tags) error {
	if !present {
		return nil
	}
	if tree == nil {
		return fmt.Errorf("%s: null", path)
	}
	o.Set = true
	return decodeValue(reflect.ValueOf(&o.Value).Elem(), tree, path, options)
}

// Nullable is a member that must be present and may be null.
type Nullable[T any] struct {
	Value T
	Valid bool
}

// NonNull is a Nullable holding a value.
func NonNull[T any](value T) Nullable[T] { return Nullable[T]{Value: value, Valid: true} }

func (n Nullable[T]) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.Value)
}

func (n *Nullable[T]) decodeMember(tree any, present bool, path string, options tags) error {
	if !present {
		return fmt.Errorf("%s: missing", path)
	}
	if tree == nil {
		return nil
	}
	n.Valid = true
	return decodeValue(reflect.ValueOf(&n.Value).Elem(), tree, path, options)
}

// Maybe is a member that may be absent, null or a value.
type Maybe[T any] struct {
	Value   T
	Present bool
	Valid   bool
}

func (m Maybe[T]) IsZero() bool { return !m.Present }

func (m Maybe[T]) MarshalJSON() ([]byte, error) {
	if !m.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(m.Value)
}

func (m *Maybe[T]) decodeMember(tree any, present bool, path string, options tags) error {
	m.Present = present
	if !present || tree == nil {
		return nil
	}
	m.Valid = true
	return decodeValue(reflect.ValueOf(&m.Value).Elem(), tree, path, options)
}

// Absent is a member that must be absent; it is never written.
type Absent struct{}

func (Absent) IsZero() bool { return true }

func (Absent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

func (*Absent) decodeMember(_ any, present bool, path string, _ tags) error {
	if present {
		return fmt.Errorf("%s: a member that must be absent is present", path)
	}
	return nil
}
