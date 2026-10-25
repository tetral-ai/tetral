package protocol

import (
	"slices"
	"strconv"
	"unicode/utf8"
)

// Optional is a member a message may leave out; Set reports whether it is
// present.
type Optional[T any] struct {
	Value T
	Set   bool
}

func Some[T any](v T) Optional[T] { return Optional[T]{Value: v, Set: true} }

// Nullable is a member that may be absent, null or a value. The three stay
// distinct, so an omitted member never reads as null or as a schema default.
type Nullable[T any] struct {
	Value   T
	Present bool // the member holds Value
	Null    bool // the member is null
}

func Value[T any](v T) Nullable[T] { return Nullable[T]{Value: v, Present: true} }

func Null[T any]() Nullable[T] { return Nullable[T]{Null: true} }

// RawObject is a JSON object the protocol carries without reading it, such as
// MCP arguments, held as its compact encoding with members and number text
// as written.
type RawObject []byte

// encodable is every type of the package: each writes its own members.
type encodable interface {
	encode(e *encoder)
}

// Encode returns the compact JSON encoding of a protocol value: only the
// members it holds, null only for a member set to null, integers as exact
// decimal text, and the same string escaping as compactSize measures.
func Encode(v encodable) []byte {
	e := &encoder{}
	v.encode(e)
	return e.buf
}

// encoder writes compact JSON. Object members follow their schema
// definition's order.
type encoder struct {
	buf   []byte
	first []bool // per open object or array: nothing written in it yet
}

func (e *encoder) begin(c byte) {
	e.buf = append(e.buf, c)
	e.first = append(e.first, true)
}

func (e *encoder) end(c byte) {
	e.buf = append(e.buf, c)
	e.first = e.first[:len(e.first)-1]
}

// next starts a member or an item of the innermost object or array.
func (e *encoder) next() {
	n := len(e.first) - 1
	if !e.first[n] {
		e.buf = append(e.buf, ',')
	}
	e.first[n] = false
}

func (e *encoder) beginObject() { e.begin('{') }
func (e *encoder) endObject()   { e.end('}') }

func (e *encoder) key(name string) {
	e.next()
	e.buf = appendString(e.buf, name)
	e.buf = append(e.buf, ':')
}

func (e *encoder) str(s string)    { e.buf = appendString(e.buf, s) }
func (e *encoder) integer(n int64) { e.buf = strconv.AppendInt(e.buf, n, 10) }
func (e *encoder) boolean(b bool)  { e.buf = strconv.AppendBool(e.buf, b) }
func (e *encoder) null()           { e.buf = append(e.buf, "null"...) }
func (e *encoder) raw(r RawObject) { e.buf = append(e.buf, r...) }
func (e *encoder) strings(items []string) {
	e.begin('[')
	for _, s := range items {
		e.next()
		e.str(s)
	}
	e.end(']')
}

func (e *encoder) stringMap(m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	e.beginObject()
	for _, k := range keys {
		e.key(k)
		e.str(m[k])
	}
	e.endObject()
}

func list[T encodable](e *encoder, items []T) {
	e.begin('[')
	for _, item := range items {
		e.next()
		item.encode(e)
	}
	e.end(']')
}

func (e *encoder) stringMember(name, s string)        { e.key(name); e.str(s) }
func (e *encoder) integerMember(name string, n int64) { e.key(name); e.integer(n) }
func (e *encoder) boolMember(name string, b bool)     { e.key(name); e.boolean(b) }
func (e *encoder) valueMember(name string, v encodable) {
	e.key(name)
	v.encode(e)
}

// optionalMember writes nothing for an absent member.
func optionalMember[T any](e *encoder, name string, o Optional[T], write func(*encoder, T)) {
	if o.Set {
		e.key(name)
		write(e, o.Value)
	}
}

// nullableMember writes null only for a member set to null, and nothing for
// an absent one.
func nullableMember[T any](e *encoder, name string, n Nullable[T], write func(*encoder, T)) {
	switch {
	case n.Present:
		e.key(name)
		write(e, n.Value)
	case n.Null:
		e.key(name)
		e.null()
	}
}

func writeString(e *encoder, s string)               { e.str(s) }
func writeInteger(e *encoder, n int64)               { e.integer(n) }
func writeBool(e *encoder, b bool)                   { e.boolean(b) }
func writeRaw(e *encoder, r RawObject)               { e.raw(r) }
func writeStrings(e *encoder, items []string)        { e.strings(items) }
func writeStringMap(e *encoder, m map[string]string) { e.stringMap(m) }
func writeValue[T encodable](e *encoder, v T)        { v.encode(e) }
func writeList[T encodable](e *encoder, items []T)   { list(e, items) }

// appendValue appends the compact encoding of a parsed value.
func appendValue(buf []byte, v *value) []byte {
	switch v.kind {
	case kindNull:
		return append(buf, "null"...)
	case kindBool:
		return strconv.AppendBool(buf, v.boolean)
	case kindNumber:
		return append(buf, v.text...)
	case kindString:
		return appendString(buf, v.text)
	case kindArray:
		buf = append(buf, '[')
		for i, item := range v.items {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendValue(buf, item)
		}
		return append(buf, ']')
	default:
		buf = append(buf, '{')
		for i, m := range v.members {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendString(buf, m.key)
			buf = append(buf, ':')
			buf = appendValue(buf, m.value)
		}
		return append(buf, '}')
	}
}

const hexDigits = "0123456789abcdef"

func appendString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c >= utf8.RuneSelf {
			_, size := utf8.DecodeRuneInString(s[i:])
			buf = append(buf, s[i:i+size]...)
			i += size
			continue
		}
		switch c {
		case '"', '\\':
			buf = append(buf, '\\', c)
		case '\b':
			buf = append(buf, '\\', 'b')
		case '\f':
			buf = append(buf, '\\', 'f')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			if c < 0x20 {
				buf = append(buf, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			} else {
				buf = append(buf, c)
			}
		}
		i++
	}
	return append(buf, '"')
}
