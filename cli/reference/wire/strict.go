package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// MaxNesting bounds how deep objects and arrays nest, at the depth the CLI's
// reader accepts.
const MaxNesting = 127

// StrictErrorKind says why the strict pass refused its input.
type StrictErrorKind int

const (
	InvalidUTF8 StrictErrorKind = iota + 1
	Syntax
	TrailingData
	TooDeep
	DuplicateMember
	LoneSurrogate
	NonFiniteNumber
	IntegerOutOfRange
)

func (kind StrictErrorKind) String() string {
	switch kind {
	case InvalidUTF8:
		return "invalid UTF-8"
	case Syntax:
		return "invalid JSON"
	case TrailingData:
		return "data after the JSON value"
	case TooDeep:
		return "nesting too deep"
	case DuplicateMember:
		return "a repeated member name"
	case LoneSurrogate:
		return "a lone surrogate escape"
	case NonFiniteNumber:
		return "a number that is not finite"
	case IntegerOutOfRange:
		return "an integer outside the signed 64-bit range"
	}
	return "unknown"
}

// StrictError is a refusal of the strict pass at a byte offset.
type StrictError struct {
	Kind   StrictErrorKind
	Offset int64
}

func (e *StrictError) Error() string {
	return fmt.Sprintf("%s at byte %d", e.Kind, e.Offset)
}

// readStrict is the strict pass: exactly one JSON value in valid UTF-8 with
// only whitespace around it, no repeated member name in any object, no `\u`
// escape of a lone surrogate, no number that is infinite as a 64-bit float
// and no integer token outside the signed 64-bit range. It returns the value
// as a tree of map[string]any, []any, string, json.Number, bool and nil.
func readStrict(input []byte) (any, error) {
	if !utf8.Valid(input) {
		return nil, &StrictError{Kind: InvalidUTF8}
	}
	reader := &strictReader{input: input, decoder: json.NewDecoder(bytes.NewReader(input))}
	reader.decoder.UseNumber()
	value, err := reader.value(0)
	if err != nil {
		return nil, err
	}
	if _, err := reader.decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, &StrictError{Kind: TrailingData, Offset: reader.decoder.InputOffset()}
	}
	return value, nil
}

type strictReader struct {
	input   []byte
	decoder *json.Decoder
}

// token reads the next token and returns it with the raw bytes it was read
// from, which hold the escapes that Token decodes away.
func (r *strictReader) token() (json.Token, []byte, error) {
	start := r.decoder.InputOffset()
	token, err := r.decoder.Token()
	if err != nil {
		return nil, nil, &StrictError{Kind: Syntax, Offset: start}
	}
	return token, r.input[start:r.decoder.InputOffset()], nil
}

func (r *strictReader) value(depth int) (any, error) {
	token, raw, err := r.token()
	if err != nil {
		return nil, err
	}
	return r.fromToken(token, raw, depth)
}

func (r *strictReader) fromToken(token json.Token, raw []byte, depth int) (any, error) {
	offset := r.decoder.InputOffset()
	switch token := token.(type) {
	case json.Delim:
		if depth+1 > MaxNesting {
			return nil, &StrictError{Kind: TooDeep, Offset: offset}
		}
		if token == '{' {
			return r.object(depth + 1)
		}
		if token == '[' {
			return r.array(depth + 1)
		}
		return nil, &StrictError{Kind: Syntax, Offset: offset}
	case string:
		if err := checkSurrogates(raw); err != nil {
			return nil, &StrictError{Kind: LoneSurrogate, Offset: offset}
		}
		return token, nil
	case json.Number:
		return token, checkNumber(token, offset)
	default:
		return token, nil
	}
}

func (r *strictReader) object(depth int) (any, error) {
	members := map[string]any{}
	for {
		token, raw, err := r.token()
		if err != nil {
			return nil, err
		}
		if token == json.Delim('}') {
			return members, nil
		}
		name, ok := token.(string)
		if !ok {
			return nil, &StrictError{Kind: Syntax, Offset: r.decoder.InputOffset()}
		}
		if err := checkSurrogates(raw); err != nil {
			return nil, &StrictError{Kind: LoneSurrogate, Offset: r.decoder.InputOffset()}
		}
		if _, repeated := members[name]; repeated {
			return nil, &StrictError{Kind: DuplicateMember, Offset: r.decoder.InputOffset()}
		}
		value, err := r.value(depth)
		if err != nil {
			return nil, err
		}
		members[name] = value
	}
}

func (r *strictReader) array(depth int) (any, error) {
	items := []any{}
	for {
		token, raw, err := r.token()
		if err != nil {
			return nil, err
		}
		if token == json.Delim(']') {
			return items, nil
		}
		value, err := r.fromToken(token, raw, depth)
		if err != nil {
			return nil, err
		}
		items = append(items, value)
	}
}

// checkSurrogates refuses a `\u` escape of a high surrogate not followed by
// one of a low surrogate, and of a low surrogate not preceded by a high one.
// raw holds the string token with the separators before it.
func checkSurrogates(raw []byte) error {
	for index := 0; index < len(raw); index++ {
		if raw[index] != '\\' {
			continue
		}
		if index+1 < len(raw) && raw[index+1] != 'u' {
			index++
			continue
		}
		unit, ok := escapeUnit(raw, index)
		if !ok {
			continue
		}
		switch {
		case utf16.IsSurrogate(unit) && unit < 0xdc00:
			low, ok := escapeUnit(raw, index+6)
			if !ok || low < 0xdc00 || low > 0xdfff {
				return errLoneSurrogate
			}
			index += 11
		case utf16.IsSurrogate(unit):
			return errLoneSurrogate
		default:
			index += 5
		}
	}
	return nil
}

var errLoneSurrogate = errors.New("lone surrogate")

func escapeUnit(raw []byte, at int) (rune, bool) {
	if at+6 > len(raw) || raw[at] != '\\' || raw[at+1] != 'u' {
		return 0, false
	}
	unit, err := strconv.ParseUint(string(raw[at+2:at+6]), 16, 16)
	return rune(unit), err == nil
}

func checkNumber(number json.Number, offset int64) error {
	text := number.String()
	if bytes.ContainsAny([]byte(text), ".eE") {
		value, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsInf(value, 0) {
			return &StrictError{Kind: NonFiniteNumber, Offset: offset}
		}
		return nil
	}
	if _, err := strconv.ParseInt(text, 10, 64); err != nil {
		return &StrictError{Kind: IntegerOutOfRange, Offset: offset}
	}
	return nil
}
