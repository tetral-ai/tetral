package agentruntimebridge

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strings"
)

const runToolCanonicalJSONMaxDepth = 256

// canonicalRunToolInput implements the same raw-token algorithm as Runtime's
// canonicalRunToolJSON. It never decodes JSON strings or numbers: insignificant
// whitespace is removed, arrays retain order, and object members are ordered
// by the UTF-8 bytes of their raw key token. This preserves every scalar byte
// emitted by JSON.stringify, including lone-surrogate escapes.
func canonicalRunToolInput(raw string) (string, string, error) {
	canonical, err := canonicalRunToolJSON(raw)
	if err != nil {
		return "", "", err
	}
	return canonical, sha256Hex(canonical), nil
}

func canonicalRunToolJSON(raw string) (string, error) {
	return canonicalRunToolJSONWithoutObjectFields(raw, nil)
}

func canonicalRunToolJSONWithoutObjectFields(raw string, excludedObjectFields map[string]struct{}) (string, error) {
	if !json.Valid([]byte(raw)) {
		return "", errors.New("invalid JSON")
	}
	parser := runToolCanonicalJSONParser{raw: raw, excludedObjectFields: excludedObjectFields}
	canonical, err := parser.parseValue(0)
	if err != nil {
		return "", err
	}
	parser.skipWhitespace()
	if parser.offset != len(raw) {
		return "", errors.New("trailing JSON input")
	}
	return canonical, nil
}

type runToolCanonicalJSONParser struct {
	raw                  string
	offset               int
	excludedObjectFields map[string]struct{}
}

type runToolCanonicalJSONMember struct {
	key   string
	value string
}

func (p *runToolCanonicalJSONParser) parseValue(depth int) (string, error) {
	if depth > runToolCanonicalJSONMaxDepth {
		return "", errors.New("JSON nesting exceeds canonicalization bound")
	}
	p.skipWhitespace()
	if p.offset >= len(p.raw) {
		return "", errors.New("missing JSON value")
	}
	switch p.raw[p.offset] {
	case '{':
		return p.parseObject(depth + 1)
	case '[':
		return p.parseArray(depth + 1)
	case '"':
		return p.parseStringToken()
	default:
		start := p.offset
		for p.offset < len(p.raw) && !isRunToolJSONDelimiter(p.raw[p.offset]) {
			p.offset++
		}
		if start == p.offset {
			return "", errors.New("invalid JSON scalar")
		}
		return p.raw[start:p.offset], nil
	}
}

func (p *runToolCanonicalJSONParser) parseObject(depth int) (string, error) {
	p.offset++
	p.skipWhitespace()
	members := make([]runToolCanonicalJSONMember, 0)
	if p.consume('}') {
		return "{}", nil
	}
	for {
		p.skipWhitespace()
		key, err := p.parseStringToken()
		if err != nil {
			return "", err
		}
		p.skipWhitespace()
		if !p.consume(':') {
			return "", errors.New("missing JSON object colon")
		}
		value, err := p.parseValue(depth)
		if err != nil {
			return "", err
		}
		excluded, err := p.objectFieldExcluded(key)
		if err != nil {
			return "", err
		}
		if !excluded {
			members = append(members, runToolCanonicalJSONMember{key: key, value: value})
		}
		p.skipWhitespace()
		if p.consume('}') {
			break
		}
		if !p.consume(',') {
			return "", errors.New("missing JSON object comma")
		}
	}
	sort.SliceStable(members, func(left int, right int) bool {
		return bytes.Compare([]byte(members[left].key), []byte(members[right].key)) < 0
	})
	var canonical strings.Builder
	canonical.WriteByte('{')
	for index, member := range members {
		if index > 0 {
			canonical.WriteByte(',')
		}
		canonical.WriteString(member.key)
		canonical.WriteByte(':')
		canonical.WriteString(member.value)
	}
	canonical.WriteByte('}')
	return canonical.String(), nil
}

func (p *runToolCanonicalJSONParser) objectFieldExcluded(rawKey string) (bool, error) {
	if len(p.excludedObjectFields) == 0 {
		return false, nil
	}
	var key string
	if err := json.Unmarshal([]byte(rawKey), &key); err != nil {
		return false, err
	}
	_, excluded := p.excludedObjectFields[key]
	return excluded, nil
}

func (p *runToolCanonicalJSONParser) parseArray(depth int) (string, error) {
	p.offset++
	p.skipWhitespace()
	values := make([]string, 0)
	if p.consume(']') {
		return "[]", nil
	}
	for {
		value, err := p.parseValue(depth)
		if err != nil {
			return "", err
		}
		values = append(values, value)
		p.skipWhitespace()
		if p.consume(']') {
			break
		}
		if !p.consume(',') {
			return "", errors.New("missing JSON array comma")
		}
	}
	return "[" + strings.Join(values, ",") + "]", nil
}

func (p *runToolCanonicalJSONParser) parseStringToken() (string, error) {
	if !p.consume('"') {
		return "", errors.New("missing JSON string")
	}
	start := p.offset - 1
	for p.offset < len(p.raw) {
		switch p.raw[p.offset] {
		case '"':
			p.offset++
			return p.raw[start:p.offset], nil
		case '\\':
			p.offset += 2
			if p.offset <= len(p.raw) && p.raw[p.offset-1] == 'u' {
				p.offset += 4
			}
		default:
			p.offset++
		}
	}
	return "", errors.New("unterminated JSON string")
}

func (p *runToolCanonicalJSONParser) skipWhitespace() {
	for p.offset < len(p.raw) {
		switch p.raw[p.offset] {
		case ' ', '\t', '\r', '\n':
			p.offset++
		default:
			return
		}
	}
}

func (p *runToolCanonicalJSONParser) consume(want byte) bool {
	if p.offset < len(p.raw) && p.raw[p.offset] == want {
		p.offset++
		return true
	}
	return false
}

func isRunToolJSONDelimiter(value byte) bool {
	switch value {
	case ' ', '\t', '\r', '\n', ',', ']', '}':
		return true
	default:
		return false
	}
}
