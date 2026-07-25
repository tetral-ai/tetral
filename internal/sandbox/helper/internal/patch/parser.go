// This file owns parsing the bounded apply_patch text format into internal
// operations; filesystem verification and mutation remain in apply.go.
package patch

import (
	"strings"
)

type opKind int

const (
	opAdd opKind = iota
	opDelete
	opUpdate
)

type op struct {
	kind   opKind
	path   string
	moveTo *string
	lines  []string
	chunks []chunk
}

type chunk struct {
	anchor   *string
	oldLines []string
	newLines []string
	isEOF    bool
}

func parsePatch(raw string) ([]op, error) {
	lines, err := preprocess(raw)
	if err != nil {
		return nil, err
	}
	if len(lines) == 0 || lines[0] != "*** Begin Patch" {
		return nil, parseError{line: 1, message: "patch missing begin marker"}
	}
	i := 1
	if i < len(lines) && strings.HasPrefix(lines[i], "*** Environment ID:") {
		if strings.TrimSpace(strings.TrimPrefix(lines[i], "*** Environment ID:")) == "" {
			return nil, parseError{line: i + 1, message: "environment id cannot be empty"}
		}
		i++
	}
	var ops []op
	for i < len(lines) {
		line := lines[i]
		if line == "*** End Patch" {
			i++
			for ; i < len(lines); i++ {
				if strings.TrimSpace(lines[i]) != "" {
					return nil, parseError{line: i + 1, message: "trailing content after end marker"}
				}
			}
			if len(ops) == 0 {
				return nil, parseError{line: i, message: "empty patch"}
			}
			return ops, nil
		}
		switch {
		case strings.HasPrefix(line, "*** Add File: "):
			pathValue := strings.TrimPrefix(line, "*** Add File: ")
			if strings.TrimSpace(pathValue) == "" {
				return nil, parseError{line: i + 1, message: "add file path is required"}
			}
			i++
			var body []string
			for i < len(lines) && !strings.HasPrefix(lines[i], "*** ") {
				if !strings.HasPrefix(lines[i], "+") {
					return nil, parseError{line: i + 1, message: "add file lines must start with +"}
				}
				body = append(body, lines[i][1:])
				i++
			}
			ops = append(ops, op{kind: opAdd, path: pathValue, lines: body})
		case strings.HasPrefix(line, "*** Delete File: "):
			pathValue := strings.TrimPrefix(line, "*** Delete File: ")
			if strings.TrimSpace(pathValue) == "" {
				return nil, parseError{line: i + 1, message: "delete file path is required"}
			}
			ops = append(ops, op{kind: opDelete, path: pathValue})
			i++
		case strings.HasPrefix(line, "*** Update File: "):
			parsed, next, err := parseUpdate(lines, i)
			if err != nil {
				return nil, err
			}
			ops = append(ops, parsed)
			i = next
		default:
			return nil, parseError{line: i + 1, message: "invalid hunk header; expected *** Add File: , *** Delete File: , or *** Update File: "}
		}
	}
	return nil, parseError{line: len(lines), message: "patch missing end marker"}
}

func parseUpdate(lines []string, i int) (op, int, error) {
	pathValue := strings.TrimPrefix(lines[i], "*** Update File: ")
	if strings.TrimSpace(pathValue) == "" {
		return op{}, i, parseError{line: i + 1, message: "update file path is required"}
	}
	i++
	var moveTo *string
	if i < len(lines) && strings.HasPrefix(lines[i], "*** Move to: ") {
		value := strings.TrimPrefix(lines[i], "*** Move to: ")
		if strings.TrimSpace(value) == "" {
			return op{}, i, parseError{line: i + 1, message: "move destination is required"}
		}
		moveTo = &value
		i++
	}
	var chunks []chunk
	var current *chunk
	closeCurrent := func() {
		if current != nil {
			chunks = append(chunks, *current)
			current = nil
		}
	}
	openCurrent := func() {
		if current == nil {
			current = &chunk{}
		}
	}
	for i < len(lines) {
		body := lines[i]
		switch {
		case body == "*** End of File":
			closeCurrent()
			if len(chunks) == 0 {
				chunks = append(chunks, chunk{})
			}
			chunks[len(chunks)-1].isEOF = true
			i++
			return finishUpdate(pathValue, moveTo, chunks, i)
		case strings.HasPrefix(body, "*** "):
			closeCurrent()
			return finishUpdate(pathValue, moveTo, chunks, i)
		case body == "@@":
			closeCurrent()
			current = &chunk{}
			i++
		case strings.HasPrefix(body, "@@ "):
			closeCurrent()
			anchor := body[3:]
			current = &chunk{anchor: &anchor}
			i++
		case strings.HasPrefix(body, " "):
			openCurrent()
			current.oldLines = append(current.oldLines, body[1:])
			current.newLines = append(current.newLines, body[1:])
			i++
		case strings.HasPrefix(body, "-"):
			openCurrent()
			current.oldLines = append(current.oldLines, body[1:])
			i++
		case strings.HasPrefix(body, "+"):
			openCurrent()
			current.newLines = append(current.newLines, body[1:])
			i++
		case body == "":
			openCurrent()
			current.oldLines = append(current.oldLines, "")
			current.newLines = append(current.newLines, "")
			i++
		default:
			return op{}, i, parseError{line: i + 1, message: "unexpected update line"}
		}
	}
	closeCurrent()
	return finishUpdate(pathValue, moveTo, chunks, i)
}

func finishUpdate(pathValue string, moveTo *string, chunks []chunk, next int) (op, int, error) {
	for _, chunk := range chunks {
		if len(chunk.oldLines) > 0 || len(chunk.newLines) > 0 {
			return op{kind: opUpdate, path: pathValue, moveTo: moveTo, chunks: chunks}, next, nil
		}
	}
	return op{}, next, parseError{line: next, message: "empty update"}
}

// preprocess normalizes the raw patch text before the parser sees any line: it
// trims the whole text; strips a heredoc wrapper only when the first line is
// exactly <<EOF, <<'EOF', or <<"EOF", the last line ends with EOF, and there
// are at least four lines (a mismatched quote style is left in place, not
// stripped); and strips exactly one trailing \r per line (a bare \r inside a
// line is content). parsePatch accepts and discards one non-empty environment
// id immediately after the begin marker.
func preprocess(raw string) ([]string, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return nil, parseError{line: 1, message: "empty patch"}
	}
	lines := splitPatchLines(text)
	if len(lines) >= 4 && isHeredocStart(lines[0]) && strings.HasSuffix(lines[len(lines)-1], "EOF") {
		lines = lines[1 : len(lines)-1]
	}
	return lines, nil
}

func splitPatchLines(text string) []string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSuffix(line, "\r")
	}
	return lines
}

func isHeredocStart(line string) bool {
	return line == "<<EOF" || line == "<<'EOF'" || line == `<<"EOF"`
}

type parseError struct {
	line    int
	message string
}

func (e parseError) Error() string {
	return e.message
}
