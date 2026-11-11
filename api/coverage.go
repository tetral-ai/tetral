package api

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// CoverageFileEnv names the file a test's developer client appends each
// Checked line to. The test runner sets it.
const CoverageFileEnv = "TETRAL_API_COVERAGE_FILE"

// Checked is one thing a developer client held to the file: an answer of
// Status to Operation, with its error type when it is an error answer, or a
// stream event of Operation.
type Checked struct {
	Operation string `json:"operation"`
	Status    string `json:"status"`
	Error     string `json:"error"`
	Event     string `json:"event"`
}

// The three kinds of thing Engine's full suite must check.
const (
	kindOperation = "operation"
	kindError     = "error type"
	kindEvent     = "stream event"
)

// kind is what a Checked line reaches: a success answer its operation, an
// error answer its error type, a stream event its event name.
func (c Checked) kind() (kind, name string) {
	switch {
	case c.Event != "":
		return kindEvent, c.Event
	case c.Error != "":
		return kindError, c.Error
	case strings.HasPrefix(c.Status, "2"):
		return kindOperation, c.Operation
	}
	return "", ""
}

// Required lists what Engine's full suite must check: a success answer of
// every operation, every declared error type on some operation and every
// declared stream event on some stream, less the list of the unreachable.
func Required() ([]Checked, error) {
	declared, unreachable, err := read()
	if err != nil {
		return nil, err
	}
	var required []Checked
	for kind, names := range declared {
		for _, name := range slices.Sorted(maps.Keys(names)) {
			if _, listed := unreachable[kind][name]; listed {
				continue
			}
			switch kind {
			case kindOperation:
				required = append(required, Checked{Operation: name, Status: "200"})
			case kindError:
				required = append(required, Checked{Status: "400", Error: name})
			case kindEvent:
				required = append(required, Checked{Event: name})
			}
		}
	}
	return required, nil
}

// Unreached compares what Engine's full suite checked with the file. It
// lists everything Required that was not checked, and every entry of the
// list of the unreachable that was checked after all, names nothing the file
// declares or gives no reason.
func Unreached(checked []Checked) ([]string, error) {
	declared, unreachable, err := read()
	if err != nil {
		return nil, err
	}
	return unreached(declared, unreachable, checked), nil
}

// names maps each kind to its names, with a value per name.
type names[V any] map[string]map[string]V

func read() (names[bool], names[string], error) {
	sources := map[string][]byte{}
	for _, name := range []string{File, Unreachable} {
		data, err := Files.ReadFile(name)
		if err != nil {
			return nil, nil, err
		}
		sources[name] = data
	}
	return parse(sources[File], sources[Unreachable])
}

func parse(openAPI, list []byte) (names[bool], names[string], error) {
	var file struct {
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
			Responses   map[string]struct {
				ErrorTypes []string `json:"x-error-types"`
				Content    map[string]struct {
					Events map[string]json.RawMessage `json:"x-events"`
				} `json:"content"`
			} `json:"responses"`
		} `json:"paths"`
	}
	var listed struct {
		Operations, Errors, Events map[string]string
	}
	if err := json.Unmarshal(openAPI, &file); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", File, err)
	}
	if err := json.Unmarshal(list, &listed); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", Unreachable, err)
	}
	declared := names[bool]{kindOperation: {}, kindError: {}, kindEvent: {}}
	for _, methods := range file.Paths {
		for _, operation := range methods {
			declared[kindOperation][operation.OperationID] = true
			for _, answer := range operation.Responses {
				for _, errorType := range answer.ErrorTypes {
					declared[kindError][errorType] = true
				}
				for _, content := range answer.Content {
					for event := range content.Events {
						declared[kindEvent][event] = true
					}
				}
			}
		}
	}
	return declared, names[string]{kindOperation: listed.Operations, kindError: listed.Errors, kindEvent: listed.Events}, nil
}

func unreached(declared names[bool], unreachable names[string], checked []Checked) []string {
	reached := names[bool]{kindOperation: {}, kindError: {}, kindEvent: {}}
	for _, line := range checked {
		if kind, name := line.kind(); kind != "" {
			reached[kind][name] = true
		}
	}
	missing := map[string]string{kindOperation: "a success answer of it", kindError: "an answer with it", kindEvent: "it on a stream"}
	var lines []string
	for kind := range declared {
		for _, name := range slices.Sorted(maps.Keys(declared[kind])) {
			if _, listed := unreachable[kind][name]; !listed && !reached[kind][name] {
				lines = append(lines, fmt.Sprintf("%s %s: no test checked %s", kind, name, missing[kind]))
			}
		}
		for _, name := range slices.Sorted(maps.Keys(unreachable[kind])) {
			switch {
			case !declared[kind][name]:
				lines = append(lines, fmt.Sprintf("%s %s: %s lists it and the file does not declare it", kind, name, Unreachable))
			case reached[kind][name]:
				lines = append(lines, fmt.Sprintf("%s %s: %s lists it and a test checked it", kind, name, Unreachable))
			case strings.TrimSpace(unreachable[kind][name]) == "":
				lines = append(lines, fmt.Sprintf("%s %s: %s gives no reason", kind, name, Unreachable))
			}
		}
	}
	slices.Sort(lines)
	return lines
}
