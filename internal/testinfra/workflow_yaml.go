package testinfra

import (
	"fmt"
	"reflect"

	"gopkg.in/yaml.v3"
)

// DecodeWorkflowYAML decodes a repository-owned workflow or composite action
// through the single YAML dependency boundary. Callers own their narrow
// structural projection and must not use this as a general configuration API.
func DecodeWorkflowYAML(body []byte, source string, destination any) error {
	if destination == nil || reflect.ValueOf(destination).Kind() != reflect.Pointer || reflect.ValueOf(destination).IsNil() {
		return fmt.Errorf("workflow %s decode destination must be a non-nil pointer", source)
	}
	if err := yaml.Unmarshal(body, destination); err != nil {
		return fmt.Errorf("parse workflow %s: %w", source, err)
	}
	return nil
}

type workflowYAMLNode = yaml.Node

const (
	workflowYAMLMapping  = yaml.MappingNode
	workflowYAMLSequence = yaml.SequenceNode
	workflowYAMLScalar   = yaml.ScalarNode
)

// parseWorkflowYAML is the single YAML dependency boundary for repository CI
// workflows and composite actions. Consumers inspect the parsed structure and
// never infer policy from text occurrence counts.
func parseWorkflowYAML(body []byte, source string) (*workflowYAMLNode, error) {
	var document workflowYAMLNode
	if err := DecodeWorkflowYAML(body, source, &document); err != nil {
		return nil, err
	}
	return &document, nil
}
