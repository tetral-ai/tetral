package testinfra

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

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
	if err := yaml.Unmarshal(body, &document); err != nil {
		return nil, fmt.Errorf("parse workflow %s: %w", source, err)
	}
	return &document, nil
}
