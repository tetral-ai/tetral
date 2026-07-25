package agent

import (
	"reflect"
	"testing"
)

func TestSupportedModelSetMatchesCurrentStageContract(t *testing.T) {
	want := []string{
		"openai/gpt-5.5",
		"openai/gpt-5.6-sol",
		"anthropic/claude-opus-4-8",
		"anthropic/claude-fable-5",
		"deepseek/deepseek-v4-pro",
		"moonshotai/kimi-k3",
		"zai/glm-5.2",
	}
	got := supportedModelIDs()
	if !reflect.DeepEqual(got[:], want) {
		t.Fatalf("supportedModelIDs = %#v; want %#v", got, want)
	}
}
