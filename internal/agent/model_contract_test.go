package agent

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/tetral-ai/tetral/api"
)

// The OpenAPI file states the model ids a client may name and the ids the
// catalog answers; both are the ids Engine admits.
func TestTheFileStatesTheSupportedModels(t *testing.T) {
	data, err := api.Files.ReadFile(api.File)
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Components struct {
			Schemas struct {
				Named struct {
					Enum []string `json:"enum"`
				} `json:"EngineAgentModelID"`
				Listed struct {
					Properties struct {
						ID struct {
							Enum []string `json:"enum"`
						} `json:"id"`
					} `json:"properties"`
				} `json:"BetaModelInfo"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	supported := supportedModelIDs()
	want := slices.Sorted(slices.Values(supported[:]))
	for name, stated := range map[string][]string{"EngineAgentModelID": file.Components.Schemas.Named.Enum, "BetaModelInfo.id": file.Components.Schemas.Listed.Properties.ID.Enum} {
		if got := slices.Sorted(slices.Values(stated)); !slices.Equal(got, want) {
			t.Errorf("the file's %s = %v; Engine admits %v", name, got, want)
		}
	}
}
