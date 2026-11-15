package testinfra

import "testing"

func TestWorkflowReferencesSecretsReadsTheSecretsContext(t *testing.T) {
	for _, row := range []struct {
		name, step string
		reads      bool
	}{
		{"a property of secrets", `run: echo "${{ secrets.TOKEN }}"`, true},
		{"an index into secrets", `env: {TOKEN: "${{ secrets['TOKEN'] }}"}`, true},
		{"secrets as a whole", `run: echo '${{ toJSON(secrets) }}'`, true},
		{"secrets in an if without braces", `if: secrets.TOKEN != ''`, true},
		{"the GitHub token only", `env: {GH_TOKEN: "${{ github.token }}"}`, false},
		{"the word outside an expression, as a property, in a string", `run: echo secrets.txt "${{ github.event.inputs.secrets }}" "${{ format('secrets') }}"`, false},
	} {
		workflow := "on: workflow_dispatch\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - " + row.step + "\n"
		reads, err := WorkflowReferencesSecrets([]byte(workflow), row.name)
		if err != nil {
			t.Fatal(err)
		}
		if reads != row.reads {
			t.Errorf("%s: reads secrets %t; want %t", row.name, reads, row.reads)
		}
	}
	called := "on: workflow_dispatch\njobs:\n  call:\n    uses: ./.github/workflows/other.yml\n    secrets: inherit\n"
	if reads, err := WorkflowReferencesSecrets([]byte(called), "called"); err != nil || !reads {
		t.Errorf("a job passing secrets to a called workflow: reads secrets %t, %v; want true", reads, err)
	}
}
