package cli

import (
	"os"
	"testing"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/runtimepath"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/testfixture"
)

var cliSuiteRuntimeRoot string

func TestMain(m *testing.M) {
	os.Exit(testfixture.Run(func() int {
		cliSuiteRuntimeRoot = runtimepath.Root()
		return m.Run()
	}))
}
