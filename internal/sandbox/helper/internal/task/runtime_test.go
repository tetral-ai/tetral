package task

import (
	"os"
	"testing"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/runtimepath"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/testfixture"
)

func TestMain(m *testing.M) { os.Exit(testfixture.Run(m.Run)) }

func setRuntimeRoot(root string) {
	runtimepath.SetForTesting(root)
}
