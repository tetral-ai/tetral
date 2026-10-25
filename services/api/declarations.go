package tetralapi

import (
	_ "embed"

	"github.com/tetral-ai/tetral/internal/workload"
)

//go:embed observability.json
var observabilityDeclarations []byte

func init() { workload.RegisterDeclarations("api", observabilityDeclarations) }
