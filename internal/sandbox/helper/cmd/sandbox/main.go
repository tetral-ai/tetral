package main

import (
	"context"
	"os"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/cli"
)

func main() {
	os.Exit(cli.Main(context.Background(), os.Args[1:], os.Stdout))
}
