// Package main is the thin command entrypoint for api.
package main

import (
	"context"
	"os"

	"github.com/tetral-ai/tetral/internal/workload"
	tetralapi "github.com/tetral-ai/tetral/services/api"
)

var runWorkload = workload.Run

func main() {
	if err := run(context.Background(), osEnv{}, tetralapi.BuildProductionApplication); err != nil {
		os.Exit(1)
	}
}

type envReader interface {
	Getenv(string) string
}

type osEnv struct{}

func (osEnv) Getenv(key string) string { return os.Getenv(key) }

type buildApplicationFunc = tetralapi.BuildApplicationFunc

func run(ctx context.Context, env envReader, buildApplication buildApplicationFunc) error {
	return tetralapi.Run(ctx, env, os.Stderr, buildApplication, runWorkload)
}
