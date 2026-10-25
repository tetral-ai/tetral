// Command tetral-reference runs the reference control server. It reads its
// startup input, one JSON object, from stdin, prints the two bound
// addresses as one JSON object to stdout, and serves until it is signalled.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/tetral-ai/tetral/cli/reference"
)

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("tetral-reference failed", "error", err.Error())
		os.Exit(1)
	}
}

func run() error {
	var config reference.Config
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return fmt.Errorf("read the startup input: %w", err)
	}
	server, err := reference.New(config)
	if err != nil {
		return err
	}
	listen := func(address string) (net.Listener, error) {
		if address == "" {
			address = reference.DefaultListen
		}
		return net.Listen("tcp", address)
	}
	protocol, err := listen(config.Listen)
	if err != nil {
		return err
	}
	control, err := listen(config.ControlListen)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(reference.Addresses{
		Listen: protocol.Addr().String(), ControlListen: control.Addr().String(),
	}); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	failed := make(chan error, 2)
	servers := []*http.Server{{Handler: server.Handler()}, {Handler: server.ControlHandler()}}
	for index, listener := range []net.Listener{protocol, control} {
		go func() { failed <- servers[index].Serve(listener) }()
	}
	select {
	case <-ctx.Done():
	case err := <-failed:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	for _, each := range servers {
		_ = each.Close()
	}
	return nil
}
