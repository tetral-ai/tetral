package main

import (
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/cli/reference"
)

// runMainVariable makes the test binary run main instead of its tests, so
// that a test starts tetral-reference as a process of its own.
const runMainVariable = "TETRAL_REFERENCE_RUN_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(runMainVariable) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestBothListenersAreOnLoopbackWhenTheInputNamesNoAddress(t *testing.T) {
	command := exec.Command(os.Args[0])
	command.Env = append(os.Environ(), runMainVariable+"=1")
	command.Stdin = strings.NewReader("{}")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	var addresses reference.Addresses
	if err := json.NewDecoder(stdout).Decode(&addresses); err != nil {
		t.Fatal(err)
	}
	for name, address := range map[string]string{"listen": addresses.Listen, "control_listen": addresses.ControlListen} {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host != "127.0.0.1" || port == "0" {
			t.Errorf("%s is %q; want a bound port on 127.0.0.1", name, address)
		}
	}
}
