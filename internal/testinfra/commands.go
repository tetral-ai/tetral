package testinfra

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// runCommand runs every external command of the runner and its fixtures.
// Standard output goes to stdout, or with standard error to the failure tail
// when stdout is nil. A failure names the program and its subcommand, not the
// other arguments, which can hold a fixture's credentials, and ends with the
// last lines the command wrote to standard error. The tail is a file, not a
// pipe, so Wait still returns when the command exits while a descendant, such
// as the node process that bun x starts, holds it open.
func runCommand(command *exec.Cmd, stdout io.Writer) error {
	tail, err := os.CreateTemp("", "tetral-command-output-")
	if err != nil {
		return err
	}
	defer func() {
		_ = tail.Close()
		_ = os.Remove(tail.Name())
	}()
	command.Stdout, command.Stderr = tail, tail
	if stdout != nil {
		command.Stdout = stdout
	}
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s: %w%s", commandName(command), err, commandOutputTail(tail))
	}
	return nil
}

// commandStdout runs command as runCommand does and returns its standard
// output.
func commandStdout(command *exec.Cmd) ([]byte, error) {
	var stdout bytes.Buffer
	err := runCommand(command, &stdout)
	return stdout.Bytes(), err
}

func commandName(command *exec.Cmd) string {
	name := filepath.Base(command.Args[0])
	if len(command.Args) > 1 && !strings.HasPrefix(command.Args[1], "-") {
		name += " " + command.Args[1]
	}
	return name
}
