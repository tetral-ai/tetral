package joint

import (
	"fmt"
	"os/exec"
	"path/filepath"
)

// ServicePackages are the Engine binaries the tests start, as packages of
// the repository.
var ServicePackages = []string{
	"./cmd/tetral-db-prepare",
	"./cmd/tetral-bootstrap",
	"./services/auth/cmd/tetral-auth",
	"./services/api/cmd/tetral-api",
	"./services/computer-hub/cmd/tetral-computer-hub",
	"./services/git-proxy/cmd/git-proxy",
	"./services/bridge/cmd/bridge-api",
	"./services/cleanup/cmd/tetral-cleanup",
}

// BuildServices builds ServicePackages from the repository at root into
// dir.
func BuildServices(root, dir string) error {
	command := exec.Command("go", append([]string{"build", "-o", dir + string(filepath.Separator)}, ServicePackages...)...)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("build the Engine services: %w: %s", err, output)
	}
	return nil
}
