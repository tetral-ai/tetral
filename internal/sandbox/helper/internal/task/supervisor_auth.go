package task

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

const supervisorAuthFDEnv = "TETRAL_SUPERVISOR_AUTH_FD"
const supervisorAuthMarker = "tetral-supervisor-reexec-v2\n"

const supervisorAuthSeals = unix.F_SEAL_SEAL | unix.F_SEAL_SHRINK | unix.F_SEAL_GROW | unix.F_SEAL_WRITE

var supervisorAuthState struct {
	sync.Mutex
	file *os.File
}

// InitializeSupervisorEntrypointAuth creates the root-owned capability before
// the helper drops to the model's runtime identity.
func InitializeSupervisorEntrypointAuth() error {
	supervisorAuthState.Lock()
	defer supervisorAuthState.Unlock()
	if supervisorAuthState.file != nil {
		return nil
	}
	if os.Geteuid() != 0 {
		return errors.New("supervisor authorization requires the helper identity")
	}
	fd, err := unix.MemfdCreate("tetral-supervisor-auth", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), "supervisor-auth")
	if file == nil {
		_ = unix.Close(fd)
		return errors.New("supervisor authorization descriptor is unavailable")
	}
	failed := true
	defer func() {
		if failed {
			_ = file.Close()
		}
	}()
	if _, err := io.WriteString(file, supervisorAuthMarker); err != nil {
		return err
	}
	if err := unix.Fchmod(fd, 0o400); err != nil {
		return err
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, supervisorAuthSeals); err != nil {
		return err
	}
	supervisorAuthState.file = file
	failed = false
	return nil
}

func attachSupervisorEntrypointAuth(cmd *exec.Cmd) (func(), error) {
	supervisorAuthState.Lock()
	defer supervisorAuthState.Unlock()
	if supervisorAuthState.file == nil {
		return nil, errors.New("supervisor authorization capability is unavailable")
	}
	if _, err := supervisorAuthState.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	cmd.ExtraFiles = append(cmd.ExtraFiles, supervisorAuthState.file)
	fd := 3 + len(cmd.ExtraFiles) - 1
	cmd.Env = append(environmentWithout(os.Environ(), supervisorAuthFDEnv), fmt.Sprintf("%s=%d", supervisorAuthFDEnv, fd))
	return func() {}, nil
}

func SupervisorEntrypointAuthorized() bool {
	rawFD := os.Getenv(supervisorAuthFDEnv)
	_ = os.Unsetenv(supervisorAuthFDEnv)
	if rawFD == "" {
		return false
	}
	fd, err := strconv.Atoi(rawFD)
	if err != nil || fd < 3 {
		return false
	}
	file := os.NewFile(uintptr(fd), "supervisor-auth")
	if file == nil {
		return false
	}
	authorized := false
	defer func() {
		if !authorized {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 {
		return false
	}
	seals, err := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
	if err != nil || seals&supervisorAuthSeals != supervisorAuthSeals {
		return false
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(len(supervisorAuthMarker)+1)))
	if err != nil {
		return false
	}
	if len(data) != len(supervisorAuthMarker) || subtle.ConstantTimeCompare(data, []byte(supervisorAuthMarker)) != 1 {
		return false
	}

	// The bootstrap must forward the root launcher's already-created memfd.
	// Adopting this descriptor, instead of closing it and calling the root-only
	// initializer again, preserves one capability across both dropped re-execs.
	unix.CloseOnExec(fd)
	supervisorAuthState.Lock()
	defer supervisorAuthState.Unlock()
	if supervisorAuthState.file != nil {
		return false
	}
	supervisorAuthState.file = file
	authorized = true
	return true
}

// ConsumeSupervisorEntrypointAuth closes the capability only after the final
// supervisor entrypoint has validated it. The detached child never inherits
// supervisor-bootstrap authority.
func ConsumeSupervisorEntrypointAuth() {
	supervisorAuthState.Lock()
	defer supervisorAuthState.Unlock()
	if supervisorAuthState.file != nil {
		_ = supervisorAuthState.file.Close()
		supervisorAuthState.file = nil
	}
}

func environmentWithout(environment []string, key string) []string {
	prefix := key + "="
	filtered := make([]string, 0, len(environment))
	for _, value := range environment {
		if !strings.HasPrefix(value, prefix) {
			filtered = append(filtered, value)
		}
	}
	return filtered
}
