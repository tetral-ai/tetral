//go:build linux

package testinfra

import (
	"os"
	"strconv"
	"strings"
)

func currentProcessIdentity() (int, string) {
	pid := os.Getpid()
	return pid, processStartIdentity(pid)
}

func processIdentityAlive(pid int, started string) bool {
	return started != "" && processStartIdentity(pid) == started
}

func processStartIdentity(pid int) string {
	body, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	closeParen := strings.LastIndexByte(string(body), ')')
	if closeParen < 0 {
		return ""
	}
	fields := strings.Fields(string(body[closeParen+1:]))
	if len(fields) <= 19 {
		return ""
	}
	return fields[19]
}
