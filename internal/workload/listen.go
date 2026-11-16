package workload

import (
	"net"
	"strconv"
)

// ListenTCP listens on address only when it is a well-formed listen address:
// an empty host, an IP or a DNS name, and a decimal port. Anything else is
// refused with a ConfigError that quotes nothing of it, because the listen
// error of a malformed value repeats the value, which may hold a secret
// pasted into the wrong setting. A well-formed address keeps its listen error.
func ListenTCP(listen func(network string, address string) (net.Listener, error), address string) (net.Listener, error) {
	if !wellFormedListenAddress(address) {
		return nil, NewConfigError("the listen address is not a host and a decimal port")
	}
	if listen == nil {
		listen = net.Listen
	}
	return listen("tcp", address)
}

func wellFormedListenAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return false
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	if number, err := strconv.Atoi(port); err != nil || number > 65535 {
		return false
	}
	if host == "" || net.ParseIP(host) != nil {
		return true
	}
	if len(host) > 253 {
		return false
	}
	for _, char := range host {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' && char != '.' {
			return false
		}
	}
	return true
}
