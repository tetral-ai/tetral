package dbconnect

import (
	"errors"
	"strings"
)

var errInvalidOperationLabel = errors.New("dbconnect: invalid operation label")

func validateOperationLabel(operation string) error {
	if operation == "" {
		return errInvalidOperationLabel
	}
	parts := strings.Split(operation, ".")
	if len(parts) < 2 {
		return errInvalidOperationLabel
	}
	for _, part := range parts {
		if part == "" {
			return errInvalidOperationLabel
		}
		for _, prefix := range resourceIDPrefixes {
			if strings.HasPrefix(part, prefix) {
				return errInvalidOperationLabel
			}
		}
		if containsResourceIDShape(part) {
			return errInvalidOperationLabel
		}
		for _, r := range part {
			if r >= 'a' && r <= 'z' {
				continue
			}
			if r >= '0' && r <= '9' {
				continue
			}
			if r == '_' {
				continue
			}
			return errInvalidOperationLabel
		}
	}
	return nil
}

func containsResourceIDShape(part string) bool {
	for index := range part {
		for _, prefix := range resourceIDPrefixes {
			if !strings.HasPrefix(part[index:], prefix) {
				continue
			}
			suffixStart := index + len(prefix)
			suffixEnd := suffixStart + resourceIDHexLength
			if suffixEnd <= len(part) && isLowercaseHex(part[suffixStart:suffixEnd]) {
				return true
			}
		}
	}
	return false
}

func isLowercaseHex(value string) bool {
	for _, r := range value {
		if r >= '0' && r <= '9' {
			continue
		}
		if r >= 'a' && r <= 'f' {
			continue
		}
		return false
	}
	return true
}

const resourceIDHexLength = 16

var resourceIDPrefixes = []string{
	"workspace_",
	"agent_",
	"agv_",
	"skill_",
	"skill_version_",
	"skv_",
	"env_",
	"vlt_",
	"cred_",
	"ak_",
	"req_",
	"sesn_",
	"sesrsc_",
	"run_",
	"sevt_",
	"sub_",
	"file_",
	"fobj_",
	"memstore_",
	"mem_",
	"memver_",
}
