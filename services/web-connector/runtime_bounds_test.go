package webconnector

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
)

// The Runtime checks a web call against the same bounds before it sends it,
// so the connector refuses nothing the Runtime let through. Both sides are
// compared as their code evaluates them.
func TestRequestBoundsMatchTheRuntime(t *testing.T) {
	command := exec.Command("bun", "-e", `
import { MaxWebRequestGrpcMessageBytes, MaxWebResponseGrpcMessageBytes } from "./packages/runtime-pod/src/bounds.ts";
import { WEB_DOMAIN_MAX_BYTES, WEB_OPERATIONS_MAX, WEB_SEARCH_DOMAINS_MAX } from "./packages/runtime-pod/src/tool-runner.ts";
import { MaxTextBytes } from "@tetral/gateway-protocol/src/bounds.js";
console.log(JSON.stringify({
  requestMessageBytes: MaxWebRequestGrpcMessageBytes, responseMessageBytes: MaxWebResponseGrpcMessageBytes,
  operations: WEB_OPERATIONS_MAX, searchDomains: WEB_SEARCH_DOMAINS_MAX, textBytes: MaxTextBytes, domainBytes: WEB_DOMAIN_MAX_BYTES,
}));`)
	command.Dir = filepath.Join("..", "agent-runtime")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("evaluate the Runtime's web bounds: %v", err)
	}
	var runtime map[string]int
	if err := json.Unmarshal(output, &runtime); err != nil {
		t.Fatalf("decode the Runtime's web bounds %q: %v", output, err)
	}
	for name, bound := range map[string]int{
		"requestMessageBytes":  maxRunWebRequestGRPCMessageBytes,
		"responseMessageBytes": maxRunWebResponseGRPCMessageBytes,
		"operations":           maxOperations,
		"searchDomains":        maxSearchDomains,
		"textBytes":            maxRequestTextBytes,
		"domainBytes":          maxDomainBytes,
	} {
		if runtime[name] != bound {
			t.Errorf("%s: the connector accepts %d and the Runtime sends up to %d", name, bound, runtime[name])
		}
	}
}
