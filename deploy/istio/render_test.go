package istio_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestLockedIstiodRender(t *testing.T) {
	for _, tool := range []string{"helm", "python3"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("mandatory render prerequisite %s: %v", tool, err)
		}
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "istio.yaml")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "python3", filepath.Join(dir, "render.py"), "--charts-dir", filepath.Join(dir, "charts"), "--output", output, "--trust-domain", "transport.example")
	if data, err := command.CombinedOutput(); err != nil {
		t.Fatalf("actual locked chart render: %v %s", err, data)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	found, meshFound := false, false
	crds := 0
	for {
		var obj map[string]any
		if err := decoder.Decode(&obj); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if obj == nil {
			continue
		}
		if obj["kind"] == "CustomResourceDefinition" {
			crds++
		}
		if obj["kind"] == "ConfigMap" && obj["metadata"].(map[string]any)["name"] == "istio-1-31-1" {
			// Sidecar access logs would add a line per routed RPC, including
			// Queue polling and heartbeats; the mesh leaves them disabled.
			var mesh map[string]any
			if err := yaml.Unmarshal([]byte(obj["data"].(map[string]any)["mesh"].(string)), &mesh); err != nil {
				t.Fatal(err)
			}
			if file, present := mesh["accessLogFile"]; present && file != "" {
				t.Fatalf("mesh-wide proxy access logging enabled: %v", file)
			}
			meshFound = true
		}
		if obj["kind"] != "Deployment" {
			continue
		}
		meta := obj["metadata"].(map[string]any)
		if meta["name"] != "istiod-1-31-1" {
			continue
		}
		found = true
		spec := obj["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		container := spec["containers"].([]any)[0].(map[string]any)
		if !strings.Contains(container["image"].(string), "istio/pilot:1.31.1@sha256:") {
			t.Fatal("unlocked discovery image")
		}
		mandatory := false
		for _, entry := range spec["volumes"].([]any) {
			volume := entry.(map[string]any)
			if volume["name"] != "cacerts" {
				continue
			}
			secret := volume["secret"].(map[string]any)
			if secret["optional"] != false {
				t.Fatal("Istiod issuer material remains optional")
			}
			keys := map[string]string{}
			for _, entry := range secret["items"].([]any) {
				item := entry.(map[string]any)
				keys[item["key"].(string)] = item["path"].(string)
			}
			want := map[string]string{"ca-cert.pem": "ca-cert.pem", "ca-key.pem": "ca-key.pem", "root-cert.pem": "root-cert.pem", "cert-chain.pem": "cert-chain.pem"}
			if !reflect.DeepEqual(keys, want) {
				t.Fatalf("issuer inventory %v", keys)
			}
			mandatory = true
		}
		if !mandatory {
			t.Fatal("missing required issuer Secret mount")
		}
	}
	if !found || !meshFound || crds != 15 {
		t.Fatalf("incomplete actual charts: discovery=%v mesh=%v CRDs=%d", found, meshFound, crds)
	}
	if !bytes.Contains(data, []byte("trustDomain: transport.example")) {
		t.Fatal("operator trust domain not projected")
	}
	corrupt := t.TempDir()
	for _, name := range []string{"base-1.31.1.tgz", "istiod-1.31.1.tgz"} {
		archive, err := os.ReadFile(filepath.Join(dir, "charts", name))
		if err != nil {
			t.Fatal(err)
		}
		if name == "istiod-1.31.1.tgz" {
			archive = append(archive, 0)
		}
		// #nosec G703 -- name is one of the two literal locked archives in this test-owned directory.
		if err := os.WriteFile(filepath.Join(corrupt, name), archive, 0600); err != nil {
			t.Fatal(err)
		}
	}
	bad := exec.CommandContext(ctx, "python3", filepath.Join(dir, "render.py"), "--charts-dir", corrupt, "--output", filepath.Join(t.TempDir(), "denied.yaml"))
	failure, err := bad.CombinedOutput()
	if err == nil || !bytes.Contains(failure, []byte("archive digest differs for istiod")) {
		t.Fatalf("modified archive accepted: %v %s", err, failure)
	}
}
