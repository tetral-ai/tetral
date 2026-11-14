.PHONY: build build-sandbox-helper install-sandbox-helper test-runner test test-affected test-full lint vulncheck integration-test run run-api run-bridge-api run-job-runner run-sandbox run-git-proxy run-cleanup run-event-stream run-computer-hub

SANDBOX_HELPER_OUT ?= bin/sandbox
SANDBOX_HELPER_INSTALL_PATH ?= /usr/local/bin/sandbox

build:
	go build ./...

build-sandbox-helper:
	CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o $(SANDBOX_HELPER_OUT) ./internal/sandbox/helper/cmd/sandbox

install-sandbox-helper: build-sandbox-helper
	install -D -m 0755 $(SANDBOX_HELPER_OUT) $(DESTDIR)$(SANDBOX_HELPER_INSTALL_PATH)

# The runner is built first and run as the recipe's own command, so the
# SIGTERM that make passes on reaches it; go run does not pass it on.
TEST_RUNNER := .test-results/bin/tetral-test

test-runner:
	go build -o $(TEST_RUNNER) ./internal/testinfra/cmd/tetral-test

test: test-runner
	$(TEST_RUNNER) --profile fast

test-affected: test-runner
	$(TEST_RUNNER) --profile affected

test-full: test-runner
	$(TEST_RUNNER) --profile full

lint:
	go vet ./...
	golangci-lint run ./...

# vulncheck is the full symbol-level vulnerability gate. It needs roughly
# 24 GiB of free memory (the sandbox service's kubernetes/AWS import closure
# dominates the analysis), so CI runs a split instead: symbol-level per tree
# except the sandbox closure, which a module-level scan covers as a
# detection superset. A module-level CI finding is triaged by running this
# target locally for the reachability verdict; prefer fixing by bumping the
# dependency.
vulncheck:
	go tool govulncheck ./...
	python3 scripts/check-envoy-secret-helper.py

# run is an alias for the public API workload.
run: run-api

run-api:
	go run ./services/api/cmd/tetral-api

run-bridge-api:
	go run ./services/bridge/cmd/bridge-api

run-job-runner:
	go run ./services/job-runner/cmd/job-runner

run-sandbox:
	go run ./services/sandbox/cmd/tetral-sandbox

run-git-proxy:
	go run ./services/git-proxy/cmd/git-proxy

run-cleanup:
	go run ./services/cleanup/cmd/tetral-cleanup

run-event-stream:
	go run ./services/event-stream/cmd/event-stream

run-computer-hub:
	go run ./services/computer-hub/cmd/tetral-computer-hub

run-auth:
	go run ./services/auth/cmd/tetral-auth

run-queue:
	go run ./services/queue/cmd/tetral-queue

run-web-connector:
	go run ./services/web-connector/cmd/web-connector

# integration-test runs the Engine integration package directly.
integration-test:
	go test -race ./integration
