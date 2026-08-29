package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tetral-ai/tetral/internal/testinfra"
)

func main() {
	artifacts := flag.String("artifacts", "", "downloaded result artifact root")
	producers := flag.String("producers", "", "comma-separated expected producer identities; defaults to the PR inventory")
	output := flag.String("output", "gate-result.json", "gate verdict output")
	flag.Parse()
	expectedProducers := split(*producers)
	if len(expectedProducers) == 0 {
		var err error
		expectedProducers, err = testinfra.LoadPRProducers()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	expectation := testinfra.GateExpectation{
		Repository: os.Getenv("TETRAL_CI_REPOSITORY"), EventHeadSHA: os.Getenv("TETRAL_CI_EVENT_HEAD_SHA"),
		EventBaseSHA: os.Getenv("TETRAL_CI_EVENT_BASE_SHA"), TestMergeSHA: os.Getenv("TETRAL_CI_TEST_MERGE_SHA"),
		RequiredCheckSHA: os.Getenv("TETRAL_CI_REQUIRED_CHECK_SHA"), WorkflowSourceSHA: os.Getenv("TETRAL_CI_WORKFLOW_SOURCE_SHA"),
		Workflow: os.Getenv("TETRAL_CI_WORKFLOW"), RunID: os.Getenv("TETRAL_CI_RUN_ID"), RunAttempt: os.Getenv("TETRAL_CI_RUN_ATTEMPT"),
		Producers: expectedProducers, ProducerJobs: testinfra.PRProducerJobs(),
	}
	if err := testinfra.VerifyUpstreamNeeds(os.Getenv("TETRAL_CI_NEEDS"), testinfra.PRUpstreamJobs()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	pullRequest, err := strconv.Atoi(os.Getenv("TETRAL_CI_PULL_REQUEST"))
	if err != nil || pullRequest <= 0 {
		fmt.Fprintln(os.Stderr, "merge Gate pull request identity is malformed")
		os.Exit(1)
	}
	runID, _ := strconv.ParseInt(expectation.RunID, 10, 64)
	runAttempt, _ := strconv.Atoi(expectation.RunAttempt)
	liveFacts, err := testinfra.ReadLiveGateFacts(context.Background(), expectation.Repository, pullRequest, runID, runAttempt)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := testinfra.VerifyLiveGateFacts(expectation, liveFacts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	verdict, err := testinfra.VerifyMergeGate(*artifacts, expectation)
	body, marshalErr := json.MarshalIndent(verdict, "", "  ")
	if marshalErr == nil {
		marshalErr = os.MkdirAll(filepath.Dir(*output), 0o700)
	}
	if marshalErr == nil {
		marshalErr = os.WriteFile(*output, append(body, '\n'), 0o600)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	if marshalErr != nil {
		fmt.Fprintln(os.Stderr, marshalErr)
	}
	if err != nil || marshalErr != nil {
		os.Exit(1)
	}
}

func split(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}
