// Command tetral-ci-flow makes the decisions of package ciflow inside a
// workflow: decide prints which products' jobs a change runs, needs applies
// a gate's rule to the results of the jobs it needs, and duration fails a
// run that reached the limit.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/testinfra/ciflow"
)

func main() {
	if len(os.Args) < 2 {
		fail(2, "usage: tetral-ci-flow decide|needs|duration [flags]")
	}
	flags := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	switch os.Args[1] {
	case "decide":
		event := flags.String("event", "", "the event that started the workflow")
		base := flags.String("base", "", "the pull request's base commit, or the commit before a push")
		head := flags.String("head", "", "the pull request's head commit, or the commit after a push")
		_ = flags.Parse(os.Args[2:])
		paths, known := ciflow.ChangedPaths(".", *event, *base, *head)
		products := ciflow.ProductsFor(paths, known)
		if known {
			fmt.Fprintf(os.Stderr, "%d changed paths\n", len(paths))
		} else {
			fmt.Fprintln(os.Stderr, "the changed paths are not known")
		}
		// The lines are a job's outputs in the form GITHUB_OUTPUT takes.
		fmt.Printf("engine=%t\ncli=%t\n", products.Engine, products.CLI)
	case "needs":
		deciding := flags.String("deciding", "decide", "the identifier of the deciding job")
		product := flags.String("product", "", "the deciding job's output that selects this gate's jobs")
		_ = flags.Parse(os.Args[2:])
		needs, err := ciflow.ParseNeeds(os.Getenv("TETRAL_CI_NEEDS"), *deciding, *product)
		if err != nil {
			fail(1, err.Error())
		}
		if failures := needs.Failures(); len(failures) > 0 {
			for _, failure := range failures {
				fmt.Fprintln(os.Stderr, failure)
			}
			os.Exit(1)
		}
		fmt.Printf("%s selected: %t; %d needed jobs allow the gate to pass\n", *product, needs.Selected, len(needs.Jobs))
	case "duration":
		gate := flags.String("gate", "", "the gate job's name, which the run's time leaves out")
		_ = flags.Parse(os.Args[2:])
		attempt, err := strconv.Atoi(os.Getenv("TETRAL_CI_RUN_ATTEMPT"))
		if err != nil {
			fail(1, "the run attempt is not a number")
		}
		jobs, err := listJobs(attempt, *gate)
		if err != nil {
			fail(1, err.Error())
		}
		// A job this attempt did not run again is listed with this attempt's
		// creation time; the attempt that ran it holds the first.
		var earlier [][]ciflow.JobTimes
		if slices.ContainsFunc(jobs, ciflow.JobTimes.RanEarlier) {
			for before := attempt - 1; before >= 1; before-- {
				listed, err := listJobs(before, *gate)
				if err != nil {
					fail(1, err.Error())
				}
				earlier = append(earlier, listed)
			}
		}
		if jobs, err = ciflow.WithFirstCreations(jobs, earlier...); err != nil {
			fail(1, err.Error())
		}
		lines, err := ciflow.DurationReport(jobs, time.Now())
		for _, line := range lines {
			fmt.Println(line)
		}
		if err != nil {
			fail(1, err.Error())
		}
	default:
		fail(2, "unknown command "+os.Args[1])
	}
}

func listJobs(attempt int, gate string) ([]ciflow.JobTimes, error) {
	listing := fmt.Sprintf("repos/%s/actions/runs/%s/attempts/%d/jobs?per_page=100",
		os.Getenv("TETRAL_CI_REPOSITORY"), os.Getenv("TETRAL_CI_RUN_ID"), attempt)
	// The listing names the run the workflow's own environment gives.
	//nolint:gosec
	command := exec.Command("gh", "api", listing)
	command.Stderr = os.Stderr
	body, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("read the jobs of attempt %d: %w", attempt, err)
	}
	return ciflow.ParseJobs(body, gate)
}

func fail(code int, message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(code)
}
