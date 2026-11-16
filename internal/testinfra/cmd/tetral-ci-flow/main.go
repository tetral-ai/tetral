// Command tetral-ci-flow makes the decisions of package ciflow inside a
// workflow: decide prints which products' jobs a change runs, needs applies
// a gate's rule to the results of the jobs it needs. duration, which no
// workflow runs, prints from a checkout how long the jobs of a run named by
// its ID waited for a runner and ran; gh reads the repository from the
// checkout and the token from the environment or its own login.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"slices"

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
		run := flags.String("run", "", "the run's ID")
		gate := flags.String("gate", "", "the gate job's name; the run ends when it finished")
		_ = flags.Parse(os.Args[2:])
		if *run == "" || *gate == "" {
			fail(2, "usage: tetral-ci-flow duration --run <ID> --gate <name>")
		}
		body, err := readAPI("repos/{owner}/{repo}/actions/runs/" + *run)
		if err != nil {
			fail(1, err.Error())
		}
		var listed struct {
			Attempt int `json:"run_attempt"`
		}
		if err := json.Unmarshal(body, &listed); err != nil || listed.Attempt < 1 {
			fail(1, "the run does not name its attempt")
		}
		jobs, err := listJobs(*run, listed.Attempt)
		if err != nil {
			fail(1, err.Error())
		}
		// A job this attempt did not run again is listed with this attempt's
		// creation time; the attempt that ran it holds the first.
		var earlier [][]ciflow.JobTimes
		if slices.ContainsFunc(jobs, ciflow.JobTimes.RanEarlier) {
			for before := listed.Attempt - 1; before >= 1; before-- {
				attempt, err := listJobs(*run, before)
				if err != nil {
					fail(1, err.Error())
				}
				earlier = append(earlier, attempt)
			}
		}
		if jobs, err = ciflow.WithFirstCreations(jobs, earlier...); err != nil {
			fail(1, err.Error())
		}
		for _, line := range ciflow.DurationReport(jobs, *gate) {
			fmt.Println(line)
		}
	default:
		fail(2, "unknown command "+os.Args[1])
	}
}

func listJobs(run string, attempt int) ([]ciflow.JobTimes, error) {
	body, err := readAPI(fmt.Sprintf("repos/{owner}/{repo}/actions/runs/%s/attempts/%d/jobs?per_page=100", run, attempt))
	if err != nil {
		return nil, fmt.Errorf("read the jobs of attempt %d: %w", attempt, err)
	}
	return ciflow.ParseJobs(body)
}

func readAPI(path string) ([]byte, error) {
	//nolint:gosec // The path names the run the caller gave.
	command := exec.Command("gh", "api", path)
	command.Stderr = os.Stderr
	return command.Output()
}

func fail(code int, message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(code)
}
