// Command runner runs this module's tests as one run: the Linux lane
// and the Mac lane at once, from a harness compiled once and Engine services
// built once. Each lane's rows, listed with their budgets, are packed table
// by table into jobs; each job is a test process of its own with one Engine,
// and each lane runs several jobs at a time. README.md ("Running the
// lanes") lists its flags and inputs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	joint "github.com/tetral-ai/tetral/integration/cli-engine"
	"golang.org/x/sys/unix"
)

// config is one run's flags and the inputs it reads itself.
type config struct {
	out, records string
	lanes        []string
	benchmark    bool
	slots        map[string]int
	jobLimit     int
	only         *regexp.Regexp
	// maxMacRoundTrip is the longest round trip to the Mac at which
	// the Mac lane starts.
	maxMacRoundTrip time.Duration
	// module is this module's directory, repository the repository's root.
	module, repository string
	mac                macHost
}

func main() {
	os.Exit(run())
}

// run returns 0 when every listed row passed and every job exited 0, 1
// otherwise, and 2 when the run could not start its jobs.
func run() int {
	started := time.Now()
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "runner:", err)
		return 2
	}
	if err := os.MkdirAll(cfg.out, 0o700); err != nil {
		fmt.Fprintln(os.Stderr, "runner:", err)
		return 2
	}
	r := &runner{cfg: cfg, started: started, results: map[string][]jobResult{}, walls: map[string]time.Duration{}, dirsBefore: machineDirs()}
	logFile, err := os.Create(filepath.Join(cfg.out, "runner.log"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "runner:", err)
		return 2
	}
	defer logFile.Close()
	r.logOut = io.MultiWriter(os.Stdout, logFile)
	// Every process the run starts stays a descendant of the runner, also
	// once its parent ended, so that the cleanup finds exactly them.
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		r.log("set the runner as child subreaper: %v", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	defer r.cleanup()
	code := r.runLanes(ctx)
	return code
}

func parseFlags(args []string) (config, error) {
	flags := flag.NewFlagSet("runner", flag.ContinueOnError)
	out := flags.String("out", "", "the run's directory, which must not exist: the harness, the Engine builds, job tables, job logs and the analysis")
	records := flags.String("records", "", "TETRAL_ACCEPTANCE_DIR of every job")
	lanes := flags.String("lanes", "linux,macos", "the lanes to run, comma-separated")
	benchmark := flags.Bool("benchmark", false, "run the benchmark lane: only the rows whose claim is a recorded number")
	linuxSlots := flags.Int("linux-slots", 6, "jobs the Linux lane runs at once")
	macSlots := flags.Int("mac-slots", 8, "jobs the Mac lane runs at once")
	jobLimit := flags.Int("job-limit", 200, "budget-seconds of one job; a row of this budget or more is a job of its own")
	only := flags.String("only", "", "a regular expression; only the rows whose name matches run")
	maxMacRoundTrip := flags.Duration("max-mac-rtt", 20*time.Millisecond, "the longest round trip to the Mac, the shortest of five connects, at which the Mac lane starts")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	cfg := config{benchmark: *benchmark, jobLimit: *jobLimit, slots: map[string]int{"linux": *linuxSlots, "macos": *macSlots}, maxMacRoundTrip: *maxMacRoundTrip}
	if *out == "" || *records == "" {
		return cfg, errors.New("-out and -records are required")
	}
	if _, err := os.Stat(*out); err == nil {
		return cfg, fmt.Errorf("-out %s exists; each run writes a directory of its own", *out)
	}
	var err error
	if cfg.out, err = filepath.Abs(*out); err != nil {
		return cfg, err
	}
	if cfg.records, err = filepath.Abs(*records); err != nil {
		return cfg, err
	}
	for lane := range strings.SplitSeq(*lanes, ",") {
		if lane != "linux" && lane != "macos" {
			return cfg, fmt.Errorf("-lanes names %q; want linux and macos", lane)
		}
		if cfg.slots[lane] < 1 {
			return cfg, fmt.Errorf("the %s lane has %d slots; want at least 1", lane, cfg.slots[lane])
		}
		cfg.lanes = append(cfg.lanes, lane)
	}
	if cfg.jobLimit < 1 {
		return cfg, errors.New("-job-limit must be positive")
	}
	if *only != "" {
		if cfg.only, err = regexp.Compile(*only); err != nil {
			return cfg, fmt.Errorf("-only: %w", err)
		}
	}
	if cfg.module, err = os.Getwd(); err != nil {
		return cfg, err
	}
	if _, err := os.Stat(filepath.Join(cfg.module, "go.mod")); err != nil {
		return cfg, errors.New("run the runner from integration/cli-engine")
	}
	cfg.repository = filepath.Dir(filepath.Dir(cfg.module))
	if slices.Contains(cfg.lanes, "macos") {
		cfg.mac = macHost{destination: os.Getenv("TETRAL_MAC_SSH_DESTINATION"), key: os.Getenv("TETRAL_MAC_SSH_KEY")}
		if cfg.mac.destination == "" || cfg.mac.key == "" || os.Getenv("TETRAL_CLI_BIN_MACOS") == "" {
			return cfg, errors.New("the Mac lane needs TETRAL_MAC_SSH_DESTINATION, TETRAL_MAC_SSH_KEY and TETRAL_CLI_BIN_MACOS")
		}
	}
	return cfg, nil
}

// runner is one run's state.
type runner struct {
	cfg     config
	started time.Time
	logMu   sync.Mutex
	logOut  io.Writer
	// binary is the compiled harness, builds the Engine services.
	binary, builds string
	// listings and jobs are each lane's listed rows and packed jobs.
	listings map[string]listing
	jobs     map[string][]job
	preflight
	mu      sync.Mutex
	results map[string][]jobResult
	walls   map[string]time.Duration
	wall    time.Duration
	// dirsBefore are the machine directories that existed before the run.
	dirsBefore []string
	// mac is the Mac lane's state on the Mac, for its cleanup.
	mac      *macLane
	networks networkSampler
}

func (r *runner) log(format string, args ...any) {
	r.logMu.Lock()
	defer r.logMu.Unlock()
	fmt.Fprintf(r.logOut, "%s +%6.1fs %s\n", time.Now().UTC().Format("15:04:05"), time.Since(r.started).Seconds(), fmt.Sprintf(format, args...))
}

func (r *runner) runLanes(ctx context.Context) int {
	r.log("run in %s: lanes %s, benchmark lane %t, slots linux %d mac %d, job limit %d s, records in %s", r.cfg.out, strings.Join(r.cfg.lanes, ","), r.cfg.benchmark, r.cfg.slots["linux"], r.cfg.slots["macos"], r.cfg.jobLimit, r.cfg.records)
	if err := r.checkHosts(ctx); err != nil {
		r.log("preflight: %v", err)
		return 2
	}
	r.binary = filepath.Join(r.cfg.out, "joint.test")
	compile := exec.CommandContext(ctx, "go", "test", "-c", "-o", r.binary, ".")
	compile.Dir = r.cfg.module
	if out, err := compile.CombinedOutput(); err != nil {
		r.log("compile the harness: %v: %s", err, out)
		return 2
	}
	r.log("harness compiled")
	r.builds = filepath.Join(r.cfg.out, "builds")
	if err := os.Mkdir(r.builds, 0o700); err != nil {
		r.log("%v", err)
		return 2
	}
	if err := joint.BuildServices(r.cfg.repository, r.builds); err != nil {
		r.log("%v", err)
		return 2
	}
	r.log("Engine services built")
	if err := r.listAndPack(ctx); err != nil {
		r.log("%v", err)
		return 2
	}
	if err := r.checkNetworks(ctx); err != nil {
		r.log("preflight: %v", err)
		return 2
	}
	r.networks.start(r)
	var wg sync.WaitGroup
	for _, machineOS := range r.cfg.lanes {
		wg.Go(func() { r.lane(ctx, machineOS) })
	}
	wg.Wait()
	r.wall = time.Since(r.started)
	r.networks.stop()
	r.log("lanes ended: run wall %.1f s", r.wall.Seconds())
	return r.report()
}

// listAndPack lists each lane's rows with the harness's list mode, which
// fails on a test with no budget, and packs them into jobs.
func (r *runner) listAndPack(ctx context.Context) error {
	r.listings, r.jobs = map[string]listing{}, map[string][]job{}
	for _, machineOS := range r.cfg.lanes {
		command := exec.CommandContext(ctx, r.binary, "-test.run", ".", "-test.count=1", "-test.v")
		command.Env = append(r.laneEnv(machineOS), "TETRAL_JOINT_LIST=1")
		out, err := command.CombinedOutput()
		if writeErr := writeFile(filepath.Join(r.cfg.out, "list-"+machineOS+".txt"), out); writeErr != nil {
			return writeErr
		}
		if err != nil {
			return fmt.Errorf("the %s listing failed: %v; %s", machineOS, err, failureLines(string(out), 5))
		}
		l, err := parseListing(string(out))
		if err != nil {
			return err
		}
		if r.cfg.only != nil {
			l.rows = slices.DeleteFunc(l.rows, func(row row) bool { return !r.cfg.only.MatchString(row.name) })
		}
		r.listings[machineOS] = l
		r.jobs[machineOS] = pack(l.rows, r.cfg.jobLimit, machineOS[:1])
		if len(r.jobs[machineOS]) > maxJobsPerLane {
			return fmt.Errorf("the %s lane has %d jobs; the port ranges hold %d", machineOS, len(r.jobs[machineOS]), maxJobsPerLane)
		}
		var table strings.Builder
		for _, j := range r.jobs[machineOS] {
			names := make([]string, len(j.rows))
			for i, row := range j.rows {
				names[i] = row.name
			}
			fmt.Fprintf(&table, "%s\t%d\t%s\t%s\n", j.name, j.budget, j.pattern(), strings.Join(names, " "))
		}
		if err := writeFile(filepath.Join(r.cfg.out, "jobs-"+machineOS+".tsv"), []byte(table.String())); err != nil {
			return err
		}
		r.log("%s: %d rows in %d jobs; needs a person at the Mac, never run here: %s", machineOS, len(l.rows), len(r.jobs[machineOS]), strings.Join(l.persons, " "))
	}
	return nil
}

// laneEnv is the environment of the lane's test processes: the runner's
// own, which holds the harness's inputs, and the lane's.
func (r *runner) laneEnv(machineOS string) []string {
	env := append(environ(), "TETRAL_MACHINE_OS="+machineOS, "TETRAL_ACCEPTANCE_DIR="+r.cfg.records, "TETRAL_ENGINE_BUILDS_DIR="+r.builds)
	if r.cfg.benchmark {
		env = append(env, "TETRAL_JOINT_LANE=benchmark")
	}
	if machineOS == "macos" && r.mac != nil && r.mac.dir != "" {
		env = append(env, "TETRAL_MAC_LANE_DIR="+r.mac.dir)
	}
	return env
}

// environ is the runner's environment without the variables the runner
// sets for each lane and job.
func environ() []string {
	return slices.DeleteFunc(os.Environ(), func(kv string) bool {
		name, _, _ := strings.Cut(kv, "=")
		return slices.Contains([]string{"TETRAL_MACHINE_OS", "TETRAL_ACCEPTANCE_DIR", "TETRAL_ENGINE_BUILDS_DIR", "TETRAL_JOINT_LANE", "TETRAL_JOINT_LIST", "TETRAL_JOINT_PORTS", "TETRAL_MAC_LANE_DIR"}, name)
	})
}

func writeFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o600)
}
