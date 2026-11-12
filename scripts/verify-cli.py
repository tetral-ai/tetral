#!/usr/bin/env python3
"""Run one complete, disjoint part of CLI verification and retain its evidence."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time


RELEASE = "a_release_build_holds_no_test_seam"
CAPACITY = "five_thousand_tasks_on_one_stream_each_issued_after_the_previous_terminal_ack_are_all_admitted"
TARGETS = {
    "release": ("release_seams", RELEASE),
    "task-capacity": ("tool_admission", CAPACITY),
}
# The checks of the reference control server, run inside its own Go module,
# cli/reference. The tests' limit is below the limit of the workflow step
# that runs them, so that a package that hangs is stopped by its own test
# binary, which names its running tests.
REFERENCE = [
    ("vet", ["go", "vet", "./..."]),
    ("lint", ["golangci-lint", "run", "./..."]),
    ("test", ["go", "test", "-race", "-count=1", "-timeout=5m", "./..."]),
]
# The PATH on which the Linux tests find the host's bwrap (SYSTEM_PATH in
# cli/tests/src/machine.rs and cli/tests/src/isolation.rs).
TEST_SYSTEM_PATH = "/usr/bin:/bin"
# Before 0.13.0, bwrap remounts each submount from a snapshot of the mount
# table and fails a launch when a mount goes away in between, as a concurrent
# test's drive mount does.
MINIMUM_BWRAP = (0, 13, 0)
# The programs the tests start (cli/tests/src/binaries.rs), built before the
# tests so that no test's time limit holds a build. `tetral` with its test
# seams shares the workspace's build directory; the build with the host
# bwrap's digest compiled in, which two Linux tests run, has its own.
SEAMS = ["cargo", "build", "--locked", "-p", "tetral", "--features", "test-seams", "--target-dir"]


def commands(profile):
    """The listing and the run of a profile under cargo-nextest, which runs
    each test in a process of its own and ends one that passes its time limit
    (cli/.config/nextest.toml). Both take the same filter, which names a test
    by its whole name."""
    if profile == "functional":
        scope = ["--locked"]
        select = ["-E", f"not (test(={RELEASE}) | test(={CAPACITY}))"]
    else:
        target, test = TARGETS[profile]
        scope = ["--locked", "-p", "tetral-tests", "--test", target]
        select = ["-E", f"test(={test})"]
    nextest = ["cargo", "nextest"]
    return (
        nextest + ["list", *scope, *select, "--message-format", "json"],
        nextest + ["run", *scope, *select, "--no-fail-fast"],
    )


def validate_selection(profile, listing):
    """The tests the profile's filter selects, each as `<binary> <test>`,
    from the listing's JSON line. A profile's own test must exist once: the
    functional profile leaves out exactly the tests the other two run."""
    lines = [line for line in listing.splitlines() if line.startswith('{"')]
    if not lines:
        raise ValueError("the listing holds no JSON line")
    names, selected = [], []
    for binary, suite in json.loads(lines[-1])["rust-suites"].items():
        for name, case in suite["testcases"].items():
            names.append(name)
            if case["filter-match"]["status"] == "matches" and not case["ignored"]:
                selected.append(f"{binary} {name}")
    required = [RELEASE, CAPACITY] if profile == "functional" else [TARGETS[profile][1]]
    for name in required:
        if names.count(name) != 1:
            raise ValueError(f"expected exactly one test named {name}, found {names.count(name)}")
    if not selected:
        raise ValueError("verification selected no tests")
    chosen = [entry.split(" ", 1)[1] for entry in selected]
    if profile == "functional":
        wrong = [name for name in required if name in chosen]
    else:
        wrong = [name for name in chosen if name not in required]
    if wrong or (profile != "functional" and len(selected) != 1):
        raise ValueError(f"the filter of {profile} selected {selected}")
    return selected


def check_bwrap():
    """The tests' bwrap and its version; ValueError when it is missing or too old."""
    path = shutil.which("bwrap", path=TEST_SYSTEM_PATH)
    if path is None:
        raise ValueError(f"no bwrap on {TEST_SYSTEM_PATH}, where the Linux CLI tests find it")
    output = subprocess.run([path, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    match = re.fullmatch(r"bubblewrap (\d+)\.(\d+)\.(\d+)", output)
    if match is None:
        raise ValueError(f"{path} --version printed {output!r}, not a bubblewrap version")
    if tuple(map(int, match.groups())) < MINIMUM_BWRAP:
        raise ValueError(
            f"{path} is {output}; the Linux CLI tests need bubblewrap 0.13.0 or later "
            "(scripts/install-bubblewrap.sh)"
        )
    return {"path": path, "version": output}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=["functional", *TARGETS, "reference"])
    parser.add_argument("--evidence-dir", required=True, type=Path)
    args = parser.parse_args()
    workspace = Path(__file__).resolve().parents[1] / "cli"
    evidence = args.evidence_dir.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    acceptance = evidence / "acceptance"
    acceptance.mkdir(exist_ok=True)
    environment = dict(os.environ, TETRAL_ACCEPTANCE_DIR=str(acceptance))
    report = {
        "profile": args.profile, "commands": [], "selected_tests": [], "success": False,
        "revision": os.environ.get("GITHUB_SHA", "local"),
        "platform": platform.platform(),
        "architecture": platform.machine(),
    }

    def save():
        (evidence / "verification.json").write_text(json.dumps(report, indent=2) + "\n")

    def run(label, command, capture=False, cwd=workspace, extra=None):
        print(f"Running {label}: {' '.join(command)}", flush=True)
        started = time.monotonic()
        output = []
        result = {"name": label, "argv": command, "exit_code": None, "seconds": None}
        report["commands"].append(result)
        save()
        with (evidence / f"{label}.log").open("w") as log:
            process = subprocess.Popen(
                command, cwd=cwd, env=dict(environment, **(extra or {})),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace",
            )
            for line in process.stdout:
                print(line, end="", flush=True)
                log.write(line)
                if capture:
                    output.append(line)
            code = process.wait()
        result.update(exit_code=code, seconds=round(time.monotonic() - started, 3))
        save()
        if code:
            raise subprocess.CalledProcessError(code, command)
        return "".join(output)

    save()
    try:
        if args.profile == "reference":
            for label, command in REFERENCE:
                run(label, command, cwd=workspace / "reference")
            report["success"] = True
            save()
            return 0
        if platform.system() == "Linux":
            report["bwrap"] = check_bwrap()
            save()
        if args.profile == "functional":
            run("build", ["cargo", "build", "--locked"])
        if args.profile != "release":
            run(
                "seams", SEAMS + ["target/test-seams"],
                extra={"CARGO_BUILD_BUILD_DIR": str(workspace / "target")},
            )
        if args.profile == "functional" and "bwrap" in report:
            digest = hashlib.sha256(Path(report["bwrap"]["path"]).read_bytes()).hexdigest()
            run("digest", SEAMS + ["target/test-seams-bwrap-digest"], extra={"CODEX_BWRAP_SHA256": digest})
        listing, test = commands(args.profile)
        report["selected_tests"] = validate_selection(args.profile, run("list", listing, capture=True))
        save()
        try:
            run("test", test)
        finally:
            # Each test's result and time, also of a run that failed.
            results = workspace / "target" / "nextest" / "default" / "junit.xml"
            if results.is_file():
                shutil.copyfile(results, evidence / "junit.xml")
        report["success"] = True
        save()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        report["error"] = str(error)
        save()
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
