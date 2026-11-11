#!/usr/bin/env python3
"""Run one complete, disjoint part of CLI verification and retain its evidence."""

import argparse
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


def commands(profile):
    if profile == "functional":
        base = ["cargo", "test", "--locked"]
        return base + ["--", "--list"], base + ["--no-fail-fast", "--", "--skip", RELEASE, "--skip", CAPACITY]
    target, test = TARGETS[profile]
    base = ["cargo", "test", "--locked", "-p", "tetral-tests", "--test", target]
    return base + ["--", "--list"], base + [test, "--", "--exact"]


def validate_selection(profile, listing):
    names = [line.removesuffix(": test") for line in listing.splitlines() if line.endswith(": test")]
    required = [RELEASE, CAPACITY] if profile == "functional" else [TARGETS[profile][1]]
    for name in required:
        if names.count(name) != 1:
            raise ValueError(f"expected exactly one test named {name}, found {names.count(name)}")
        if profile == "functional":
            # libtest --skip is a substring filter. Reject future names that
            # would otherwise be skipped without a corresponding producer.
            shadowed = [other for other in names if name in other and other != name]
            if shadowed:
                raise ValueError(f"the skip for {name} would also omit {shadowed}")
    selected = [name for name in names if name not in required] if profile == "functional" else required
    if not selected:
        raise ValueError("verification selected no tests")
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

    def run(label, command, capture=False, cwd=workspace):
        print(f"Running {label}: {' '.join(command)}", flush=True)
        started = time.monotonic()
        output = []
        result = {"name": label, "argv": command, "exit_code": None, "seconds": None}
        report["commands"].append(result)
        save()
        with (evidence / f"{label}.log").open("w") as log:
            process = subprocess.Popen(
                command, cwd=cwd, env=environment,
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
        listing, test = commands(args.profile)
        report["selected_tests"] = validate_selection(args.profile, run("list", listing, capture=True))
        save()
        run("test", test)
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
