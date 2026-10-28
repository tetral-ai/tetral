#!/usr/bin/env python3
"""Run one complete, disjoint part of CLI verification and retain its evidence."""

import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time


RELEASE = "a_release_build_holds_no_test_seam"
CAPACITY = "five_thousand_tasks_on_one_stream_each_issued_after_the_previous_terminal_ack_are_all_admitted"
TARGETS = {
    "release": ("release_seams", RELEASE),
    "task-capacity": ("tool_admission", CAPACITY),
}


def commands(profile):
    if profile == "functional":
        base = ["cargo", "test", "--locked"]
        return base + ["--", "--list"], base + ["--", "--skip", RELEASE, "--skip", CAPACITY]
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=["functional", *TARGETS])
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

    def run(label, command, capture=False):
        print(f"Running {label}: {' '.join(command)}", flush=True)
        started = time.monotonic()
        output = []
        result = {"name": label, "argv": command, "exit_code": None, "seconds": None}
        report["commands"].append(result)
        save()
        with (evidence / f"{label}.log").open("w") as log:
            process = subprocess.Popen(
                command, cwd=workspace, env=environment,
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
