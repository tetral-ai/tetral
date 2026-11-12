#!/usr/bin/env python3
"""Run one complete, disjoint part of CLI verification and retain its evidence."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import signal
import subprocess
import sys
import time


RELEASE = "a_release_build_holds_no_test_seam"
CAPACITY = "five_thousand_tasks_on_one_stream_each_issued_after_the_previous_terminal_ack_are_all_admitted"
TARGETS = {
    "release": ("release_seams", RELEASE),
    "task-capacity": ("tool_admission", CAPACITY),
}
# The tests the functional profile leaves to a profile of their own. The
# task-capacity test is compiled on Linux only.
DEDICATED = [RELEASE, CAPACITY] if platform.system() == "Linux" else [RELEASE]
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
    required = DEDICATED if profile == "functional" else [TARGETS[profile][1]]
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


# What a run of the tests leaves when it is stopped or killed: processes its
# tests started and mounts they made. A run keeps its tests' temporary
# directories below a directory of its own and its tests in a session of
# their own, and holds a lock while it lives, in which it records that
# session. Clearing a run kills the processes of its session and those whose
# working directory is below its directories, unmounts what is mounted there
# and removes the directories. A run clears itself when it ends, also when it
# is stopped with SIGINT or SIGTERM, and the next run clears the runs whose
# locks are free, which a killed run's is.
#
# The directory is short because the tests' Unix sockets live below it, and
# a socket's path is at most 103 bytes on macOS. On macOS it is in the home
# directory: below /private/tmp a command sandboxed with nothing to read still
# reads the tests' files, and the user's temporary directory is too long.
RUNS = Path(
    os.environ.get("TETRAL_CLI_RUNS")
    or (
        str(Path.home() / ".tetral-cli-runs")
        if platform.system() == "Darwin"
        else f"/tmp/tetral-cli-runs-{os.getuid()}"
    )
)
# Where the tests keep the machines they ask to be in memory, on Linux.
MEMORY = Path("/dev/shm")
MEMORY_VARIABLE = "TETRAL_TESTS_MEMORY_DIR"


class Stopped(Exception):
    """SIGINT or SIGTERM reached this script."""


def run_directories(name):
    directories = [Path(os.path.realpath(RUNS)) / name]
    if MEMORY.is_dir():
        directories.append(MEMORY / RUNS.name / name)
    return directories


def below(path, directories):
    return any(Path(path) == directory or directory in Path(path).parents for directory in directories)


def boot():
    """What names this boot of the host."""
    if platform.system() == "Linux":
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    return subprocess.run(["sysctl", "-n", "kern.boottime"], capture_output=True, text=True, check=True).stdout.split("}")[0]


def started(pid):
    """When the process `pid` started, or "" when there is none."""
    return subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()


def session_record(pid):
    """What a lock records of the session its run's tests run in, led by `pid`."""
    return json.dumps({"session": pid, "boot": boot(), "started": started(pid)})


def recorded_session(text):
    """The session a lock's `text` records, or 0 when it may now be another's:
    one of an earlier boot, or one whose leader's process ID now names a
    process that started at another time. While a session has members its
    ID is not given to a new process, so with its leader gone it is still
    the run's."""
    try:
        record = json.loads(text)
        session = record["session"]
    except (ValueError, TypeError, KeyError):
        return 0
    if not isinstance(session, int) or session <= 0 or record.get("boot") != boot():
        return 0
    return session if started(session) in ("", record.get("started")) else 0


def run_processes(session, directories):
    """The processes of `session` and those whose working directory is below
    one of `directories`. This process and another user's are left out, and
    so is one that has ended and waits for its parent, and, by its working
    directory, one with a terminal, as a shell is, which no test has."""
    listing = subprocess.run(["ps", "-axo", "pid=,stat=,tty="], capture_output=True, text=True, check=True).stdout
    rows = [line.split() for line in listing.splitlines()]
    listed = [row[0] for row in rows if not row[1].startswith("Z")]
    terminal = {int(row[0]) for row in rows if len(row) > 2 and row[2] not in ("?", "??")}
    working = {}
    if platform.system() != "Linux":
        # lsof exits 1 when it could not read every process; what it printed holds.
        fields = subprocess.run(["lsof", "-a", "-d", "cwd", "-F", "pn"], capture_output=True, text=True).stdout
        for field in fields.splitlines():
            if field.startswith("p"):
                pid = int(field[1:])
            elif field.startswith("n"):
                working[pid] = field[1:]
    found = []
    for pid in map(int, listed):
        if pid == os.getpid():
            continue
        try:
            if session and os.getsid(pid) == session:
                found.append(pid)
                continue
            if pid in terminal:
                continue
            directory = working.get(pid) or os.readlink(f"/proc/{pid}/cwd")
        except OSError:
            continue
        if below(directory, directories):
            found.append(pid)
    return found


def run_mounts(directories):
    """The mounts below `directories`, the deepest first, each as its point,
    its file system's type and, on Linux, its device's minor number."""
    found = []
    if platform.system() == "Linux":
        for line in Path("/proc/self/mountinfo").read_text().splitlines():
            fields = line.split()
            point = re.sub(r"\\([0-7]{3})", lambda octal: chr(int(octal[1], 8)), fields[4])
            found.append((point, fields[fields.index("-") + 1], fields[2].split(":")[1]))
    else:
        listing = subprocess.run(["mount"], capture_output=True, text=True, check=True).stdout
        for line in listing.splitlines():
            match = re.fullmatch(r"(.+) on (.+) \((\w+).*\)", line)
            if match:
                found.append((match[2], "disk" if match[1].startswith("/dev/disk") else match[3], None))
    return sorted((mount for mount in found if below(mount[0], directories)), key=lambda mount: -len(mount[0]))


def unmount(point, kind, minor):
    quiet = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    if kind.startswith("fuse"):
        # A process killed while it waits for its own mount's answer never
        # ends; the abort ends that wait.
        try:
            Path(f"/sys/fs/fuse/connections/{minor}/abort").write_text("1")
        except OSError:
            pass
        if subprocess.run(["fusermount3", "-u", "-z", point], **quiet).returncode == 0:
            return
    if kind == "disk":
        subprocess.run(["hdiutil", "detach", "-force", point], **quiet)
    elif platform.system() == "Linux":
        subprocess.run(["umount", "-l", point], **quiet)
    else:
        subprocess.run(["umount", "-f", point], **quiet)


def clear_run(session, directories):
    """Kills, unmounts and removes what a run left. Returns what it found,
    as `process <pid>` and `mount <point>`, and what of it is still there."""
    found = set()

    def kill():
        deadline = time.monotonic() + 10
        while (left := run_processes(session, directories)) and time.monotonic() < deadline:
            for pid in left:
                found.add(f"process {pid}")
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
            time.sleep(0.1)
        return left

    kill()
    for mount in run_mounts(directories):
        found.add(f"mount {mount[0]}")
        unmount(*mount)
    left = [f"process {pid}" for pid in kill()] + [f"mount {mount[0]}" for mount in run_mounts(directories)]
    if not left:
        for directory in directories:
            remove_tree(directory)
        left += [f"directory {directory}" for directory in directories if os.path.lexists(directory)]
    if platform.system() == "Darwin":
        # The RAM disks of the tests' in-memory machines, named for the test
        # process that attached them.
        for volume in Path("/Volumes").glob("tetral-tests-*-*"):
            try:
                os.kill(int(volume.name.split("-")[2]), 0)
            except (OSError, ValueError):
                subprocess.run(["hdiutil", "detach", "-force", str(volume)], capture_output=True)
                if volume.exists():
                    left.append(f"volume {volume}")
    return sorted(found), left


def remove_tree(directory):
    """Removes `directory`, giving this user's directories in it back the
    permissions their removal needs, as tests take them away."""
    if not os.path.isdir(directory) or os.path.islink(directory):
        return

    def allow(path):
        try:
            os.chmod(path, 0o700)
        except OSError:
            pass

    allow(directory)
    # Top down: each directory is allowed before the walk enters it.
    for root, names, _ in os.walk(directory):
        for name in names:
            if not os.path.islink(os.path.join(root, name)):
                allow(os.path.join(root, name))
    shutil.rmtree(directory, ignore_errors=True)


def clear_ended_runs():
    """Clears every run whose lock is free. Returns what those runs had left
    and what of it is still there."""
    found, left = [], []
    RUNS.mkdir(mode=0o700, exist_ok=True)
    status = RUNS.lstat()
    if not RUNS.is_dir() or RUNS.is_symlink() or status.st_uid != os.getuid():
        raise ValueError(f"{RUNS} is not a directory of this user")
    for path in sorted(RUNS.glob("*.lock")):
        try:
            lock = path.open("r+")
        except FileNotFoundError:
            # Another run cleared it meanwhile.
            continue
        with lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                continue
            cleared = clear_run(recorded_session(lock.read()), run_directories(path.stem))
            found += cleared[0]
            left += cleared[1]
            if not cleared[1]:
                path.unlink(missing_ok=True)
    return found, left


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

    # The lock of this run and the session of its tests, once they run.
    own = {"lock": None, "path": None, "session": 0, "directories": []}

    def run(label, command, capture=False, cwd=workspace, extra=None, session=False):
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
                start_new_session=session,
            )
            if session:
                own["session"] = process.pid
                own["lock"].write(session_record(process.pid))
                own["lock"].flush()
            try:
                for line in process.stdout:
                    print(line, end="", flush=True)
                    # Kept also when the run is killed.
                    log.write(line)
                    log.flush()
                    if capture:
                        output.append(line)
                code = process.wait()
            except Stopped:
                # What the command leaves is cleared with the run, so it
                # gets a short time to end by itself.
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                raise
        result.update(exit_code=code, seconds=round(time.monotonic() - started, 3))
        save()
        if code:
            raise subprocess.CalledProcessError(code, command)
        return "".join(output)

    def stop(number, _frame):
        raise Stopped(signal.Signals(number).name)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
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
        found, left = clear_ended_runs()
        if found:
            report["cleared_of_ended_runs"] = found
            save()
        if left:
            raise ValueError(f"an earlier run left what could not be cleared: {', '.join(left)}")
        name = f"run-{os.getpid()}-{secrets.token_hex(4)}"
        # Locked before it is named as a lock, so that no other run's
        # clearing finds it free.
        own["lock"] = (RUNS / f"{name}.new").open("w")
        fcntl.flock(own["lock"], fcntl.LOCK_EX)
        own["path"] = RUNS / f"{name}.lock"
        os.rename(own["lock"].name, own["path"])
        own["directories"] = run_directories(name)
        for directory in own["directories"]:
            directory.mkdir(mode=0o700, parents=True)
        environment["TMPDIR"] = str(RUNS / name)
        if len(own["directories"]) > 1:
            environment[MEMORY_VARIABLE] = str(own["directories"][1])
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
            run("test", test, session=True)
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
    except Stopped as stopped:
        report["error"] = f"stopped by {stopped}"
        report["success"] = False
        save()
        print(report["error"], file=sys.stderr)
        return 1
    finally:
        if own["lock"] is not None:
            # A second signal must not end the clearing halfway.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            found, left = clear_run(own["session"], own["directories"])
            if found:
                report["cleared"] = found
            if left:
                report["left"] = left
                report["success"] = False
                print(f"the run left what could not be cleared: {', '.join(left)}", file=sys.stderr)
            else:
                own["path"].unlink(missing_ok=True)
            own["lock"].close()
            save()
    return 1 if "left" in report else 0


if __name__ == "__main__":
    sys.exit(main())
