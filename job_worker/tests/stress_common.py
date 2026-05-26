"""Shared helpers for Tier 1 and Tier 2 stress tests.

Kept deliberately small: invariants checker, percentile math, JSON
report writer, and (for Tier 2) helpers to spawn the real
``job_worker_runner.py`` as a subprocess.

Scenarios opt into reporting; they are not required to. Reporting
failures (e.g. read-only filesystem) downgrade to a log line so a
broken artifact path never breaks a test.
"""

import contextlib
import json
import logging
import os
import signal
import subprocess
import time
from datetime import datetime, timezone

import odoo
from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)

# Location of the standalone runner script inside the container. The
# repo is bind-mounted into the test image at /mnt/extra-addons.
RUNNER_SCRIPT_DEFAULT = (
    "/mnt/extra-addons/odoo-job-worker/job_worker_runner.py"
)


def clear_queue(env):
    """Drop every queue.job row. Use in setUp for stress scenarios.

    Stress tests own the queue exclusively; partial cleanup risks
    cross-scenario interference (e.g. a leftover ``started`` row from a
    crashed run blocks acquisition).
    """
    env["queue.job"].search([]).unlink()


def percentile(values, pct):
    """Return the pct-th percentile of values (0 ≤ pct ≤ 100).

    Linear interpolation between the two nearest ranks; matches numpy's
    default. Returns 0.0 for an empty sequence so callers can record
    "no samples" without a separate code path.
    """
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    k = (len(ordered) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    if lo == hi:
        return float(ordered[lo])
    return float(ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo))


def assert_queue_invariants(testcase, env, expected_total):
    """Cross-cutting invariants asserted on the *final* DB state.

    Called by every scenario at end.

    - total terminal (done + failed + cancelled) == expected_total
    - no ``started`` rows remain (no live worker in Tier 1)
    - no duplicate identity_key in (waiting, pending, started)
    """
    QueueJob = env["queue.job"]
    terminal = QueueJob.search_count(
        [("state", "in", ["done", "failed", "cancelled"])]
    )
    testcase.assertEqual(
        terminal,
        expected_total,
        f"Invariant: terminal-state count ({terminal}) "
        f"!= expected ({expected_total})",
    )
    stuck_started = QueueJob.search_count([("state", "=", "started")])
    testcase.assertEqual(
        stuck_started,
        0,
        f"Invariant: {stuck_started} jobs stuck in 'started' at end of scenario",
    )
    env.cr.execute(
        """
        SELECT identity_key, COUNT(*) FROM queue_job
        WHERE identity_key IS NOT NULL
          AND state IN ('waiting', 'pending', 'started')
        GROUP BY identity_key HAVING COUNT(*) > 1
        """
    )
    dupes = env.cr.fetchall()
    testcase.assertFalse(
        dupes,
        f"Invariant: identity_key duplicates in non-terminal states: {dupes}",
    )


def _reports_root():
    """Return the report-directory root.

    Honors ``JOB_WORKER_STRESS_REPORT_DIR`` if set; otherwise defaults to
    ``/tmp/job_worker_stress_reports`` which is writable under the
    test docker image (the source-mount at ``/mnt/extra-addons`` is
    read-only by design).
    """
    return os.environ.get(
        "JOB_WORKER_STRESS_REPORT_DIR", "/tmp/job_worker_stress_reports"
    )


_run_timestamp = None


def _run_dir():
    """Return a stable per-process timestamped directory.

    All scenarios in the same test run share one directory so their JSON
    artifacts cluster together. Created lazily on first call.
    """
    global _run_timestamp
    if _run_timestamp is None:
        _run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(_reports_root(), _run_timestamp)


def record_report(scenario, data):
    """Persist a scenario report to disk and the log.

    Failures (read-only fs, permission denied) downgrade to a log line.
    Tests never fail because reporting failed.
    """
    payload = dict(data)
    payload.setdefault("scenario", scenario)
    payload.setdefault(
        "recorded_at", datetime.now(timezone.utc).isoformat()
    )
    # Compact JSON for the log line so the entire report stays on a single
    # line and survives Odoo's log formatter. Pretty-printed JSON goes to
    # the file artifact for human consumption.
    compact = json.dumps(payload, default=str)
    pretty = json.dumps(payload, default=str, indent=2)
    _logger.info("STRESS REPORT %s: %s", scenario, compact)
    try:
        target_dir = _run_dir()
        os.makedirs(target_dir, exist_ok=True)
        path = os.path.join(target_dir, f"{scenario}.json")
        with open(path, "w") as fh:
            fh.write(pretty)
        _logger.info("STRESS REPORT %s written to %s", scenario, path)
    except OSError as err:
        _logger.warning(
            "STRESS REPORT %s: could not write artifact (%s). Report in log only.",
            scenario,
            err,
        )


def pg_version(registry):
    """Return PostgreSQL server_version as a string (e.g. '18.1').

    Takes a registry rather than an env so callers can invoke it outside
    a ``with cursor()`` block without hitting "cursor already closed".
    Recorded in every scenario's report so PG-version-conditional
    behaviour is visible in trend data.
    """
    with registry.cursor() as cr:
        cr.execute("SHOW server_version")
        row = cr.fetchone()
        return row[0] if row else "unknown"


def fresh_env(registry):
    """Open an isolated cursor/env pair for setup or assertion work.

    Mirrors ``tests/common.external_env`` but returns the pair so the
    caller controls the lifetime (often needed when multiple commits
    are required between phases).
    """
    cr = registry.cursor()
    return cr, api.Environment(cr, SUPERUSER_ID, {})


# ---------------------------------------------------------------------------
# Tier 2 helpers — spawn the real job_worker_runner.py as a subprocess.
# ---------------------------------------------------------------------------


def _runner_argv(db_name, runner_script=RUNNER_SCRIPT_DEFAULT):
    """Build the argv for a runner subprocess against ``db_name``.

    Copies addons-path and DB connection params from the currently
    running Odoo's config so the child process can reach the same
    database (typically the test DB) with the same modules visible.
    """
    cfg = odoo.tools.config
    addons_path = cfg["addons_path"]
    if isinstance(addons_path, list):
        addons_path = ",".join(addons_path)
    argv = [
        "python3",
        runner_script,
        "-d",
        db_name,
        "--addons-path=" + addons_path,
    ]
    for key in ("db_host", "db_port", "db_user", "db_password"):
        value = cfg.get(key)
        if value:
            argv.append(f"--{key}={value}")
    # Info-level so the test can see "Worker starting", discovery
    # decisions, and per-DB log messages on shutdown.
    argv.append("--log-level=info")
    return argv


def spawn_runner(db_name, *, concurrency=2, env_overrides=None,
                 runner_script=RUNNER_SCRIPT_DEFAULT):
    """Spawn the real ``job_worker_runner.py`` as a subprocess.

    Returns a ``subprocess.Popen``. Caller is responsible for terminating
    via :func:`terminate_runner` (or use :func:`runner_subprocess` as a
    context manager).

    Concurrency is set via ``QUEUE_JOB_CONCURRENCY`` env var, which
    ``QueueJobRunner.from_environ_or_config`` already reads.
    """
    env = dict(os.environ)
    env["QUEUE_JOB_CONCURRENCY"] = str(int(concurrency))
    # Force a stable discovery interval (default 60s) so newly-installed
    # modules are picked up by the supervisor without long waits.
    env.setdefault("PYTHONUNBUFFERED", "1")
    if env_overrides:
        env.update({k: str(v) for k, v in env_overrides.items()})
    argv = _runner_argv(db_name, runner_script=runner_script)
    _logger.info("Spawning runner: %s (concurrency=%d)", " ".join(argv), concurrency)
    return subprocess.Popen(
        argv,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def terminate_runner(proc, *, sig=signal.SIGTERM, graceful_timeout=15,
                     kill_timeout=5):
    """Send ``sig`` to a runner subprocess and wait for exit.

    Falls back to SIGKILL after ``graceful_timeout`` seconds. Drains
    stdout/stderr to avoid orphaned pipes. Returns the captured (stdout,
    stderr) bytes for diagnostics.

    Idempotent: safe to call on an already-exited process.
    """
    if proc.poll() is None:
        try:
            proc.send_signal(sig)
        except ProcessLookupError:
            pass
    try:
        stdout, stderr = proc.communicate(timeout=graceful_timeout)
    except subprocess.TimeoutExpired:
        _logger.warning(
            "Runner pid=%s did not exit on %s within %ds; sending SIGKILL",
            proc.pid,
            sig,
            graceful_timeout,
        )
        proc.kill()
        try:
            stdout, stderr = proc.communicate(timeout=kill_timeout)
        except subprocess.TimeoutExpired:
            _logger.error("Runner pid=%s did not exit even on SIGKILL", proc.pid)
            stdout, stderr = b"", b""
    return stdout, stderr


@contextlib.contextmanager
def runner_subprocess(db_name, *, concurrency=2, env_overrides=None,
                      shutdown_signal=signal.SIGTERM, graceful_timeout=15):
    """Context manager wrapper around :func:`spawn_runner`.

    Guarantees cleanup of the spawned process even on test failure.
    Logs subprocess stdout/stderr (tail-only) at info/warning level so
    failures don't disappear into the void.
    """
    proc = spawn_runner(
        db_name, concurrency=concurrency, env_overrides=env_overrides
    )
    try:
        yield proc
    finally:
        stdout, stderr = terminate_runner(
            proc, sig=shutdown_signal, graceful_timeout=graceful_timeout
        )
        if stdout:
            tail = stdout[-4000:].decode("utf-8", errors="replace")
            _logger.info("Runner pid=%s stdout (tail):\n%s", proc.pid, tail)
        if stderr:
            tail = stderr[-4000:].decode("utf-8", errors="replace")
            _logger.warning("Runner pid=%s stderr (tail):\n%s", proc.pid, tail)


def wait_for_started_count(registry, *, channel, minimum, timeout=30):
    """Block until ``count(started)`` for ``channel`` reaches ``minimum``.

    Returns the elapsed wall-clock seconds. Raises ``TimeoutError`` if
    the threshold isn't reached. Use this to wait until the runner has
    actually picked jobs up before driving the next test phase.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with registry.cursor() as cr:
            cr.execute(
                "SELECT COUNT(*) FROM queue_job "
                "WHERE channel = %s AND state = 'started'",
                (channel,),
            )
            count = cr.fetchone()[0]
        if count >= minimum:
            return time.monotonic() - (deadline - timeout)
        time.sleep(0.1)
    raise TimeoutError(
        f"channel {channel!r}: only saw {count} started jobs in {timeout}s "
        f"(needed {minimum})"
    )


def wait_for_terminal_count(registry, *, channel, expected, timeout=120,
                            poll_interval=0.2):
    """Block until ``count(done+failed+cancelled)`` reaches ``expected``.

    Returns the elapsed wall-clock seconds. Raises ``TimeoutError`` on
    expiry. Returns immediately if already at expected.
    """
    deadline = time.monotonic() + timeout
    start = time.monotonic()
    while time.monotonic() < deadline:
        with registry.cursor() as cr:
            cr.execute(
                "SELECT COUNT(*) FROM queue_job "
                "WHERE channel = %s "
                "  AND state IN ('done', 'failed', 'cancelled')",
                (channel,),
            )
            count = cr.fetchone()[0]
        if count >= expected:
            return time.monotonic() - start
        time.sleep(poll_interval)
    raise TimeoutError(
        f"channel {channel!r}: only {count} reached terminal in {timeout}s "
        f"(needed {expected})"
    )


def sample_started_count(registry, *, channel):
    """One-shot ``count(started)`` query for sampling loops (S4)."""
    with registry.cursor() as cr:
        cr.execute(
            "SELECT COUNT(*) FROM queue_job "
            "WHERE channel = %s AND state = 'started'",
            (channel,),
        )
        return cr.fetchone()[0]


def install_module(db_name, module_name):
    """Install ``module_name`` into ``db_name`` via a one-shot odoo subprocess.

    Used by Tier 2 scenarios that need ``job_worker_stress`` (sleep_for,
    fail_always, retry helpers) installed in the test DB. Idempotent —
    if the module is already installed odoo silently no-ops.
    """
    cfg = odoo.tools.config
    addons_path = cfg["addons_path"]
    if isinstance(addons_path, list):
        addons_path = ",".join(addons_path)
    argv = [
        "odoo",
        "-d",
        db_name,
        "--addons-path=" + addons_path,
        "-i",
        module_name,
        "--stop-after-init",
        "--log-level=warn",
        "--without-demo=all",
        "--workers=0",
    ]
    for key in ("db_host", "db_port", "db_user", "db_password"):
        value = cfg.get(key)
        if value:
            argv.append(f"--{key}={value}")
    _logger.info("Installing %s into %s", module_name, db_name)
    result = subprocess.run(argv, capture_output=True, timeout=120, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"failed to install {module_name} into {db_name}: "
            f"stderr={result.stderr[-2000:].decode('utf-8', errors='replace')}"
        )
