"""Shared helpers for Tier 1 stress tests.

Kept deliberately small: invariants checker, percentile math, and a
report writer that drops a JSON artifact under
``<repo>/scripts/stress/_reports/<UTC-timestamp>/<scenario>.json`` so
Tier 1 and Tier 2 share an output location.

Scenarios opt into reporting; they are not required to. Reporting
failures (e.g. read-only filesystem) downgrade to a log line so a
broken artifact path never breaks a test.
"""

import json
import logging
import os
from datetime import datetime, timezone

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


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
