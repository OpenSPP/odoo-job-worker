"""Heartbeat liveness file for the standalone Job Worker runner.

The runner has no HTTP endpoint, so a container orchestrator cannot probe
it with an HTTP healthcheck.  Instead the supervisor loop writes a small
heartbeat file on every healthy iteration and an external healthcheck
(``job_worker_healthcheck.py``) verifies the file is recent.

A stale heartbeat therefore means one of:

* the runner process is gone (the file is never refreshed),
* the supervisor loop has stopped iterating (hung / deadlocked), or
* the worker fleet is degraded and the runner is deliberately withholding
  the heartbeat (see ``QueueJobRunner._update_heartbeat``).

This module imports only the standard library so the healthcheck can run
without bootstrapping Odoo.
"""

import os
import time

# Default location of the heartbeat file.  ``/tmp`` is always writable by
# the unprivileged container user and is wiped on container restart, which
# is exactly what we want for an ephemeral liveness marker.
DEFAULT_HEARTBEAT_FILE = "/tmp/job_worker_heartbeat"

# Default maximum age (in seconds) before the heartbeat is considered stale.
# The supervisor loop refreshes roughly every 10s, so 60s tolerates a few
# slow iterations (e.g. a discovery pass querying many databases) without
# producing false negatives.
DEFAULT_MAX_AGE_SECONDS = 60

ENV_HEARTBEAT_FILE = "JOB_WORKER_HEARTBEAT_FILE"
ENV_MAX_AGE_SECONDS = "JOB_WORKER_HEARTBEAT_MAX_AGE"


def heartbeat_file_path():
    """Return the heartbeat file path from the environment or the default."""
    return os.environ.get(ENV_HEARTBEAT_FILE) or DEFAULT_HEARTBEAT_FILE


def max_age_seconds():
    """Return the staleness threshold (seconds) from the environment.

    Falls back to ``DEFAULT_MAX_AGE_SECONDS`` when the variable is unset,
    non-numeric, or not strictly positive.
    """
    raw = os.environ.get(ENV_MAX_AGE_SECONDS)
    if not raw:
        return DEFAULT_MAX_AGE_SECONDS
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return DEFAULT_MAX_AGE_SECONDS
    return value if value > 0 else DEFAULT_MAX_AGE_SECONDS


def write_heartbeat(path=None, now=None):
    """Atomically record the current wall-clock time in the heartbeat file.

    Wall-clock time (``time.time``) is used rather than ``time.monotonic``
    because the value is read back by a *separate* process (the
    healthcheck), and monotonic clocks are not comparable across processes.

    The write is atomic — content goes to a temporary file that is then
    ``os.replace``-d onto the target — so a concurrent reader never observes
    a half-written value.
    """
    if path is None:
        path = heartbeat_file_path()
    if now is None:
        now = time.time()
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as handle:
        handle.write(repr(float(now)))
    os.replace(tmp_path, path)


def read_heartbeat(path=None):
    """Return the timestamp stored in the heartbeat file, or ``None``.

    ``None`` is returned when the file is missing, empty, or unparseable.
    """
    if path is None:
        path = heartbeat_file_path()
    try:
        with open(path) as handle:
            content = handle.read().strip()
    except OSError:
        return None
    if not content:
        return None
    try:
        return float(content)
    except ValueError:
        return None


def is_fresh(path=None, max_age=None, now=None):
    """Return ``True`` if the heartbeat exists and is newer than ``max_age``."""
    if max_age is None:
        max_age = max_age_seconds()
    if now is None:
        now = time.time()
    timestamp = read_heartbeat(path)
    if timestamp is None:
        return False
    return (now - timestamp) < max_age
