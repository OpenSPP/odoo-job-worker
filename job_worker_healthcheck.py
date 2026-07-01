#!/usr/bin/env python
"""Standalone healthcheck for the Job Worker runner.

Exits 0 when the runner's heartbeat file is fresh, 1 otherwise.  Intended
as a container ``HEALTHCHECK`` command, e.g. in docker-compose:

    healthcheck:
      test: ["CMD-SHELL", "python /path/to/job_worker_healthcheck.py || exit 1"]

Unlike ``job_worker_runner.py`` this deliberately does *not* import Odoo:
it stays fast and dependency-free by importing only the standard-library
heartbeat helper.  The heartbeat file location and staleness threshold are
read from the ``JOB_WORKER_HEARTBEAT_FILE`` and ``JOB_WORKER_HEARTBEAT_MAX_AGE``
environment variables (with sensible defaults), exactly as the runner does.
"""

import os
import sys

# Import the heartbeat helper directly from the cli package directory
# without importing the ``job_worker`` Odoo addon package (whose __init__
# pulls in Odoo and psycopg2).  The helper uses only the standard library.
_CLI_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "job_worker", "cli")
sys.path.insert(0, _CLI_DIR)

import heartbeat  # noqa: E402  (import deferred until sys.path is set)


def main():
    sys.exit(0 if heartbeat.is_fresh() else 1)


if __name__ == "__main__":
    main()
