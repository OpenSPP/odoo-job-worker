#!/usr/bin/env python
"""Standalone entry point for the Job Worker runner.

Usage:
    python job_worker_runner.py -c /etc/odoo/odoo.conf
    python job_worker_runner.py --addons-path=... -d mydb

This script bootstraps the Odoo configuration (which registers the
addons path) before importing the runner, avoiding the
ModuleNotFoundError that occurs with ``python -m odoo.addons.job_worker.cli``.
"""


def main():
    import faulthandler
    import signal
    import sys

    from odoo.tools import config

    # Pass sys.argv[1:] explicitly. Calling parse_config() with no
    # args only reads odoo.conf and ignores CLI flags entirely; see
    # the docstring of odoo.tools.config.parse_config.
    config.parse_config(sys.argv[1:])

    # SIGUSR1 dumps Python thread stacks to stderr. Cheap and runs in
    # a signal context, so safe to leave enabled — useful for
    # production debugging of stuck workers via `kill -USR1 <pid>`.
    faulthandler.register(signal.SIGUSR1, all_threads=True)

    from odoo.addons.job_worker.cli.runner import QueueJobRunner

    runner = QueueJobRunner.from_environ_or_config()
    runner.run()


if __name__ == "__main__":
    main()
