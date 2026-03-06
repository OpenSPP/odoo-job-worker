#!/usr/bin/env python
"""Standalone entry point for the Job Worker runner.

Usage:
    python job_worker_runner.py -c /etc/odoo/odoo.conf
    python job_worker_runner.py --addons-path=... -d mydb

This script bootstraps the Odoo configuration (which registers the
addons path) before importing the runner, avoiding the
ModuleNotFoundError that occurs with ``python -m odoo.addons.job_worker.cli``.
"""

from odoo.tools import config

config.parse_config()

from odoo.addons.job_worker.cli.runner import QueueJobRunner  # noqa: E402

runner = QueueJobRunner.from_environ_or_config()
runner.run()
