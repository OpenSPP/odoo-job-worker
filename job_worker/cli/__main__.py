"""Entry point for ``python -m odoo.addons.job_worker.cli``.

Parses the Odoo configuration and starts the multi-database runner.

All imports from ``odoo.addons.job_worker`` are deferred until after
``parse_config()`` so that the addons path is on ``sys.path`` when
Python resolves them.
"""

import faulthandler
import signal
import sys

import odoo


def main():
    # Pass sys.argv[1:] explicitly. Calling parse_config() with no
    # args only reads odoo.conf and ignores CLI flags entirely; see
    # the docstring of odoo.tools.config.parse_config.
    odoo.tools.config.parse_config(sys.argv[1:])
    # SIGUSR1 dumps Python thread stacks to stderr. Cheap, runs in a
    # signal context, useful for production debugging of stuck workers
    # via `kill -USR1 <pid>`.
    faulthandler.register(signal.SIGUSR1, all_threads=True)
    # Import after parse_config so the addons path is registered
    from .runner import QueueJobRunner

    runner = QueueJobRunner.from_environ_or_config()
    runner.run()


if __name__ == "__main__":
    main()
