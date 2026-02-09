"""Entry point for ``python -m odoo.addons.job_worker.cli``.

Parses the Odoo configuration and starts the multi-database runner.
"""

import odoo

from .runner import QueueJobRunner


def main():
    odoo.tools.config.parse_config()
    runner = QueueJobRunner.from_environ_or_config()
    runner.run()


if __name__ == "__main__":
    main()
