"""Helpers for loading the runner module in tests without Odoo installed.

The runner module imports ``odoo``, ``psycopg2``, and ``QueueWorker``
from the sibling ``worker`` module.  In test environments where Odoo
is not available, we inject lightweight stub modules into
``sys.modules`` before importing the runner so that the module-level
imports succeed.  Individual tests then patch these stubs with proper
mocks as needed.
"""

import importlib.util
import os
import sys
import types
from unittest.mock import MagicMock


def _ensure_stub(name):
    """Insert a stub module into sys.modules if not already present."""
    if name not in sys.modules:
        sys.modules[name] = types.ModuleType(name)


def load_runner():
    """Load and return the runner module with stubbed dependencies."""
    # Stub out odoo and its submodules
    for mod_name in (
        "odoo",
        "odoo.tools",
        "odoo.service",
        "odoo.service.db",
        "odoo.sql_db",
    ):
        _ensure_stub(mod_name)

    # Give odoo.tools a config dict-like object
    odoo_stub = sys.modules["odoo"]
    tools_stub = sys.modules["odoo.tools"]
    if not hasattr(tools_stub, "config"):
        tools_stub.config = {}
    odoo_stub.tools = tools_stub

    service_stub = sys.modules["odoo.service"]
    db_stub = sys.modules["odoo.service.db"]
    service_stub.db = db_stub
    odoo_stub.service = service_stub

    sql_db_stub = sys.modules["odoo.sql_db"]
    odoo_stub.sql_db = sql_db_stub

    # Stub psycopg2. The hierarchy matters, not just the names: the runner
    # catches psycopg2.Error to retry a registry load in place, and
    # OperationalError must be a subclass of it exactly as in the real driver,
    # or a test raising OperationalError would take the wrong branch. Anything
    # already present (the real driver, if it was imported first) is left
    # alone.
    _ensure_stub("psycopg2")
    psycopg2_stub = sys.modules["psycopg2"]
    if not hasattr(psycopg2_stub, "Error"):
        psycopg2_stub.Error = type("Error", (Exception,), {})
    if not hasattr(psycopg2_stub, "OperationalError"):
        psycopg2_stub.OperationalError = type(
            "OperationalError", (psycopg2_stub.Error,), {}
        )

    # Stub the worker module so ``from .worker import QueueWorker`` resolves.
    # We create a fake package structure that allows relative imports.
    cli_dir = os.path.join(os.path.dirname(__file__), "..", "cli")

    # Register the cli package and its worker submodule
    _ensure_stub("job_worker")
    _ensure_stub("job_worker.cli")
    _ensure_stub("job_worker.cli.worker")

    worker_stub = sys.modules["job_worker.cli.worker"]
    worker_stub.QueueWorker = MagicMock(name="QueueWorker")

    cli_stub = sys.modules["job_worker.cli"]
    cli_stub.worker = worker_stub

    # Load the real heartbeat module (standard-library only, no Odoo) so the
    # runner's ``from .heartbeat import ...`` resolves to the genuine
    # implementation rather than a stub.
    heartbeat_path = os.path.join(cli_dir, "heartbeat.py")
    heartbeat_spec = importlib.util.spec_from_file_location(
        "job_worker.cli.heartbeat", heartbeat_path
    )
    heartbeat_mod = importlib.util.module_from_spec(heartbeat_spec)
    heartbeat_mod.__package__ = "job_worker.cli"
    heartbeat_spec.loader.exec_module(heartbeat_mod)
    sys.modules["job_worker.cli.heartbeat"] = heartbeat_mod
    cli_stub.heartbeat = heartbeat_mod

    # Load the runner module as part of the job_worker.cli package
    runner_path = os.path.join(cli_dir, "runner.py")
    spec = importlib.util.spec_from_file_location(
        "job_worker.cli.runner",
        runner_path,
        submodule_search_locations=[],
    )
    runner = importlib.util.module_from_spec(spec)
    runner.__package__ = "job_worker.cli"
    spec.loader.exec_module(runner)
    sys.modules["job_worker.cli.runner"] = runner
    return runner
