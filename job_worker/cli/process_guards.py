"""Process-wide safety guards for the job-worker runner process.

The job worker loads Odoo registries, but it never installs, upgrades or
uninstalls modules — another process (the web container's ``-u``, a one-shot
upgrade) owns that. Odoo does not know the difference: when *any* registry
load fails, ``Registry.new()`` calls ``odoo.modules.loading.reset_modules_state``,
which runs ``UPDATE ir_module_module SET state='installed' WHERE state IN
('to remove', 'to upgrade')`` (and ``to install`` → ``uninstalled``). Run from
the job worker while an upgrade is in progress elsewhere, that overwrites the
upgrading process's module states in the middle of its work, and logs
"Transient module states were reset" from the wrong process.

The upgrade gate keeps the worker from loading its registry during an
upgrade, but a small window remains between the gate's probe and the load.
Replacing ``reset_modules_state`` in this process makes the harmful write
impossible rather than unlikely. ``Registry.new()`` imports the function at
call time, so the replacement is what it calls.

Only ever installed from the runner process (``QueueJobRunner.run``), never
from a process that serves HTTP or runs upgrades.
"""

import logging

_logger = logging.getLogger(__name__)


def _guarded_reset_modules_state(db_name):
    _logger.warning(
        "Registry load failed for database %s in the job-worker process; NOT "
        "resetting transient module states (Odoo would have logged "
        '"Transient module states were reset" here). Module states belong to '
        "the process running the install/upgrade.",
        db_name,
    )


def install():
    """Replace Odoo's ``reset_modules_state`` in this process.

    Returns the original function when it was replaced, ``None`` when there
    was nothing to do (already installed, or Odoo is not importable).
    """
    try:
        from odoo.modules import loading
    except ImportError:
        return None
    original = getattr(loading, "reset_modules_state", None)
    if original is None or original is _guarded_reset_modules_state:
        return None
    loading.reset_modules_state = _guarded_reset_modules_state
    _logger.info(
        "Job worker process guard installed: this process will never reset "
        "module states"
    )
    return original
