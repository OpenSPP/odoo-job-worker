"""Pause the job worker while a module install/upgrade owns the database.

A job worker running beside Odoo (a "sidecar") must never take part in a
module install or upgrade that another process is running. Loading the Odoo
registry is not read-only: ``Registry.new()`` runs every model's
``_register_hook`` (which some modules use to write data), turns into a module
*upgrade* if it finds ``base.partially_updated_database``, and on any failure
calls ``reset_modules_state()`` — which marks the other process's
``to upgrade`` modules ``installed`` in the middle of its upgrade.

The gate answers one question, cheaply and without a registry: *may this
worker load its registry and take jobs right now?* It is closed while

* any module is ``to install`` / ``to upgrade`` / ``to remove`` — the same
  condition Odoo's own cron runner refuses to run under
  (``ir.cron._check_modules_state``);
* ``base.partially_updated_database`` is set — a previous upgrade stopped
  part-way, and the next registry load would resume it;
* the version of an installed module on this worker's addons path differs
  from the one recorded in the database — new code on the old schema (the
  upgrade has not run yet) or old code on the new schema (this worker was not
  redeployed); or
* the probe itself is blocked by a lock or statement timeout — an upgrade
  holding a schema lock is the usual cause, so this fails closed.

Any other database error propagates, so an unreachable database is still
handled — and reported degraded — by the worker's own recovery instead of
hiding behind an endless "upgrade" pause.

Unlike Odoo's cron, the gate never resets module states. A pause that does
not end is reported unhealthy (``is_overdue``) and logged loudly; deciding
that an upgrade is dead belongs to a person, or to the upgrading process.

Odoo is imported lazily so the runner's pure-Python tests can load this module
against stubs.
"""

import logging
import time
from collections import namedtuple

import psycopg2

_logger = logging.getLogger(__name__)

REASON_MODULES_IN_TRANSITION = "modules-in-transition"
REASON_PARTIAL_UPDATE = "partial-update-flag"
REASON_VERSION_SKEW = "version-skew"
REASON_PROBE_BLOCKED = "probe-blocked"

# lock_not_available, query_canceled (statement_timeout)
_BLOCKED_PGCODES = ("55P03", "57014")

# How many entries of a details list a log line shows before summarising.
_MAX_LOGGED_DETAILS = 20

GateState = namedtuple("GateState", "open reason details missing", defaults=((), ()))
OPEN = GateState(True, None)

_PROBE_QUERY = """
    SELECT
        (SELECT array_agg(name || ':' || state ORDER BY name)
           FROM ir_module_module
          WHERE state IN ('to install', 'to upgrade', 'to remove')),
        EXISTS (SELECT 1 FROM ir_config_parameter
                 WHERE key = 'base.partially_updated_database'),
        (SELECT array_agg(
                    m.name || ': code ' || c.version
                    || ', database ' || COALESCE(m.latest_version, 'none')
                    ORDER BY m.name)
           FROM ir_module_module m
           JOIN unnest(%(names)s::text[], %(versions)s::text[])
                AS c(name, version) ON c.name = m.name
          WHERE m.state = 'installed'
            AND m.latest_version IS DISTINCT FROM c.version),
        (SELECT array_agg(m.name ORDER BY m.name)
           FROM ir_module_module m
          WHERE %(check_versions)s
            AND m.state = 'installed'
            AND m.name <> ALL(%(names)s::text[]))
"""

_REMEDIES = {
    REASON_MODULES_IN_TRANSITION: (
        "a module install/upgrade/uninstall is in progress in another process. "
        "If none is running, the last one was interrupted: re-run it (-u) from "
        "the upgrading process"
    ),
    REASON_PARTIAL_UPDATE: (
        "Odoo's base.partially_updated_database flag is set: a previous upgrade "
        "stopped part-way and the next registry load would resume it. Re-run "
        "the upgrade (-u); the job worker will not perform it"
    ),
    REASON_VERSION_SKEW: (
        "this worker's code and the database disagree on module versions. Run "
        "the upgrade (-u) for these modules, or deploy matching code to the "
        "worker. JOB_WORKER_UPGRADE_GATE_VERSION_CHECK=0 skips this check"
    ),
    REASON_PROBE_BLOCKED: (
        "the gate's probe could not read the module table within its lock "
        "timeout; a module upgrade holding a schema lock is the usual cause"
    ),
}


def addon_code_versions():
    """Return ``{module name: version}`` for every module on the addons path.

    Versions are in the canonical form Odoo stores in
    ``ir_module_module.latest_version``.
    """
    from odoo.modules.module import Manifest

    return {
        manifest.name: manifest.version for manifest in Manifest.all_addon_manifests()
    }


def probe(cr, code_versions):
    """Run the gate's probe query on ``cr`` and return a :class:`GateState`.

    ``code_versions`` maps module names to their code version; ``None``
    disables the version check. The caller owns the cursor, its timeouts and
    its transaction.
    """
    names = sorted(code_versions or {})
    cr.execute(
        _PROBE_QUERY,
        {
            "check_versions": code_versions is not None,
            "names": names,
            "versions": [code_versions[name] for name in names],
        },
    )
    transient, partial, skew, missing = cr.fetchone()
    missing = tuple(missing or ())
    if transient:
        return GateState(False, REASON_MODULES_IN_TRANSITION, tuple(transient), missing)
    if partial:
        return GateState(False, REASON_PARTIAL_UPDATE, (), missing)
    if skew:
        return GateState(False, REASON_VERSION_SKEW, tuple(skew), missing)
    return GateState(True, None, (), missing)


def _format_duration(seconds):
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def _format_details(details):
    if not details:
        return ""
    shown = ", ".join(details[:_MAX_LOGGED_DETAILS])
    hidden = len(details) - _MAX_LOGGED_DETAILS
    if hidden > 0:
        shown += f", … (+{hidden} more)"
    return f" [{shown}]"


class UpgradeGate:
    """Cached, logged view of :func:`probe` for one database.

    One instance per database is shared by the runner (before the first
    registry load) and the worker loop (before registry reloads and job
    acquisition), so a pause is measured once however it is reached. The
    supervisor thread reads :meth:`is_overdue` for the health signal.
    """

    # How often a pause that has not ended is logged again, in seconds.
    still_paused_log_interval_seconds = 300

    def __init__(
        self,
        db_name,
        enabled=True,
        version_check=True,
        interval_seconds=5,
        unhealthy_after_seconds=3600,
        lock_timeout_ms=2000,
        statement_timeout_ms=5000,
        code_versions=None,
        connection_factory=None,
    ):
        self.db_name = db_name
        self.enabled = enabled
        self.version_check = version_check
        self.interval_seconds = max(0.1, float(interval_seconds))
        self.unhealthy_after_seconds = max(0.0, float(unhealthy_after_seconds))
        self.lock_timeout_ms = int(lock_timeout_ms)
        self.statement_timeout_ms = int(statement_timeout_ms)
        self._code_versions = code_versions
        self._connection_factory = connection_factory or self._default_connection
        self._clock = time.monotonic
        self._state = OPEN
        self._checked_at = None
        self._closed_since = None
        self._last_reminder_at = None
        self._overdue_logged = False
        self._missing_warned = set()

    def is_open(self):
        return self.check().open

    def check(self, force=False):
        """Return the current :class:`GateState`, probing at most once per interval."""
        if not self.enabled:
            return OPEN
        now = self._clock()
        if (
            not force
            and self._checked_at is not None
            and now - self._checked_at < self.interval_seconds
        ):
            return self._state
        state = self._probe_once()
        self._checked_at = now
        self._record(state, now)
        return state

    def seconds_closed(self):
        """How long the gate has been closed, in seconds; ``0.0`` when open."""
        if self._closed_since is None:
            return 0.0
        return max(0.0, self._clock() - self._closed_since)

    def is_overdue(self):
        """True once a pause has outlasted ``unhealthy_after_seconds``."""
        if not self.unhealthy_after_seconds or self._closed_since is None:
            return False
        return self.seconds_closed() >= self.unhealthy_after_seconds

    def _default_connection(self):
        """A short-lived psycopg2 connection of the probe's own.

        Deliberately not an Odoo cursor: Odoo logs every failed statement at
        ERROR with its full SQL, and a probe blocked by an upgrade's schema
        lock is an expected outcome, repeated every interval for as long as
        the upgrade runs.
        """
        import odoo

        connection_info = dict(odoo.sql_db.connection_info_for(self.db_name)[1])
        # statement_timeout bounds the query, not connection establishment.
        connection_info.setdefault("connect_timeout", 5)
        return psycopg2.connect(**connection_info)

    def _versions(self):
        if not self.version_check:
            return None
        if self._code_versions is None:
            self._code_versions = addon_code_versions()
        return self._code_versions

    def _probe_once(self):
        connection = self._connection_factory()
        try:
            cr = connection.cursor()
            try:
                # SET LOCAL: the timeouts end with this read-only transaction,
                # which is rolled back below.
                cr.execute("SET LOCAL lock_timeout = %s", (self.lock_timeout_ms,))
                cr.execute(
                    "SET LOCAL statement_timeout = %s", (self.statement_timeout_ms,)
                )
                return probe(cr, self._versions())
            finally:
                cr.close()
        except psycopg2.Error as error:
            pgcode = getattr(error, "pgcode", None)
            if pgcode not in _BLOCKED_PGCODES:
                raise
            return GateState(False, REASON_PROBE_BLOCKED, (pgcode,))
        finally:
            try:
                connection.rollback()
            finally:
                connection.close()

    def _record(self, state, now):
        self._warn_missing(state.missing)
        previous = self._state
        self._state = state
        if state.open:
            if not previous.open:
                _logger.info(
                    "Job worker resumed for database %s after %s; reloading "
                    "the registry.",
                    self.db_name,
                    _format_duration(now - self._closed_since),
                )
            self._closed_since = None
            self._last_reminder_at = None
            self._overdue_logged = False
            return
        if previous.open:
            self._closed_since = now
            self._last_reminder_at = now
        if previous.open or previous.reason != state.reason:
            _logger.warning(
                "Job worker paused for database %s: %s%s. Not loading the "
                "registry or taking new jobs until this clears: %s.",
                self.db_name,
                state.reason,
                _format_details(state.details),
                _REMEDIES[state.reason],
            )
            return
        paused_for = now - self._closed_since
        if self.is_overdue() and not self._overdue_logged:
            self._overdue_logged = True
            _logger.error(
                "Job worker for database %s has been paused for %s (%s%s); "
                "reporting unhealthy. If no install/upgrade is running, the "
                "database is stuck: %s.",
                self.db_name,
                _format_duration(paused_for),
                state.reason,
                _format_details(state.details),
                _REMEDIES[state.reason],
            )
            return
        if now - self._last_reminder_at >= self.still_paused_log_interval_seconds:
            self._last_reminder_at = now
            _logger.info(
                "Job worker still paused for database %s after %s: %s%s",
                self.db_name,
                _format_duration(paused_for),
                state.reason,
                _format_details(state.details),
            )

    def _warn_missing(self, missing):
        new = [name for name in missing if name not in self._missing_warned]
        if not new:
            return
        self._missing_warned.update(new)
        _logger.warning(
            "Module(s) installed in database %s are not on this worker's "
            "addons_path, so their versions cannot be checked and jobs for "
            "their models will fail:%s",
            self.db_name,
            _format_details(tuple(new)),
        )
