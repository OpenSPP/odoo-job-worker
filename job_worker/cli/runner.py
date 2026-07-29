import logging
import os
import signal
import sys
import threading
import time
from contextlib import closing, suppress

import psycopg2

import odoo

from .heartbeat import heartbeat_file_path, write_heartbeat
from .worker import QueueWorker

_logger = logging.getLogger(__name__)

# Advisory lock ID for runner exclusivity. Distinct from OCA's
# queue_job runner lock (2293787760715711918) to avoid conflicts.
PG_ADVISORY_LOCK_ID = 7283946150382917643

# Default technical name of this addon, used to check whether
# the module is installed in a given database.
_MODULE_NAME = "job_worker"


class CompositeStopEvent:
    """Combines a global stop event and a per-database stop event.

    QueueWorker checks ``stop_event.is_set()`` in its main loop.
    This class returns True from ``is_set()`` when *either* constituent
    event is set, allowing both global shutdown and per-DB removal to
    stop a worker without modifying QueueWorker itself.
    """

    def __init__(self, global_event, local_event):
        self._global_event = global_event
        self._local_event = local_event

    def is_set(self):
        return self._global_event.is_set() or self._local_event.is_set()

    def set(self):
        self._global_event.set()
        self._local_event.set()

    def wait(self, timeout=None):
        """Wait on the global event.

        The global event is the one set by the supervisor loop, so
        delegating to it gives the correct blocking behavior for the
        supervisor's ``stop_event.wait(timeout=10)`` call.
        """
        return self._global_event.wait(timeout=timeout)


def _get_database_names():
    """Return list of database names from Odoo config or auto-discovery."""
    db_names = odoo.tools.config["db_name"]
    if db_names:
        return [name.strip() for name in db_names.split(",") if name.strip()]
    return odoo.service.db.list_dbs(True)


def _database_has_module(db_name, module_name=_MODULE_NAME):
    """Check whether *module_name* is installed in *db_name*.

    Opens a lightweight psycopg2 connection (autocommit) and runs two
    queries: one to verify the database contains ``ir_module_module``
    (i.e. is an Odoo database), and another to check the module state.
    The connection is always closed, even on error.
    """
    try:
        connection_info = odoo.sql_db.connection_info_for(db_name)[1]
        conn = psycopg2.connect(**connection_info)
    except Exception:
        _logger.debug("Cannot connect to database %s", db_name, exc_info=True)
        return False
    try:
        conn.autocommit = True
        with closing(conn.cursor()) as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_tables WHERE tablename = %s",
                ("ir_module_module",),
            )
            if not cursor.fetchone():
                _logger.debug("%s does not appear to be an Odoo database", db_name)
                return False
            cursor.execute(
                "SELECT 1 FROM ir_module_module WHERE name = %s AND state = %s",
                (module_name, "installed"),
            )
            if not cursor.fetchone():
                _logger.debug(
                    "%s is not installed in database %s", module_name, db_name
                )
                return False
        return True
    except Exception:
        _logger.debug("Error checking module in %s", db_name, exc_info=True)
        return False
    finally:
        conn.close()


def _try_advisory_lock(db_name):
    """Attempt to acquire a PostgreSQL advisory lock for *db_name*.

    Returns the connection (kept alive to hold the lock) on success,
    or ``None`` on failure (connection is closed).
    """
    try:
        connection_info = odoo.sql_db.connection_info_for(db_name)[1]
        conn = psycopg2.connect(**connection_info)
    except Exception:
        _logger.debug("Cannot connect for advisory lock on %s", db_name, exc_info=True)
        return None
    try:
        conn.autocommit = True
        with closing(conn.cursor()) as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (PG_ADVISORY_LOCK_ID,))
            if cursor.fetchone()[0]:
                return conn
        _logger.debug("Advisory lock not acquired for %s", db_name)
        conn.close()
        return None
    except Exception:
        _logger.debug("Error acquiring advisory lock for %s", db_name, exc_info=True)
        conn.close()
        return None


class QueueJobRunner:
    """Supervisor that discovers databases and spawns per-DB worker threads.

    Each database gets its own ``QueueWorker`` running in a daemon thread.
    The runner monitors thread health, restarts crashed workers (with
    quarantine for repeat offenders), and supports hot addition/removal
    of databases via periodic re-discovery.
    """

    def __init__(
        self,
        database_names=None,
        discovery_interval_seconds=60,
        use_advisory_lock=True,
        maximum_consecutive_failures=5,
        failure_window_seconds=300,
        join_timeout_seconds=30,
        worker_stall_timeout_seconds=120,
        worker_keyword_arguments=None,
        heartbeat_file=None,
    ):
        self.database_names = database_names
        self.discovery_interval_seconds = discovery_interval_seconds
        self.use_advisory_lock = use_advisory_lock
        self.maximum_consecutive_failures = maximum_consecutive_failures
        self.failure_window_seconds = failure_window_seconds
        self.join_timeout_seconds = join_timeout_seconds
        # A worker whose main loop has not advanced `last_progress` within this
        # many seconds is treated as hung. A blocked Python thread cannot be
        # killed, so the only reliable recovery is to exit the process and let
        # the container's restart policy bring it back. 0 disables the watchdog.
        self.worker_stall_timeout_seconds = worker_stall_timeout_seconds
        self.worker_keyword_arguments = worker_keyword_arguments or {}
        self._heartbeat_file = heartbeat_file or heartbeat_file_path()

        self.stop_event = threading.Event()
        self._worker_threads = {}
        self._worker_instances = {}
        self._per_database_stop_events = {}
        self._advisory_lock_connections = {}
        self._failure_timestamps = {}
        self._quarantined_databases = set()

    @classmethod
    def from_environ_or_config(cls):
        """Create a runner from Odoo config values."""
        db_names_raw = odoo.tools.config.get("db_name", "")
        database_names = None
        if db_names_raw:
            if isinstance(db_names_raw, list):
                database_names = [name.strip() for name in db_names_raw if name.strip()]
            else:
                database_names = [
                    name.strip() for name in db_names_raw.split(",") if name.strip()
                ]
        concurrency = int(os.environ.get("QUEUE_JOB_CONCURRENCY", "2"))
        worker_kwargs = {"concurrency": concurrency}
        if "QUEUE_JOB_STATEMENT_TIMEOUT" in os.environ:
            worker_kwargs["statement_timeout_seconds"] = int(
                os.environ["QUEUE_JOB_STATEMENT_TIMEOUT"]
            )
        if "QUEUE_JOB_LOCK_TIMEOUT" in os.environ:
            worker_kwargs["lock_timeout_seconds"] = int(
                os.environ["QUEUE_JOB_LOCK_TIMEOUT"]
            )
        runner_kwargs = {}
        if "QUEUE_JOB_WORKER_STALL_TIMEOUT" in os.environ:
            runner_kwargs["worker_stall_timeout_seconds"] = int(
                os.environ["QUEUE_JOB_WORKER_STALL_TIMEOUT"]
            )
        # Advisory lock is on by default (serialises supervisors per DB).
        # Stress tests intentionally run multiple supervisors against one
        # DB to exercise inter-process SKIP LOCKED contention and set
        # this to 0.
        advisory_lock_env = (
            os.environ.get("QUEUE_JOB_RUNNER_USE_ADVISORY_LOCK", "1").strip().lower()
        )
        use_advisory_lock = advisory_lock_env not in ("0", "false", "no", "off")
        return cls(
            database_names=database_names,
            use_advisory_lock=use_advisory_lock,
            worker_keyword_arguments=worker_kwargs,
            **runner_kwargs,
        )

    def run(self):
        """Main supervisor loop."""
        self._setup_signal_handlers()
        last_discovery = 0  # force immediate first discovery

        while not self.stop_event.is_set():
            now = time.monotonic()
            if (
                self.discovery_interval_seconds > 0
                and now - last_discovery >= self.discovery_interval_seconds
            ):
                self.discover_databases()
                last_discovery = time.monotonic()

            self._check_thread_health()
            self._check_worker_progress()
            self._update_heartbeat()
            self.stop_event.wait(timeout=10)

        self._cleanup()

    def stop(self):
        """Signal all workers and the supervisor loop to stop."""
        _logger.info("Graceful stop requested")
        self.stop_event.set()

    def discover_databases(self):
        """Find databases, check for module, acquire locks, manage workers."""
        self._quarantined_databases.clear()
        database_names = self.database_names or _get_database_names()
        active_databases = set()

        for db_name in sorted(database_names):
            try:
                if not _database_has_module(db_name):
                    continue
                active_databases.add(db_name)
                if db_name in self._worker_threads:
                    continue
                if self.use_advisory_lock:
                    lock_connection = _try_advisory_lock(db_name)
                    if lock_connection is None:
                        _logger.warning(
                            "Advisory lock failed for %s, skipping", db_name
                        )
                        continue
                    self._advisory_lock_connections[db_name] = lock_connection
                self._start_worker_thread(db_name)
            except Exception:
                _logger.exception("Discovery failed for database %s, skipping", db_name)

        # Hot-remove databases that disappeared
        for db_name in list(self._worker_threads):
            if db_name not in active_databases:
                _logger.info(
                    "Database %s no longer discovered, stopping worker", db_name
                )
                self._stop_worker_thread(db_name)

    def _start_worker_thread(self, db_name):
        """Create and start a worker thread for *db_name*."""
        local_event = threading.Event()
        self._per_database_stop_events[db_name] = local_event
        composite = CompositeStopEvent(self.stop_event, local_event)

        thread = threading.Thread(
            target=self._worker_thread_target,
            args=(db_name, composite),
            name=f"job-worker-{db_name}",
            daemon=True,
        )
        self._worker_threads[db_name] = thread
        thread.start()
        _logger.info("Started worker thread for database %s", db_name)

    def _stop_worker_thread(self, db_name):
        """Signal and join the worker thread for *db_name*."""
        local_event = self._per_database_stop_events.pop(db_name, None)
        if local_event:
            local_event.set()
        thread = self._worker_threads.pop(db_name, None)
        if thread:
            thread.join(timeout=self.join_timeout_seconds)
            if thread.is_alive():
                _logger.warning(
                    "Worker thread for %s did not stop within %ds",
                    db_name,
                    self.join_timeout_seconds,
                )
        self._worker_instances.pop(db_name, None)
        lock_conn = self._advisory_lock_connections.pop(db_name, None)
        if lock_conn:
            try:
                lock_conn.close()
            except Exception:
                _logger.debug(
                    "Error closing advisory lock connection for %s",
                    db_name,
                    exc_info=True,
                )
        self._failure_timestamps.pop(db_name, None)

    def _check_thread_health(self):
        """Restart dead threads unless quarantined or stopping."""
        for db_name in list(self._worker_threads):
            if self.stop_event.is_set():
                return
            thread = self._worker_threads[db_name]
            if thread.is_alive():
                continue
            if db_name in self._quarantined_databases:
                _logger.debug("Database %s is quarantined, not restarting", db_name)
                continue
            _logger.warning("Worker for %s is dead, restarting", db_name)
            self._record_failure(db_name)
            if db_name in self._quarantined_databases:
                continue
            # Remove old thread entry and start fresh
            self._worker_threads.pop(db_name, None)
            self._worker_instances.pop(db_name, None)
            self._per_database_stop_events.pop(db_name, None)
            self._start_worker_thread(db_name)

    def _check_worker_progress(self):
        """Recycle a worker whose main loop has stalled (hung but alive).

        ``_check_thread_health`` only restarts *dead* threads. A worker
        blocked in a DB call or a wedged loop stays ``is_alive()`` yet stops
        advancing its ``last_progress`` timestamp — a "zombie worker" that
        drains nothing while the process (and ``restart: always``) sees
        nothing wrong. This is the preprod 2026-07-13 failure mode.

        A blocked Python thread cannot be force-killed, so the only reliable
        recovery is to exit the process and let the container restart policy
        bring every worker back fresh.
        """
        if self.worker_stall_timeout_seconds <= 0:
            return
        now = time.monotonic()
        for db_name in list(self._worker_instances):
            if self.stop_event.is_set():
                return
            worker = self._worker_instances.get(db_name)
            thread = self._worker_threads.get(db_name)
            if worker is None or thread is None or not thread.is_alive():
                continue
            age = now - worker.last_progress
            if age > self.worker_stall_timeout_seconds:
                self._terminate_stalled_worker(db_name, age)

    def _stamp_stalled_jobs(self, db_name, age):
        """Record on the in-flight rows WHY they are about to be orphaned.

        Without this, the jobs this process is holding are reclaimed with
        ``WorkerDiedJobError`` and an otherwise empty diagnosis — byte-for-byte
        what an OOM kill leaves behind. The two have opposite remedies (lower
        the memory ceiling / chunk size vs. find the blocked query), and on
        preprod the ambiguity sent the investigation after ``limit_memory_hard``
        for hours while the real cause was this watchdog. Naming the stall in
        ``exc_info`` makes the difference readable straight off the job row.

        Best-effort and strictly bounded: a stalled worker often means a wedged
        database, so this takes its own connection with a short
        ``statement_timeout`` and never blocks the exit it precedes.
        """
        worker = self._worker_instances.get(db_name)
        if worker is None:
            return
        reason = (
            f"WorkerStalledError: the worker made no progress for {age:.0f}s "
            f"(limit {self.worker_stall_timeout_seconds}s) and the process was "
            "terminated so the restart policy could recover it. This is NOT an "
            "OOM kill — look for a blocked query or a wedged loop rather than "
            "limit_memory_hard."
        )
        conn = None
        try:
            connection_info = odoo.sql_db.connection_info_for(db_name)[1]
            conn = psycopg2.connect(**connection_info)
            conn.autocommit = True
            with closing(conn.cursor()) as cursor:
                cursor.execute("SET statement_timeout = 5000")
                cursor.execute(
                    "UPDATE queue_job SET exc_info = %s"
                    " WHERE state = 'started' AND worker_id = %s",
                    (reason, worker.worker_uuid),
                )
                _logger.error(
                    "Stamped %s in-flight job(s) on %s as stalled (not OOM)",
                    cursor.rowcount,
                    db_name,
                )
        except Exception:
            # Never let diagnostics keep a hung process alive.
            _logger.warning(
                "Could not stamp in-flight jobs on %s before exiting",
                db_name,
                exc_info=True,
            )
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()

    def _terminate_stalled_worker(self, db_name, age):
        """Exit the process so a hung worker is recovered by the restart policy.

        Isolated in its own method so tests can assert the escalation without
        actually killing the test process.
        """
        _logger.error(
            "Worker for %s has not made progress in %.0fs (limit %ss); "
            "exiting process for restart-policy recovery",
            db_name,
            age,
            self.worker_stall_timeout_seconds,
        )
        self._stamp_stalled_jobs(db_name, age)
        # os._exit bypasses normal interpreter shutdown, so buffered log
        # records and stdio would be lost — flush them first, or the
        # diagnostic above never reaches the logs.
        sys.stdout.flush()
        sys.stderr.flush()
        logging.shutdown()
        os._exit(1)

    def _record_failure(self, db_name):
        """Track a failure timestamp and quarantine if threshold exceeded."""
        now = time.monotonic()
        timestamps = self._failure_timestamps.setdefault(db_name, [])
        timestamps.append(now)
        # Prune old timestamps outside the failure window
        cutoff = now - self.failure_window_seconds
        self._failure_timestamps[db_name] = [ts for ts in timestamps if ts >= cutoff]
        if len(self._failure_timestamps[db_name]) >= self.maximum_consecutive_failures:
            _logger.error(
                "Database %s quarantined after %d failures in %ds",
                db_name,
                len(self._failure_timestamps[db_name]),
                self.failure_window_seconds,
            )
            self._quarantined_databases.add(db_name)

    def _update_heartbeat(self):
        """Refresh the heartbeat file unless the worker fleet is degraded.

        The heartbeat is written on every healthy supervisor iteration, so
        the freshness of the file reflects that the loop is alive *and*
        iterating.  When the fleet is degraded the heartbeat is left to go
        stale so an external healthcheck reports the container unhealthy.

        This is a best-effort side channel: any failure — an unwritable
        path, or a transient error while inspecting fleet state that worker
        threads mutate concurrently — is logged and swallowed.  It must
        never take down the supervisor loop.
        """
        try:
            if self._fleet_is_degraded():
                return
            write_heartbeat(self._heartbeat_file)
        except Exception:
            _logger.warning(
                "Could not update heartbeat file %s",
                self._heartbeat_file,
                exc_info=True,
            )

    def _fleet_is_degraded(self):
        """Return True if any database's worker is failing past the threshold.

        A quarantined database is degraded by definition.  We also treat a
        database whose recent failure count has reached the quarantine
        threshold as degraded even when it is not currently quarantined:
        ``discover_databases`` clears the quarantine set on every pass, but
        ``_failure_timestamps`` survives, so checking the failure history
        keeps the signal stable for a persistently broken database instead
        of flapping healthy after each rediscovery.
        """
        if self._quarantined_databases:
            return True
        cutoff = time.monotonic() - self.failure_window_seconds
        # Snapshot the values: a crashing worker thread may add a key to
        # _failure_timestamps via _record_failure concurrently, and iterating
        # the live dict would raise "dictionary changed size during iteration".
        # This mirrors the list(...) guarding used in _check_thread_health.
        for timestamps in list(self._failure_timestamps.values()):
            recent = sum(1 for ts in timestamps if ts >= cutoff)
            if recent >= self.maximum_consecutive_failures:
                return True
        return False

    def _cleanup(self):
        """Stop all workers and close all lock connections."""
        for db_name in list(self._worker_threads):
            self._stop_worker_thread(db_name)

    def _worker_thread_target(self, db_name, composite_stop_event):
        """Entry point for each worker thread.

        Pre-loads the Odoo registry before starting the worker loop so
        that module import errors surface as clear startup failures
        rather than being masked as job execution errors.
        """
        try:
            _logger.info("Worker starting for database %s", db_name)
            _logger.info("Loading registry for database %s", db_name)
            from odoo.orm.registry import Registry

            registry = Registry(db_name)
            registry.check_signaling()
            _logger.info("Registry loaded for database %s", db_name)
            worker = QueueWorker(
                db_name,
                stop_event=composite_stop_event,
                **self.worker_keyword_arguments,
            )
            # Publish the instance so the supervisor's progress watchdog can
            # read its ``last_progress`` liveness timestamp.
            self._worker_instances[db_name] = worker
            worker.run()
            _logger.info("Worker stopped for database %s", db_name)
        except Exception:
            _logger.exception("Worker crashed for database %s", db_name)
            self._record_failure(db_name)
        finally:
            self._worker_instances.pop(db_name, None)

    def _setup_signal_handlers(self):
        """Install signal handlers for graceful shutdown."""
        if threading.current_thread() is not threading.main_thread():
            _logger.warning("Cannot install signal handlers from non-main thread")
            return
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        """Signal callback that triggers a graceful stop."""
        _logger.info("Received signal %s, stopping", signum)
        self.stop()
