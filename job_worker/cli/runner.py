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
        database_unhealthy_after_seconds=300,
        registry_load_backoff_seconds=1,
        registry_load_backoff_cap_seconds=60,
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
        # A database error no longer kills a worker thread, so neither the
        # quarantine set nor the failure history can see a database that is
        # persistently unreachable. A database stuck this long -- retrying its
        # registry load, or recovering inside the main loop -- is reported
        # degraded, which stales the heartbeat file and turns the container
        # unhealthy. Without it, the fix below would trade a loud crash loop
        # for a silent worker that serves nothing and looks fine.
        self.database_unhealthy_after_seconds = database_unhealthy_after_seconds
        self.registry_load_backoff_seconds = max(
            0.05, float(registry_load_backoff_seconds)
        )
        self.registry_load_backoff_cap_seconds = max(
            self.registry_load_backoff_seconds,
            float(registry_load_backoff_cap_seconds),
        )

        self.stop_event = threading.Event()
        self._worker_threads = {}
        self._worker_instances = {}
        self._per_database_stop_events = {}
        self._advisory_lock_connections = {}
        self._failure_timestamps = {}
        self._quarantined_databases = set()
        # Thread objects whose death has already been counted, so that one
        # corpse contributes exactly one failure timestamp however many
        # supervisor ticks it sits in ``_worker_threads`` for. Entries are
        # discarded with the thread they refer to.
        self._deaths_counted = set()
        # db_name -> monotonic timestamp when its registry load first failed
        # with a database error. Cleared once the registry loads.
        self._registry_load_retry_since = {}

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
        self._expire_quarantines()
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

    def _forget_thread(self, db_name):
        """Drop the thread entry for *db_name*, and return it.

        Kept in one place so the ``_deaths_counted`` bookkeeping cannot
        outlive the thread it refers to: every removal from
        ``_worker_threads`` goes through here.
        """
        thread = self._worker_threads.pop(db_name, None)
        if thread is not None:
            self._deaths_counted.discard(thread)
        return thread

    def _stop_worker_thread(self, db_name):
        """Signal and join the worker thread for *db_name*."""
        local_event = self._per_database_stop_events.pop(db_name, None)
        if local_event:
            local_event.set()
        thread = self._forget_thread(db_name)
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
        # The quarantine has to go with the history it was armed from. Left
        # behind, it keeps ``_fleet_is_degraded`` reporting a database this
        # process no longer serves, withholding the heartbeat past the
        # healthcheck's staleness threshold for no live fault.
        self._quarantined_databases.discard(db_name)

    def _check_thread_health(self):
        """Restart dead threads unless quarantined or stopping.

        A corpse is counted **once**. It has to be, because a dead thread is
        not removed from ``_worker_threads`` while its database is
        quarantined: without ``_deaths_counted`` the same corpse is re-counted
        on every tick that finds the quarantine lifted, and a single stamp per
        discovery pass is enough to keep the failure window full forever — the
        database is then never retried, which is #30 in a subtler shape than
        the one ``_expire_quarantines`` fixes. Counting once also stops an
        exception crash being stamped twice (here and in
        ``_worker_thread_target``), which made ``maximum_consecutive_failures``
        quarantine after half as many real crashes as it says.
        """
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
            if thread not in self._deaths_counted:
                # A clean ``return`` from the worker records nothing itself,
                # so the health check is the only place that can count it.
                self._deaths_counted.add(thread)
                self._record_failure(db_name)
            if db_name in self._quarantined_databases:
                continue
            # Remove old thread entry and start fresh
            self._forget_thread(db_name)
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

        ``os._exit`` orphans the in-flight jobs of **every** database this
        process serves, not just the stalled one, so every database with a live
        worker is stamped. The others get a reason naming the database that
        actually stalled — without it their jobs stay OOM-ambiguous, which is
        the exact confusion this exists to remove.

        Best-effort and strictly bounded: a stalled worker often means a wedged
        database, so each stamp takes its own connection with a short
        ``connect_timeout`` and ``statement_timeout``, and never blocks the exit
        it precedes.
        """
        stalled_reason = (
            f"WorkerStalledError: the worker made no progress for {age:.0f}s "
            f"(limit {self.worker_stall_timeout_seconds}s) and the process was "
            "terminated so the restart policy could recover it. This is NOT an "
            "OOM kill — look for a blocked query or a wedged loop rather than "
            "limit_memory_hard."
        )
        collateral_reason = (
            "WorkerStalledError: this job was orphaned because the worker for "
            f"database '{db_name}' in the same process made no progress for "
            f"{age:.0f}s (limit {self.worker_stall_timeout_seconds}s) and the "
            "process was terminated. This job's own database was not necessarily "
            "at fault, and this is NOT an OOM kill."
        )
        for target_db, worker in list(self._worker_instances.items()):
            if worker is None:
                continue
            self._stamp_one_db(
                target_db,
                worker,
                stalled_reason if target_db == db_name else collateral_reason,
            )

    def _stamp_one_db(self, db_name, worker, reason):
        """Stamp ``reason`` onto one database's in-flight rows. Never raises."""
        conn = None
        try:
            connection_info = odoo.sql_db.connection_info_for(db_name)[1]
            # statement_timeout bounds queries but NOT connection establishment,
            # which against an unroutable host can hang far longer than the exit
            # this precedes is allowed to wait.
            connection_info.setdefault("connect_timeout", 5)
            conn = psycopg2.connect(**connection_info)
            conn.autocommit = True
            with closing(conn.cursor()) as cursor:
                cursor.execute("SET statement_timeout = 5000")
                # Append rather than overwrite, for the same reason the reclaim
                # path does: a wedged database is precisely where a job has
                # already been stamped "TimeoutJobError", and that is the more
                # specific diagnosis. Overwriting destroys it.
                cursor.execute(
                    "UPDATE queue_job"
                    " SET exc_info = CASE"
                    "     WHEN exc_info IS NULL OR exc_info = '' THEN %s"
                    "     ELSE exc_info || E'\\n' || %s"
                    " END"
                    " WHERE state = 'started' AND worker_id = %s",
                    (reason, reason, worker.worker_uuid),
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

    def _recent_failure_count(self, db_name):
        """Prune failures outside the window and return how many remain.

        Pruning on *read* as well as on write is what lets a quarantine
        expire. A quarantined database records no new failures — the health
        check skips it before reaching ``_record_failure`` — so its history
        only ages, and nothing else would ever notice that it had.
        """
        timestamps = self._failure_timestamps.get(db_name)
        if not timestamps:
            return 0
        cutoff = time.monotonic() - self.failure_window_seconds
        remaining = [ts for ts in timestamps if ts >= cutoff]
        if remaining:
            self._failure_timestamps[db_name] = remaining
        else:
            self._failure_timestamps.pop(db_name, None)
        return len(remaining)

    def _expire_quarantines(self):
        """Release databases whose failures have aged out of the window.

        Runs once per discovery pass, in place of the unconditional
        ``_quarantined_databases.clear()`` this replaces. That clear looked
        like it granted a quarantined database another chance, but it did the
        opposite: with the set emptied, ``_check_thread_health`` re-counted
        the *same* dead thread on the next tick and re-armed the quarantine.
        One corpse recounted once per pass keeps the window permanently full,
        so the pruning meant to release the database could never drain and it
        was retried exactly zero times — a five-hour queue stall on dev
        payroll, 2026-08-11 (#30), long after the fault behind it had cleared.

        Expiring on the pruned history instead bounds a quarantine to at most
        one ``failure_window_seconds`` past the last genuine death — but only
        because ``_check_thread_health`` counts each corpse once. Re-counting
        it reinstates the same permanent lock one stamp at a time, so the two
        halves of the fix are not separable.
        """
        for db_name in sorted(self._quarantined_databases):
            if self._recent_failure_count(db_name) >= self.maximum_consecutive_failures:
                continue
            self._quarantined_databases.discard(db_name)
            _logger.info(
                "Quarantine expired for database %s; it will be retried",
                db_name,
            )

    def _record_failure(self, db_name):
        """Track a failure timestamp and quarantine if threshold exceeded."""
        self._failure_timestamps.setdefault(db_name, []).append(time.monotonic())
        if self._recent_failure_count(db_name) >= self.maximum_consecutive_failures:
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
        ``discover_databases`` releases a quarantine as soon as the pruned
        history drops below the threshold, and a released database is
        restarted before anything proves it healthy, so reading the failure
        history keeps the signal stable across that retry instead of
        flapping healthy on every expiry.

        Neither of those can see a database that is *unreachable* rather than
        crashing, because a database error now recovers in place and records no
        failure at all.  So a database that has been retrying its registry load
        or recovering inside the main loop for longer than
        ``database_unhealthy_after_seconds`` is degraded too — that is what
        keeps a wedged database from looking healthy while it serves nothing.
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
        threshold = self.database_unhealthy_after_seconds
        if not threshold:
            return False
        now = time.monotonic()
        # Stuck before the worker even exists (registry load), and stuck after
        # it does (the main loop). Both are snapshotted for the same reason as
        # above: worker threads mutate them concurrently.
        for started in list(self._registry_load_retry_since.values()):
            if now - started >= threshold:
                return True
        for worker in list(self._worker_instances.values()):
            if worker is None:
                continue
            if worker.seconds_in_database_error_recovery() >= threshold:
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
            if not self._load_registry(db_name, composite_stop_event):
                return
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
            # Claim this death before the supervisor can see the thread end,
            # so ``_check_thread_health`` does not stamp the same crash again.
            self._deaths_counted.add(threading.current_thread())
            self._record_failure(db_name)
        finally:
            self._worker_instances.pop(db_name, None)
            self._registry_load_retry_since.pop(db_name, None)

    def _load_registry(self, db_name, stop_event):
        """Load the Odoo registry, retrying in place on a *database* error.

        Returns True once loaded, False if asked to stop before that.

        This is the other half of "a database error must never quarantine the
        database", and it is the half the preprod incident actually went
        through. The repeated deaths there were raised from ``env.ref()`` in a
        module's ``ir.module.module`` menu-icon hook, reached during registry
        load — which happens *here*, before ``worker.run()`` is ever called. No
        guard inside ``run`` can see them: they land in this method's caller,
        which counts a failure and, five times over, quarantines the database.
        (``QueueWorker._check_registry`` cannot see them either — the in-loop
        re-check swallows every exception itself.)

        So a database error retries here instead of counting as a worker death.
        Anything else — a genuine module import error, a broken model
        definition — still propagates to the caller and is recorded, which
        keeps the original intent that startup errors surface loudly rather
        than being masked.
        """
        attempt = 0
        while not stop_event.is_set():
            try:
                _logger.info("Loading registry for database %s", db_name)
                from odoo.orm.registry import Registry

                registry = Registry(db_name)
                registry.check_signaling()
            except psycopg2.Error:
                attempt += 1
                self._registry_load_retry_since.setdefault(db_name, time.monotonic())
                delay = min(
                    self.registry_load_backoff_seconds * (2 ** (attempt - 1)),
                    self.registry_load_backoff_cap_seconds,
                )
                _logger.exception(
                    "Registry load for %s failed with a database error "
                    "(attempt %s); retrying in %.1fs. The thread stays alive, "
                    "so this is not counted as a worker death.",
                    db_name,
                    attempt,
                    delay,
                )
                stop_event.wait(delay)
                continue
            _logger.info("Registry loaded for database %s", db_name)
            self._registry_load_retry_since.pop(db_name, None)
            return True
        return False

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
