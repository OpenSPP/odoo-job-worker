import datetime
import functools
import json
import logging
import random
import select
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress

from psycopg2 import OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED

import odoo
from odoo import api, fields

from ..exception import RetryableJobError, TransientRegistryError

_logger = logging.getLogger(__name__)

# PostgreSQL concurrency error codes that should be retried
PG_CONCURRENCY_ERRORS_TO_RETRY = (
    "40001",  # serialization_failure
    "40P01",  # deadlock_detected
    "55P03",  # lock_not_available
)

# Transient errors where a control-plane op (heartbeat / acquire) should back
# off and retry on the next loop, NOT crash the worker thread. Crashing forces
# the supervisor to restart the thread and reload the Odoo registry — heavy DB
# work that, under the very load that caused the timeout, snowballs into a
# registry-reload storm. This is the concurrency set plus statement_timeout
# cancellation (57014), both now reachable because control-plane cursors carry
# SET LOCAL timeouts.
PG_TRANSIENT_CONTROL_PLANE_ERRORS = PG_CONCURRENCY_ERRORS_TO_RETRY + (
    "57014",  # query_canceled (statement_timeout)
)


def _apply_control_plane_timeouts(cr, statement_timeout_ms, lock_timeout_ms):
    """Bound a control-plane query so it can never block forever.

    ``SET LOCAL`` scopes the timeouts to the current transaction, so they
    never leak to the next borrower of this pooled connection — job
    execution cursors must stay untouched or a long-running job would be
    cancelled. A blocked heartbeat/acquire now raises (``query_canceled``
    / ``lock_not_available``) instead of hanging the worker loop
    indefinitely, which was the preprod zombie-worker root cause: with no
    timeout, a blocked main-loop query froze the heartbeat while the
    process stayed alive, so ``restart: always`` never fired.

    Values ``<= 0`` are skipped (leaves the server/session default).
    """
    if statement_timeout_ms and statement_timeout_ms > 0:
        cr.execute("SET LOCAL statement_timeout = %s", (int(statement_timeout_ms),))
    if lock_timeout_ms and lock_timeout_ms > 0:
        cr.execute("SET LOCAL lock_timeout = %s", (int(lock_timeout_ms),))


@contextmanager
def read_committed_cursor(db, statement_timeout_ms=0, lock_timeout_ms=0):
    """Context manager for READ COMMITTED isolation.

    PostgreSQL 18 has stricter serialization checks. For heartbeat
    and status updates where we want latest committed state,
    READ COMMITTED is appropriate and avoids SerializationFailure.

    No isolation restoration needed: each call creates its own cursor
    from the pool, and Odoo's cursor.__exit__ handles cleanup.
    Uses Odoo's internal cr._cnx (psycopg2 connection), which is
    stable in Odoo 19 but is a private API.

    When ``statement_timeout_ms`` / ``lock_timeout_ms`` are given, the
    query is bounded via ``SET LOCAL`` (see
    ``_apply_control_plane_timeouts``); pass them for control-plane
    (heartbeat) operations, leave them 0 for the job-execution path.
    """
    with db.cursor() as cr:
        cr._cnx.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        _apply_control_plane_timeouts(cr, statement_timeout_ms, lock_timeout_ms)
        yield cr


def _retry_db_operation(
    operation, operation_name="operation", max_retries=3, base_delay=0.1
):
    """Execute a database operation with retry on serialization errors.

    Args:
        operation: Callable that performs the DB operation
        operation_name: Name for logging
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for exponential backoff

    Returns:
        Result of the operation

    Raises:
        OperationalError: If non-retryable error or max retries exceeded
    """
    for attempt in range(max_retries):
        try:
            return operation()
        except OperationalError as err:
            if err.pgcode not in PG_CONCURRENCY_ERRORS_TO_RETRY:
                raise
            if attempt == max_retries - 1:
                _logger.error(
                    "%s failed after %d retries: %s",
                    operation_name,
                    max_retries,
                    err.pgcode,
                )
                raise

            delay = base_delay * (2**attempt) * (0.5 + random.random() * 0.5)
            _logger.warning(
                "%s failed with %s (attempt %d/%d), retry in %.3fs",
                operation_name,
                err.pgcode,
                attempt + 1,
                max_retries,
                delay,
            )
            time.sleep(delay)


def retry_on_serialization_failure(max_retries=3, base_delay=0.1):
    """Decorator to retry operations on PostgreSQL concurrency errors."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return _retry_db_operation(
                lambda: func(*args, **kwargs),
                operation_name=func.__name__,
                max_retries=max_retries,
                base_delay=base_delay,
            )

        return wrapper

    return decorator


class QueueWorker:
    def __init__(
        self,
        db_name,
        stop_event=None,
        poll_timeout=5,
        stale_after_seconds=60,
        heartbeat_interval_seconds=15,
        max_backoff_seconds=3600,
        concurrency=2,
        registry_check_interval=30,
        statement_timeout_seconds=30,
        lock_timeout_seconds=10,
        transient_registry_max_age_seconds=3600,
    ):
        self.db_name = db_name
        self.worker_uuid = str(uuid.uuid4())
        self.active_job_ids = set()
        self._active_lock = threading.Lock()
        self.stop_event = stop_event or threading.Event()
        self.poll_timeout = poll_timeout
        self.stale_after_seconds = max(1, int(stale_after_seconds))
        self.heartbeat_interval_seconds = max(1, int(heartbeat_interval_seconds))
        self.max_backoff_seconds = max(10, int(max_backoff_seconds))
        self.concurrency = max(1, int(concurrency))
        self.registry_check_interval = max(5, int(registry_check_interval))
        # How long (seconds) to keep retrying a job whose model is missing from
        # the registry WITHOUT counting attempts (a transient module
        # install/upgrade window). Past this age the model is presumed gone for
        # good and the job is failed rather than looping forever.
        self.transient_registry_max_age_seconds = max(
            0, int(transient_registry_max_age_seconds)
        )
        # Control-plane query timeouts (ms), applied via SET LOCAL to the
        # heartbeat/acquire cursors only — NOT the job-execution path. A
        # blocked main-loop query then raises instead of hanging the worker
        # forever. 0 disables (leaves the server default).
        self.control_statement_timeout_ms = max(
            0, int(statement_timeout_seconds * 1000)
        )
        self.control_lock_timeout_ms = max(0, int(lock_timeout_seconds * 1000))
        # Monotonic timestamp of the last main-loop iteration. The supervisor
        # reads this as a liveness signal: a hung loop stops advancing it even
        # though the thread stays alive (see QueueJobRunner._check_worker_progress).
        self.last_progress = time.monotonic()
        self.db = odoo.sql_db.db_connect(self.db_name)
        self._pool = ThreadPoolExecutor(
            max_workers=self.concurrency,
            thread_name_prefix=f"job-exec-{self.db_name}",
        )
        self._last_registry_check = time.monotonic()

    def _check_registry(self):
        """Check if the Odoo registry has changed and reload if needed.

        Odoo increments a database sequence when modules are installed,
        updated, or uninstalled.  ``registry.check_signaling()`` detects
        this and transparently rebuilds the registry so that newly added
        model methods (e.g. from a freshly installed module) become
        available to the worker without a manual restart.
        """
        now = time.monotonic()
        if now - self._last_registry_check < self.registry_check_interval:
            return
        self._last_registry_check = now
        try:
            from odoo.orm.registry import Registry

            registry = Registry(self.db_name)
            registry.check_signaling()
        except Exception:
            _logger.warning("Registry check failed for %s", self.db_name, exc_info=True)

    def run(self):
        """
        Main Worker Loop
        """
        try:
            # Connect to DB for Listen/Notify
            with self.db.cursor() as cr:
                cr.execute("LISTEN queue_job_wake_up")
                cr.commit()

                _logger.info("Listening for jobs (concurrency=%d)...", self.concurrency)

                while not self.stop_event.is_set():
                    # Liveness signal for the supervisor watchdog: a hung
                    # loop stops advancing this even though the thread lives.
                    self.last_progress = time.monotonic()

                    # 0. Check for registry changes (module install/update)
                    self._check_registry()

                    # 1. Process jobs until queue is empty or limit reached
                    self.process_jobs()

                    # 2. Update heartbeats for currently running jobs (if any).
                    # A transient DB timeout here must not crash the loop (see
                    # _acquire_job) — skip this cycle and retry next iteration.
                    try:
                        self.update_heartbeats()
                    except OperationalError as err:
                        if err.pgcode not in PG_TRANSIENT_CONTROL_PLANE_ERRORS:
                            raise
                        _logger.warning(
                            "Transient DB error during heartbeat update (%s); "
                            "skipping this cycle",
                            err.pgcode,
                        )

                    # 3. Wait for notification or timeout
                    conn = cr._cnx
                    if select.select([conn], [], [], self.poll_timeout) == (
                        [],
                        [],
                        [],
                    ):
                        pass
                    else:
                        conn.poll()
                        while conn.notifies:
                            notify = conn.notifies.pop(0)
                            _logger.debug("Received notification: %s", notify.channel)
        finally:
            self._pool.shutdown(wait=True, cancel_futures=True)

    @retry_on_serialization_failure(max_retries=3, base_delay=0.1)
    def update_heartbeats(self):
        """Update heartbeat for all jobs owned by this worker.

        Uses READ COMMITTED to avoid serialization conflicts with
        job completion updates on PostgreSQL 18+.
        """
        with self._active_lock:
            snapshot = list(self.active_job_ids)
        if not snapshot:
            return

        _logger.debug("Updating heartbeats for jobs: %s", snapshot)
        with read_committed_cursor(
            self.db,
            statement_timeout_ms=self.control_statement_timeout_ms,
            lock_timeout_ms=self.control_lock_timeout_ms,
        ) as cr:
            cr.execute(
                "UPDATE queue_job SET heartbeat = NOW()"
                " WHERE id = ANY(%s) AND worker_id = %s",
                (snapshot, self.worker_uuid),
            )
            cr.commit()

    def process_jobs(self):
        """
        Submit jobs to the thread pool and drain the queue.

        Fills up to ``concurrency`` pool slots, then waits for active
        jobs to complete before retrying.  Returns when no acquirable
        jobs remain and no jobs are active, or when ``concurrency``
        slots are occupied (so the main loop can do heartbeats and
        wait for notifications).

        The TOCTOU gap between checking active count and adding a job is
        benign: only the main thread adds to active_job_ids, pool threads
        only remove. The worst case is missing one fill cycle.
        """
        while not self.stop_event.is_set():
            with self._active_lock:
                if len(self.active_job_ids) >= self.concurrency:
                    break
            try:
                job_id = self._acquire_job()
            except OperationalError as err:
                # A transient DB spike (a concurrency failure surviving the
                # retry wrapper, or the control-plane statement/lock timeout)
                # must not crash the thread: that forces a supervisor restart +
                # Odoo registry reload, which under the very load that caused
                # the timeout snowballs into a registry-reload storm. Back off
                # and retry on the next loop instead; non-transient errors
                # still propagate so the runner restarts the worker.
                if err.pgcode not in PG_TRANSIENT_CONTROL_PLANE_ERRORS:
                    raise
                _logger.warning(
                    "Transient DB error during job acquisition (%s); backing off",
                    err.pgcode,
                )
                job_id = None
            if not job_id:
                # No jobs acquirable right now. If jobs are still
                # running, wait for a slot to free up — the completing
                # job may unblock a channel-limited successor.
                with self._active_lock:
                    has_active = bool(self.active_job_ids)
                if not has_active:
                    break
                time.sleep(0.05)
                continue
            with self._active_lock:
                self.active_job_ids.add(job_id)
            self._pool.submit(self._execute_and_cleanup, job_id)

    @retry_on_serialization_failure(max_retries=3, base_delay=0.05)
    def _acquire_job(self):
        """Acquire one job using SKIP LOCKED and commit the 'started' state.

        Returns the job ID or None if no job is available. A transient DB
        error (a concurrency failure that survives the retry wrapper, or a
        statement/lock timeout) propagates and is backed off by the caller
        (``process_jobs``) rather than crashing the worker thread.

        Uses READ COMMITTED isolation and a serialization-failure retry
        wrapper: the acquire query has a WITH clause whose aggregation
        subqueries (fresh_running_counts, recent_starts_counts) read many
        rows, and under REPEATABLE READ contention these snapshots can race
        with concurrent commits and surface as ``SerializationFailure`` —
        even though the FOR UPDATE SKIP LOCKED clause itself is conflict-free.
        Control-plane statement/lock timeouts are applied so a blocked
        acquire cannot hang the loop indefinitely.
        """
        with read_committed_cursor(
            self.db,
            statement_timeout_ms=self.control_statement_timeout_ms,
            lock_timeout_ms=self.control_lock_timeout_ms,
        ) as cr:
            job_id = self.acquire_job_lock(cr)
            # Commit unconditionally: acquire_job_lock may have marked a
            # reclaim-exhausted job 'failed' and returned None, and that write
            # must persist rather than roll back when the cursor closes.
            cr.commit()
            return job_id

    def _execute_and_cleanup(self, job_id):
        """Pool thread entry point. Executes a single job and cleans up.

        Always removes the job from active_job_ids, even on failure.
        """
        try:
            with self.db.cursor() as cr:
                try:
                    self.execute_job(cr, job_id)
                finally:
                    # Prevent Odoo's cursor context manager from committing
                    # a potentially poisoned transaction.
                    cr.rollback()
        except Exception:
            _logger.exception("Unexpected error executing job %s", job_id)
        finally:
            with self._active_lock:
                self.active_job_ids.discard(job_id)

    def acquire_job_lock(self, cr):
        """
        Execute the SQL query to find a job.
        Includes:
        - Stale job recovery (heartbeat < NOW() - 60s)
        - Concurrency throttling (per channel)
        - RPS (Rate Limiting) per channel
        """
        stale_seconds = self.stale_after_seconds
        query = """
            WITH fresh_running_counts AS (
                SELECT channel, COUNT(*) as running
                FROM queue_job
                WHERE state = 'started'
                  AND heartbeat > NOW() - %s * INTERVAL '1 second'
                GROUP BY channel
            ),
            recent_starts_counts AS (
                -- Count jobs started in the last second to enforce RPS
                SELECT channel, COUNT(*) as started_count
                FROM queue_job
                WHERE (state = 'started' OR state = 'done')
                  AND (heartbeat > NOW() - INTERVAL '1 second'
                       OR write_date > NOW() - INTERVAL '1 second')
                GROUP BY channel
            ),
            limits AS (
                SELECT name, "limit", rate_limit FROM queue_limit
            )
            SELECT j.id, j.state, j.attempts, j.max_retries, j.graph_uuid
            FROM queue_job j
            LEFT JOIN fresh_running_counts rc ON j.channel = rc.channel
            LEFT JOIN recent_starts_counts rsc ON j.channel = rsc.channel
            LEFT JOIN limits l ON j.channel = l.name
            WHERE (
                j.state = 'pending'
                OR (
                    j.state = 'started'
                    AND (
                        j.heartbeat IS NULL
                        OR j.heartbeat < NOW() - %s * INTERVAL '1 second'
                    )
                )
            )
            AND (j.scheduled_at IS NULL OR j.scheduled_at <= NOW())
            -- Enforce Concurrency Limit
            AND (COALESCE(rc.running, 0) < COALESCE(l."limit", 1))
            -- Enforce RPS (Rate Limit)
            AND (l.rate_limit IS NULL OR l.rate_limit = 0
                 OR COALESCE(rsc.started_count, 0) < l.rate_limit)
            ORDER BY j.priority ASC, j.scheduled_at ASC, j.id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """
        cr.execute(query, (stale_seconds, stale_seconds))
        res = cr.fetchone()
        if not res:
            return None
        job_id, prior_state, attempts, max_retries, graph_uuid = res

        if prior_state != "started":
            # Fresh 'pending' pickup. ``attempts`` is owned by the execution
            # and timeout paths; leave it untouched here.
            cr.execute(
                "UPDATE queue_job"
                " SET state = 'started', heartbeat = NOW(),"
                " worker_id = %s, write_date = NOW(),"
                " started_at = NOW()"
                " WHERE id = %s",
                (self.worker_uuid, job_id),
            )
            return job_id

        # Reclaiming a job still in 'started': the previous worker died or was
        # killed mid-execution (OOM under limit_memory_hard, the stall
        # watchdog's os._exit, a container restart) and its heartbeat went
        # stale. That kind of death raises no in-process exception and, when
        # the job has no per-attempt timeout, never trips _handle_timeout — so
        # neither normal attempt-counting path ran. Count the reclaim as a
        # failed attempt here, so a job that repeatedly kills its worker cannot
        # be reclaimed and re-run forever (the "restarts but never finishes"
        # loop): once it exhausts max_retries, fail it with a diagnostic
        # instead of running it again.
        # ``attempts`` may be NULL for rows created before the column existed;
        # coerce so the arithmetic and the > comparison below are safe.
        attempts = (attempts or 0) + 1
        # max_retries == 0 means "retry infinitely" (matches _handle_timeout).
        if max_retries and attempts > max_retries:
            reason = (
                "WorkerDiedJobError: reclaimed after the worker died or was "
                "killed mid-execution without completing (no exception, no "
                "timeout); exhausted max_retries."
            )
            cr.execute(
                "UPDATE queue_job"
                " SET state = 'failed', attempts = %s, exc_info = %s,"
                " completed_at = NOW(), worker_id = NULL, heartbeat = NULL,"
                " write_date = NOW()"
                " WHERE id = %s",
                (attempts, reason, job_id),
            )
            # Cascade the failure so dependents don't hang in 'waiting' forever
            # (mirrors handle_exception's _cascade_failure_to_dependents, but in
            # raw SQL — this control-plane path has no ORM env). Waiting
            # children first, then multi-parent graph-barrier dependents.
            cascade_reason = f"Parent job {job_id} failed (worker died)"
            cr.execute(
                "UPDATE queue_job"
                " SET state = 'failed', exc_info = %s, write_date = NOW()"
                " WHERE parent_id = %s AND state = 'waiting'",
                (cascade_reason, job_id),
            )
            if graph_uuid:
                cr.execute(
                    "UPDATE queue_job"
                    " SET state = 'failed', exc_info = %s, write_date = NOW()"
                    " WHERE graph_uuid = %s AND state = 'waiting'"
                    " AND dependency_job_ids IS NOT NULL"
                    " AND dependency_job_ids @> (%s)::jsonb",
                    (cascade_reason, graph_uuid, json.dumps([job_id])),
                )
            _logger.error(
                "Job %s exhausted max_retries (%s) after repeated worker "
                "deaths; marking failed instead of reclaiming.",
                job_id,
                attempts,
            )
            return None

        cr.execute(
            "UPDATE queue_job"
            " SET state = 'started', heartbeat = NOW(),"
            " worker_id = %s, write_date = NOW(),"
            " started_at = NOW(), attempts = %s"
            " WHERE id = %s",
            (self.worker_uuid, attempts, job_id),
        )
        _logger.warning(
            "Reclaiming stale job %s (attempt %s); previous worker left it "
            "'started' without completing.",
            job_id,
            attempts,
        )
        return job_id

    @retry_on_serialization_failure(max_retries=3, base_delay=0.1)
    def _heartbeat_job(self, job_id):
        """Update heartbeat for single job. Uses READ COMMITTED isolation."""
        with read_committed_cursor(
            self.db,
            statement_timeout_ms=self.control_statement_timeout_ms,
            lock_timeout_ms=self.control_lock_timeout_ms,
        ) as hb_cr:
            hb_cr.execute(
                """
                UPDATE queue_job
                   SET heartbeat = NOW()
                 WHERE id = %s
                   AND state = 'started'
                   AND worker_id = %s
                """,
                (job_id, self.worker_uuid),
            )
            hb_cr.commit()

    def _heartbeat_loop(
        self, job_id, stop_event, timeout=0, started_at=None, timeout_event=None
    ):
        while not stop_event.wait(self.heartbeat_interval_seconds):
            try:
                self._heartbeat_job(job_id)
            except Exception:
                _logger.exception("Failed to refresh heartbeat for job %s", job_id)
            if timeout and timeout_event is not None and started_at is not None:
                elapsed = time.monotonic() - started_at
                if elapsed >= timeout:
                    _logger.warning(
                        "Job %s exceeded timeout of %ds (elapsed: %.1fs)",
                        job_id,
                        timeout,
                        elapsed,
                    )
                    # Signal the execution thread immediately so it
                    # can discard results as soon as the method returns.
                    timeout_event.set()
                    try:
                        self._handle_timeout(job_id, timeout)
                    except Exception:
                        _logger.exception("Failed to handle timeout for job %s", job_id)
                    stop_event.set()
                    return

    @retry_on_serialization_failure(max_retries=3, base_delay=0.1)
    def _handle_timeout(self, job_id, timeout):
        """Mark a timed-out job for retry or permanent failure.

        Uses raw SQL (not ORM) for speed — this runs in the heartbeat
        thread and must complete before the execution thread's join
        timeout expires.
        """
        with read_committed_cursor(self.db) as cr:
            cr.execute(
                "SELECT state, attempts, max_retries, started_at, graph_uuid"
                " FROM queue_job WHERE id = %s",
                (job_id,),
            )
            row = cr.fetchone()
            if not row or row[0] != "started":
                return
            _state, attempts, max_retries, started_at, graph_uuid = row
            attempts += 1
            exc_info = f"TimeoutJobError: Job exceeded {timeout}s timeout"

            retry_infinitely = max_retries == 0
            retry_remaining = max_retries and attempts <= max_retries
            if retry_infinitely or retry_remaining:
                delay_seconds = min(
                    10 * (2 ** max(attempts - 1, 0)),
                    self.max_backoff_seconds,
                )
                scheduled_at = fields.Datetime.now() + datetime.timedelta(
                    seconds=delay_seconds
                )
                cr.execute(
                    "UPDATE queue_job"
                    " SET state = 'pending', attempts = %s, exc_info = %s,"
                    " scheduled_at = %s, worker_id = NULL, heartbeat = NULL,"
                    " write_date = NOW()"
                    " WHERE id = %s",
                    (attempts, exc_info, scheduled_at, job_id),
                )
                _logger.warning(
                    "Job %s timed out (attempt %s/%s). Retrying in %ds.",
                    job_id,
                    attempts,
                    max_retries,
                    delay_seconds,
                )
            else:
                now = fields.Datetime.now()
                duration = None
                if started_at:
                    duration = max(0.0, (now - started_at).total_seconds())
                cr.execute(
                    "UPDATE queue_job"
                    " SET state = 'failed', attempts = %s, exc_info = %s,"
                    " completed_at = %s, duration = %s,"
                    " scheduled_at = NULL, worker_id = NULL, heartbeat = NULL,"
                    " write_date = NOW()"
                    " WHERE id = %s",
                    (attempts, exc_info, now, duration, job_id),
                )
                # Cascade failure to waiting children
                cr.execute(
                    "UPDATE queue_job"
                    " SET state = 'failed',"
                    " exc_info = %s,"
                    " write_date = NOW()"
                    " WHERE parent_id = %s AND state = 'waiting'",
                    (f"Parent job {job_id} failed (timeout)", job_id),
                )
                # Cascade failure to multi-parent dependents (group barriers)
                if graph_uuid:
                    cr.execute(
                        """
                        UPDATE queue_job
                        SET state = 'failed',
                            exc_info = %s,
                            write_date = NOW()
                        WHERE graph_uuid = %s
                          AND state = 'waiting'
                          AND dependency_job_ids IS NOT NULL
                          AND dependency_job_ids @> (%s)::jsonb
                        """,
                        (
                            f"Parent job {job_id} failed (timeout)",
                            graph_uuid,
                            json.dumps([job_id]),
                        ),
                    )
                _logger.error(
                    "Job %s timed out permanently after %s attempts.",
                    job_id,
                    attempts,
                )
            cr.commit()

    @retry_on_serialization_failure(max_retries=3, base_delay=0.1)
    def _clear_worker_ownership(self, job_id):
        """Clear worker_id and heartbeat for stale job recovery."""
        with read_committed_cursor(self.db) as cr:
            cr.execute(
                "UPDATE queue_job SET worker_id = NULL, heartbeat = NULL WHERE id = %s",
                (job_id,),
            )
            cr.commit()

    @staticmethod
    def _restore_thread_identity(thread, dbname, uid):
        """Restore (or clear) a pooled thread's ``dbname``/``uid`` after a job.

        Threads are reused across jobs, so the job's database/user context must
        not linger on the thread once it finishes.
        """
        for attr, value in (("dbname", dbname), ("uid", uid)):
            if value is None:
                if hasattr(thread, attr):
                    delattr(thread, attr)
            else:
                setattr(thread, attr, value)

    def execute_job(self, cr, job_id):
        """
        Execute the job.
        """
        # Flush any pending state so error handling (which opens a
        # separate cursor) sees a consistent view of the job.
        cr.commit()

        # Re-open registry/environment
        admin_env = api.Environment(cr, odoo.SUPERUSER_ID, {})
        job = admin_env["queue.job"].browse(job_id)

        # Timeout configuration
        timeout = job.timeout or 0
        timeout_event = threading.Event() if timeout else None
        started_at_mono = time.monotonic() if timeout else None

        run_uid = job.user_id.id or odoo.SUPERUSER_ID
        run_company_id = job.company_id.id or admin_env.company.id
        run_user = admin_env["res.users"].browse(run_uid)
        run_context = dict(
            admin_env.context,
            allowed_company_ids=[run_company_id],
            company_id=run_company_id,
            job_uuid=job.uuid or str(job.id),
        )
        if run_user.lang:
            run_context["lang"] = run_user.lang
        if run_user.tz:
            run_context["tz"] = run_user.tz
        run_env = api.Environment(cr, run_uid, run_context)
        # Tag the executing pool thread with the database name (and uid), the
        # same way Odoo's HTTP dispatcher and cron runner do. Odoo's QWeb
        # report renderer reads ``threading.current_thread().dbname`` in
        # ``ir.qweb`` (``QwebContent.irQweb``); on a pool thread that attribute
        # is unset, so accessing it raises ``AttributeError`` inside the QWeb
        # lazy-value property, which then recurses through ``__getattr__`` ->
        # ``__html__`` -> ``__str__`` until the stack overflows. Any job that
        # renders a QWeb report (e.g. a PDF disbursement voucher) crashes
        # without this. Setting it makes report rendering inside jobs behave
        # like a request/cron.
        #
        # Pool threads are reused across jobs, so the originals are captured
        # here and restored in the ``finally`` below — leaving stale dbname/uid
        # on an idle pooled thread could otherwise contaminate later work that
        # reads them (e.g. registry/env/security code).
        current_thread = threading.current_thread()
        orig_thread_dbname = getattr(current_thread, "dbname", None)
        orig_thread_uid = getattr(current_thread, "uid", None)
        current_thread.dbname = cr.dbname
        current_thread.uid = run_uid
        heartbeat_stop = threading.Event()
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(job_id, heartbeat_stop),
            kwargs={
                "timeout": timeout,
                "started_at": started_at_mono,
                "timeout_event": timeout_event,
            },
            daemon=True,
        )
        heartbeat_thread.start()

        try:
            _logger.info("Starting job %s (ID: %s)", job.channel, job.id)

            # Deserialize
            from ..models.job_serialization import JobDecoder

            raw_payload = (
                job.payload if isinstance(job.payload, str) else json.dumps(job.payload)
            )
            payload = json.loads(raw_payload, cls=JobDecoder, env=run_env)

            model_name = payload["model"]
            method_name = payload["method"]
            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})
            record_ids = payload.get("ids")
            try:
                run_model = run_env[model_name].with_company(run_company_id)
            except KeyError as key_exc:
                # Model absent from this worker's registry — almost always a
                # transient module install/upgrade window (the registry is
                # reloaded every registry_check_interval via check_signaling).
                # Retry without burning an attempt so the job survives the
                # upgrade instead of exhausting max_retries; handle_exception
                # bounds this by job age so a genuinely removed model still
                # fails eventually.
                raise TransientRegistryError(
                    f"Model {model_name!r} is not in the registry yet "
                    f"(worker registry may be mid-reload during a module "
                    f"install/upgrade); retrying without counting the attempt.",
                    seconds=self.registry_check_interval,
                    ignore_retry=True,
                ) from key_exc

            # Execute
            if record_ids:
                records = run_model.browse(record_ids)
                existing_ids = set(records.exists().ids)
                missing_ids = [
                    record_id
                    for record_id in record_ids
                    if record_id not in existing_ids
                ]
                if missing_ids:
                    raise ValueError(
                        f"Record(s) {missing_ids} not found in model {model_name}"
                    )
                result = getattr(records, method_name)(*args, **kwargs)
            else:
                result = getattr(run_model, method_name)(*args, **kwargs)

            # Check if timed out during execution
            if timeout_event and timeout_event.is_set():
                admin_env.cr.rollback()
                heartbeat_stop.set()
                with suppress(Exception):
                    heartbeat_thread.join(timeout=self.heartbeat_interval_seconds + 1)
                self._clear_worker_ownership(job_id)
                _logger.info("Job %s timed out, results discarded", job_id)
                return

            serialized_result = None
            try:
                serialized_result = job._serialize_result(result)
            except Exception:
                _logger.warning(
                    "Failed to serialize result for job %s",
                    job.id,
                    exc_info=True,
                )

            # Commit the job method's side effects (partner writes, etc.)
            # Do NOT write to the job record on this cursor — the heartbeat
            # thread may have committed UPDATEs to the same row during
            # execution, which would cause a SerializationFailure under
            # Odoo's REPEATABLE READ isolation when this cursor flushes.
            admin_env.cr.commit()

            heartbeat_stop.set()
            with suppress(Exception):
                heartbeat_thread.join(timeout=self.heartbeat_interval_seconds + 1)

            # Check timeout after joining heartbeat thread (race window
            # between pre-commit check and commit).
            if timeout_event and timeout_event.is_set():
                _logger.warning("Job %s timed out after side-effect commit", job_id)
                return

            # Finalize job state with READ COMMITTED to avoid serialization
            # conflicts with heartbeat updates. Uses retry helper for robustness.
            def finalize_job():
                with read_committed_cursor(self.db) as cr_done:
                    env_done = api.Environment(cr_done, odoo.SUPERUSER_ID, {})
                    done_job = env_done["queue.job"].browse(job_id)
                    done_job.state = "done"
                    done_job.completed_at = fields.Datetime.now()
                    if done_job.started_at:
                        duration_seconds = (
                            fields.Datetime.to_datetime(done_job.completed_at)
                            - fields.Datetime.to_datetime(done_job.started_at)
                        ).total_seconds()
                        done_job.duration = max(0.0, duration_seconds)
                    if serialized_result is not None:
                        done_job.result = serialized_result
                    done_job.worker_id = False
                    done_job.heartbeat = False
                    for child in done_job.child_ids.filtered(
                        lambda c: c.state == "waiting"
                    ):
                        child.state = "pending"
                    # Flush ORM writes so raw SQL sees done_job.state = "done"
                    env_done.flush_all()
                    # Release multi-parent dependents (group barriers)
                    if done_job.graph_uuid:
                        env_done.cr.execute(
                            """
                            UPDATE queue_job
                            SET pending_dependency_count =
                                    pending_dependency_count - 1,
                                state = CASE
                                    WHEN pending_dependency_count - 1 <= 0
                                        THEN 'pending'
                                    ELSE state
                                END,
                                write_date = NOW()
                            WHERE graph_uuid = %s
                              AND state = 'waiting'
                              AND dependency_job_ids IS NOT NULL
                              AND dependency_job_ids @> (%s)::jsonb
                            """,
                            (done_job.graph_uuid, json.dumps([job_id])),
                        )
                    env_done.cr.execute("NOTIFY queue_job_wake_up")
                    env_done.cr.commit()

            _retry_db_operation(finalize_job, f"finalize job {job_id}")
            _logger.info("Job %s finished successfully", job_id)

        except Exception as exc:
            admin_env.cr.rollback()

            # If timed out, _handle_timeout already updated the state
            if timeout_event and timeout_event.is_set():
                _logger.info("Job %s timed out (exception path), skipping", job_id)
                return

            # We assume the job execution failed and we rolled back its changes.
            # Now we need to update the job status to failed/retry.
            with self.db.cursor() as cr2:
                # Re-read job data to get current attempts
                env2 = api.Environment(cr2, odoo.SUPERUSER_ID, {})
                job2 = env2["queue.job"].browse(job_id)
                self.handle_exception(job2, exc=exc)
        finally:
            # Restore the thread's original dbname/uid — pool threads are
            # reused, so we must not leak this job's context onto the next.
            self._restore_thread_identity(
                current_thread, orig_thread_dbname, orig_thread_uid
            )
            heartbeat_stop.set()
            with suppress(Exception):
                heartbeat_thread.join(timeout=self.heartbeat_interval_seconds + 1)

    def handle_exception(self, job, exc=None):
        tb = traceback.format_exc()
        job.exc_info = tb

        # Model missing from the registry: retry WITHOUT counting the attempt
        # while the job is young enough to be inside a plausible module
        # install/upgrade window; once it ages past the cap, treat the model as
        # permanently gone and fail (so a renamed/removed model can't loop
        # forever). Checked before the generic RetryableJobError branch because
        # TransientRegistryError subclasses it.
        if isinstance(exc, TransientRegistryError):
            age_seconds = 0.0
            if job.create_date:
                age_seconds = (fields.Datetime.now() - job.create_date).total_seconds()
            if age_seconds < self.transient_registry_max_age_seconds:
                delay_seconds = exc.seconds or self.registry_check_interval
                job.state = "pending"
                job.scheduled_at = fields.Datetime.now() + datetime.timedelta(
                    seconds=delay_seconds
                )
                job.worker_id = False
                job.heartbeat = False
                _logger.warning(
                    "Job %s model missing from registry (age %.0fs < %ss cap); "
                    "retrying in %ds without counting the attempt (stays %s).",
                    job.id,
                    age_seconds,
                    self.transient_registry_max_age_seconds,
                    delay_seconds,
                    job.attempts,
                )
                job.env.cr.commit()
                return
            job.state = "failed"
            job.completed_at = fields.Datetime.now()
            if job.started_at:
                job.duration = max(
                    0.0,
                    (
                        fields.Datetime.to_datetime(job.completed_at)
                        - fields.Datetime.to_datetime(job.started_at)
                    ).total_seconds(),
                )
            job.scheduled_at = False
            job.worker_id = False
            job.heartbeat = False
            self._cascade_failure_to_dependents(job, f"Parent job {job.id} failed")
            _logger.error(
                "Job %s model still missing from registry after %.0fs (> %ss cap); "
                "failing — the model appears to be permanently removed.",
                job.id,
                age_seconds,
                self.transient_registry_max_age_seconds,
            )
            job.env.cr.commit()
            return

        if isinstance(exc, RetryableJobError):
            if not exc.ignore_retry:
                job.attempts += 1
            if exc.seconds is not None:
                delay_seconds = exc.seconds
            else:
                delay_seconds = min(
                    10 * (2 ** max(job.attempts - 1, 0)),
                    self.max_backoff_seconds,
                )
            job.state = "pending"
            job.scheduled_at = fields.Datetime.now() + datetime.timedelta(
                seconds=delay_seconds
            )
            job.worker_id = False
            job.heartbeat = False
            _logger.warning(
                "Job %s raised RetryableJobError (attempt %s/%s). Retrying in %ds.",
                job.id,
                job.attempts,
                job.max_retries,
                delay_seconds,
            )
            job.env.cr.commit()
            return

        job.attempts += 1

        retry_infinitely = job.max_retries == 0
        retry_remaining = job.max_retries and job.attempts <= job.max_retries
        if retry_infinitely or retry_remaining:
            job.state = "pending"
            # Exponential backoff
            delay_seconds = min(
                10 * (2 ** max(job.attempts - 1, 0)),
                self.max_backoff_seconds,
            )  # 10s, 20s, 40s... capped
            job.scheduled_at = fields.Datetime.now() + datetime.timedelta(
                seconds=delay_seconds
            )
            job.worker_id = False
            job.heartbeat = False
            _logger.warning(
                "Job %s failed (Check %s/%s). Retrying in %ds.",
                job.id,
                job.attempts,
                job.max_retries,
                delay_seconds,
            )
        else:
            job.state = "failed"
            job.completed_at = fields.Datetime.now()
            if job.started_at:
                duration_seconds = (
                    fields.Datetime.to_datetime(job.completed_at)
                    - fields.Datetime.to_datetime(job.started_at)
                ).total_seconds()
                job.duration = max(0.0, duration_seconds)
            job.scheduled_at = False
            job.worker_id = False
            job.heartbeat = False
            self._cascade_failure_to_dependents(job, f"Parent job {job.id} failed")
            _logger.error("Job %s failed permanently.\n%s", job.id, tb)

        job.env.cr.commit()

    def _cascade_failure_to_dependents(self, job, reason):
        """Fail this job's waiting children and group-barrier dependents.

        Shared by the permanent-failure paths so a failed parent does not
        leave dependents stuck in 'waiting' forever.
        """
        for child in job.child_ids.filtered(lambda c: c.state == "waiting"):
            child.state = "failed"
            child.exc_info = reason
        if job.graph_uuid:
            job.env.cr.execute(
                """
                UPDATE queue_job
                SET state = 'failed',
                    exc_info = %s,
                    write_date = NOW()
                WHERE graph_uuid = %s
                  AND state = 'waiting'
                  AND dependency_job_ids IS NOT NULL
                  AND dependency_job_ids @> (%s)::jsonb
                """,
                (reason, job.graph_uuid, json.dumps([job.id])),
            )
