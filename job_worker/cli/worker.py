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

from ..exception import RetryableJobError

_logger = logging.getLogger(__name__)

# PostgreSQL concurrency error codes that should be retried
PG_CONCURRENCY_ERRORS_TO_RETRY = (
    "40001",  # serialization_failure
    "40P01",  # deadlock_detected
    "55P03",  # lock_not_available
)


@contextmanager
def read_committed_cursor(db):
    """Context manager for READ COMMITTED isolation.

    PostgreSQL 18 has stricter serialization checks. For heartbeat
    and status updates where we want latest committed state,
    READ COMMITTED is appropriate and avoids SerializationFailure.

    No isolation restoration needed: each call creates its own cursor
    from the pool, and Odoo's cursor.__exit__ handles cleanup.
    Uses Odoo's internal cr._cnx (psycopg2 connection), which is
    stable in Odoo 19 but is a private API.
    """
    with db.cursor() as cr:
        cr._cnx.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
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
        registry_lag_grace_seconds=3600,
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
        # How long a job may keep getting re-queued purely because its model is
        # missing from this worker's registry (worker lagging the app server).
        # Generous by default so it comfortably covers a redeploy; past it the
        # model is almost certainly never coming, so the job stops being treated
        # as "transiently lagging" and fails through the normal retry path
        # instead of re-queueing forever.
        self.registry_lag_grace_seconds = max(0, int(registry_lag_grace_seconds))
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
                    # 0. Check for registry changes (module install/update)
                    self._check_registry()

                    # 1. Process jobs until queue is empty or limit reached
                    self.process_jobs()

                    # 2. Update heartbeats for currently running jobs (if any)
                    self.update_heartbeats()

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
        with read_committed_cursor(self.db) as cr:
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
            job_id = self._acquire_job()
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

    def _acquire_job(self):
        """Acquire one job using SKIP LOCKED and commit the 'started' state.

        Returns the job ID or None if no job is available.
        Exceptions propagate up (appropriate for connection failures —
        runner will restart the worker).
        """
        with self.db.cursor() as cr:
            job_id = self.acquire_job_lock(cr)
            if not job_id:
                return None
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
            SELECT j.id
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
        if res:
            job_id = res[0]
            # Mark as started and update heartbeat/worker_id
            cr.execute(
                "UPDATE queue_job"
                " SET state = 'started', heartbeat = NOW(),"
                " worker_id = %s, write_date = NOW(),"
                " started_at = NOW()"
                " WHERE id = %s",
                (self.worker_uuid, job_id),
            )
            return job_id
        return None

    @retry_on_serialization_failure(max_retries=3, base_delay=0.1)
    def _heartbeat_job(self, job_id):
        """Update heartbeat for single job. Uses READ COMMITTED isolation."""
        with read_committed_cursor(self.db) as hb_cr:
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

    def _guard_model_in_registry(self, run_env, model_name, job):
        """Cope with a job whose model is missing from this worker's registry.

        A missing model almost always means the worker is transiently behind
        the app server, NOT that the job is bad: the worker is a separate
        process that builds its registry by importing module code from its own
        addons-path, so right after a module is installed/updated from the UI
        (before the 30s signaling reload lands) or while the worker is being
        redeployed, the model can be absent here while present on the server.
        A bare ``run_env[model_name]`` lookup would raise ``KeyError``, be
        treated as a normal failure, and burn the whole retry budget — failing
        a job that would succeed seconds later once the registry reloads.

        For a generous grace window we instead expedite the next signaling
        check and re-queue WITHOUT consuming an attempt, so the job rides out
        the catch-up window. Past the grace window the model is almost
        certainly never coming (a typo'd/uninstalled model, or a worker that
        will never get the code), so we return and let the caller's normal
        lookup raise and fail through the standard retry budget rather than
        re-queueing forever.
        """
        if model_name in run_env.registry.models:
            return
        job_age = (
            (fields.Datetime.now() - job.create_date).total_seconds()
            if job.create_date
            else 0.0
        )
        if job_age <= self.registry_lag_grace_seconds:
            self._last_registry_check = 0.0  # force a reload on the next loop tick
            raise RetryableJobError(
                f"Model {model_name!r} is not in this worker's registry yet — "
                f"the worker likely lags the app server (a module install/"
                f"update or redeploy in progress). Reloading the registry and "
                f"retrying.",
                seconds=self.registry_check_interval,
                ignore_retry=True,
            )
        _logger.error(
            "Model %r still missing from this worker's registry after %.0fs "
            "(grace %ss); the worker's code likely never received the module "
            "(check its addons-path / redeploy). Failing job %s through the "
            "normal retry path.",
            model_name,
            job_age,
            self.registry_lag_grace_seconds,
            job.id,
        )

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

            self._guard_model_in_registry(run_env, model_name, job)
            run_model = run_env[model_name].with_company(run_company_id)

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
            heartbeat_stop.set()
            with suppress(Exception):
                heartbeat_thread.join(timeout=self.heartbeat_interval_seconds + 1)

    def handle_exception(self, job, exc=None):
        tb = traceback.format_exc()
        job.exc_info = tb

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
            # Cascade failure to waiting children
            for child in job.child_ids.filtered(lambda c: c.state == "waiting"):
                child.state = "failed"
                child.exc_info = f"Parent job {job.id} failed"
            # Cascade failure to multi-parent dependents (group barriers)
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
                    (
                        f"Parent job {job.id} failed",
                        job.graph_uuid,
                        json.dumps([job.id]),
                    ),
                )
            _logger.error("Job %s failed permanently.\n%s", job.id, tb)

        job.env.cr.commit()
