"""Stress S10 — PG18 serialization-retry under sustained heartbeat conflict (Tier 1).

200 jobs pre-set to ``started`` state. 10 threads drive simultaneous
heartbeat updates (READ COMMITTED) and completion updates (default
isolation) wrapped in ``_retry_db_operation``. Validates that the
retry decorator absorbs any ``SerializationFailure`` PG18 raises at
scale — the single-conflict case is covered by
``test_concurrency_and_recovery.py::test_read_committed_prevents_heartbeat_completion_conflict``;
this scenario stresses the same path at volume.

Opt-in via ``--test-tags=stress``.
"""

import threading
import time
import uuid
from contextlib import closing

import odoo
from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import _retry_db_operation, read_committed_cursor
from .stress_common import (
    assert_queue_invariants,
    clear_queue,
    pg_version,
    record_report,
)

TOTAL_JOBS = 200
THREAD_COUNT = 10
HEARTBEATS_PER_JOB = 5  # interleave several heartbeats per completion

assert TOTAL_JOBS % THREAD_COUNT == 0, "jobs must divide evenly across threads"
JOBS_PER_THREAD = TOTAL_JOBS // THREAD_COUNT


@tagged("post_install", "-at_install", "-standard", "stress")
class TestS10Pg18SerializationStorm(TransactionCase):
    def setUp(self):
        super().setUp()
        with self.env.registry.cursor() as cr:
            clear_queue(api.Environment(cr, SUPERUSER_ID, {}))
            cr.commit()

    def _seed_started_jobs(self, db, worker_id):
        """Insert TOTAL_JOBS rows directly in ``started`` state.

        Using raw SQL skips the ``enqueue() → run_now() → state=started``
        ORM path. We're stressing the heartbeat/completion race, not the
        acquire path.
        """
        with closing(db.cursor()) as cr:
            cr.execute(
                """
                INSERT INTO queue_job
                    (payload, state, channel, priority, max_retries, timeout,
                     attempts, scheduled_at, worker_id, heartbeat,
                     started_at, create_date, write_date, create_uid, write_uid,
                     company_id, uuid)
                SELECT
                    %s::jsonb,
                    'started',
                    %s,
                    10, 5, 0, 0,
                    NOW(),
                    %s,
                    NOW(),
                    NOW(),
                    NOW(),
                    NOW(),
                    %s, %s, NULL,
                    gen_random_uuid()::text
                FROM generate_series(1, %s)
                RETURNING id
                """,
                (
                    '{"model": "res.users", "method": "search", '
                    '"ids": [], "args": [[["id", "=", 1]]], "kwargs": {}}',
                    f"stress_s10_{worker_id[:8]}",
                    worker_id,
                    SUPERUSER_ID,
                    SUPERUSER_ID,
                    TOTAL_JOBS,
                ),
            )
            ids = [row[0] for row in cr.fetchall()]
            cr.commit()
        return ids

    def _heartbeat(self, db, job_id, worker_id):
        """Heartbeat path mirrors worker._heartbeat_job (read committed)."""
        with read_committed_cursor(db) as cr:
            cr.execute(
                """
                UPDATE queue_job
                   SET heartbeat = NOW()
                 WHERE id = %s
                   AND state = 'started'
                   AND worker_id = %s
                """,
                (job_id, worker_id),
            )
            cr.commit()

    def _complete(self, db, job_id, worker_id):
        """Completion path uses default isolation, mirrors worker._mark_done."""
        with closing(db.cursor()) as cr:
            cr.execute(
                """
                UPDATE queue_job
                   SET state = 'done',
                       completed_at = NOW(),
                       duration = EXTRACT(EPOCH FROM (NOW() - started_at)),
                       write_date = NOW()
                 WHERE id = %s
                   AND state = 'started'
                   AND worker_id = %s
                """,
                (job_id, worker_id),
            )
            cr.commit()

    def test_s10_pg18_serialization_storm(self):
        worker_id = f"stress-s10-{uuid.uuid4().hex}"
        db = odoo.sql_db.db_connect(self.env.cr.dbname)

        job_ids = self._seed_started_jobs(db, worker_id)
        self.assertEqual(len(job_ids), TOTAL_JOBS)

        # Partition job IDs evenly across threads. No overlap — we're
        # stressing parallel heartbeat/completion paths, not same-row
        # contention (which test_concurrency_and_recovery.py covers).
        partitions = [
            job_ids[i * JOBS_PER_THREAD : (i + 1) * JOBS_PER_THREAD]
            for i in range(THREAD_COUNT)
        ]

        barrier = threading.Barrier(THREAD_COUNT)
        exceptions = []  # (thread_idx, job_id, exception)
        retry_summary = {"attempts": 0}

        def thread_body(idx, my_jobs):
            try:
                barrier.wait(timeout=10)
            except threading.BrokenBarrierError as err:
                exceptions.append((idx, None, err))
                return
            for job_id in my_jobs:
                try:
                    for _ in range(HEARTBEATS_PER_JOB):
                        _retry_db_operation(
                            lambda jid=job_id: self._heartbeat(db, jid, worker_id),
                            f"stress_s10_heartbeat_{job_id}",
                            max_retries=5,
                        )
                        retry_summary["attempts"] += 1
                    _retry_db_operation(
                        lambda jid=job_id: self._complete(db, jid, worker_id),
                        f"stress_s10_complete_{job_id}",
                        max_retries=5,
                    )
                    retry_summary["attempts"] += 1
                except Exception as err:  # noqa: BLE001 — capture all
                    exceptions.append((idx, job_id, err))

        threads = [
            threading.Thread(target=thread_body, args=(idx, partition), daemon=True)
            for idx, partition in enumerate(partitions)
        ]
        started_at = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)
        elapsed = time.monotonic() - started_at

        for t in threads:
            self.assertFalse(t.is_alive(), "thread did not finish within timeout")

        # Hard assertions: zero unhandled exceptions, every job done.
        self.assertFalse(
            exceptions,
            f"{len(exceptions)} unhandled exceptions: "
            f"{[(i, j, repr(e)) for i, j, e in exceptions[:5]]}",
        )

        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            done_count = env["queue.job"].search_count(
                [("worker_id", "=", worker_id), ("state", "=", "done")]
            )
            self.assertEqual(done_count, TOTAL_JOBS)
            assert_queue_invariants(self, env, expected_total=TOTAL_JOBS)

        record_report(
            "S10",
            {
                "jobs": {"seeded": TOTAL_JOBS, "done": done_count},
                "threads": THREAD_COUNT,
                "heartbeats_per_job": HEARTBEATS_PER_JOB,
                "total_db_operations": retry_summary["attempts"],
                "unhandled_exceptions": len(exceptions),
                "elapsed_seconds": round(elapsed, 3),
                "operations_per_sec": round(retry_summary["attempts"] / elapsed, 2)
                if elapsed > 0
                else None,
                "pg_version": pg_version(self.env.registry),
            },
        )
