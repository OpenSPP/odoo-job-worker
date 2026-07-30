from contextlib import closing
from unittest.mock import patch

from psycopg2 import OperationalError

import odoo
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import (
    QueueWorker,
    _apply_control_plane_timeouts,
    read_committed_cursor,
)


@tagged("post_install", "-at_install")
class TestWorkerDbTimeouts(TransactionCase):
    """Control-plane cursors must be bounded and transaction-scoped.

    A blocked heartbeat/acquire query with no timeout froze the worker
    main loop while the process stayed alive — the preprod zombie-worker
    root cause. These queries now carry SET LOCAL statement/lock timeouts
    that must (a) actually apply, (b) not leak to the next borrower of the
    pooled connection (job execution must stay unbounded).
    """

    def _db(self):
        return odoo.sql_db.db_connect(self.env.cr.dbname)

    def test_read_committed_cursor_applies_timeouts(self):
        db = self._db()
        with read_committed_cursor(
            db, statement_timeout_ms=250, lock_timeout_ms=750
        ) as cr:
            cr.execute("SHOW statement_timeout")
            self.assertEqual(cr.fetchone()[0], "250ms")
            cr.execute("SHOW lock_timeout")
            self.assertEqual(cr.fetchone()[0], "750ms")

    def test_timeouts_are_transaction_scoped_no_leak(self):
        """SET LOCAL must revert at transaction end so a job-execution cursor
        borrowing the same pooled connection is not silently bounded."""
        db = self._db()
        with read_committed_cursor(db, statement_timeout_ms=250) as cr:
            cr.execute("SHOW statement_timeout")
            self.assertEqual(cr.fetchone()[0], "250ms")
        with closing(db.cursor()) as cr2:
            cr2.execute("SHOW statement_timeout")
            self.assertNotEqual(cr2.fetchone()[0], "250ms")

    def test_no_args_leaves_default(self):
        db = self._db()
        with closing(db.cursor()) as base:
            base.execute("SHOW statement_timeout")
            default = base.fetchone()[0]
        with read_committed_cursor(db) as cr:
            cr.execute("SHOW statement_timeout")
            self.assertEqual(cr.fetchone()[0], default)

    def test_statement_timeout_cancels_slow_query(self):
        """The whole point: a query that overruns the bound is cancelled
        (raises) rather than blocking the loop forever."""
        db = self._db()
        with self.assertRaises(OperationalError):
            with read_committed_cursor(db, statement_timeout_ms=100) as cr:
                cr.execute("SELECT pg_sleep(2)")

    def test_apply_helper_skips_nonpositive(self):
        """0 / negative timeouts leave the session default untouched."""
        db = self._db()
        with closing(db.cursor()) as base:
            base.execute("SHOW statement_timeout")
            default = base.fetchone()[0]
        with closing(db.cursor()) as cr:
            _apply_control_plane_timeouts(cr, 0, 0)
            cr.execute("SHOW statement_timeout")
            self.assertEqual(cr.fetchone()[0], default)

    def test_worker_converts_seconds_to_ms(self):
        worker = QueueWorker(
            self.env.cr.dbname,
            statement_timeout_seconds=30,
            lock_timeout_seconds=10,
        )
        self.assertEqual(worker.control_statement_timeout_ms, 30000)
        self.assertEqual(worker.control_lock_timeout_ms, 10000)

    def test_worker_zero_timeouts_disable(self):
        worker = QueueWorker(
            self.env.cr.dbname,
            statement_timeout_seconds=0,
            lock_timeout_seconds=0,
        )
        self.assertEqual(worker.control_statement_timeout_ms, 0)
        self.assertEqual(worker.control_lock_timeout_ms, 0)

    def test_process_jobs_backs_off_on_transient_acquire_error(self):
        """A transient acquire error (here a statement_timeout, which the
        serialization-retry wrapper does not retry) must be backed off by the
        process_jobs loop, not crash the thread — else the supervisor restart +
        registry reload snowballs under load."""

        class _StatementTimeout(OperationalError):
            pgcode = "57014"  # query_canceled (statement_timeout)

        worker = QueueWorker(self.env.cr.dbname)
        with patch.object(worker, "_acquire_job", side_effect=_StatementTimeout()):
            # No active jobs -> one iteration, then a clean break. Must not raise.
            worker.process_jobs()

    def test_process_jobs_reraises_non_transient_acquire_error(self):
        """A non-transient DB error still propagates so the runner can react."""

        class _OtherError(OperationalError):
            pgcode = "23505"  # unique_violation — not transient

        worker = QueueWorker(self.env.cr.dbname)
        with patch.object(worker, "_acquire_job", side_effect=_OtherError()):
            with self.assertRaises(OperationalError):
                worker.process_jobs()


@tagged("post_install", "-at_install")
class TestWorkerSurvivesJobLevelDbErrors(TransactionCase):
    """``run()`` must not let a database error kill the worker thread.

    Regression for the preprod incident of 2026-07-30. A duplicate-key
    ``UniqueViolation`` aborted the transaction; a later ``env.ref()`` on that
    same poisoned cursor raised ``InFailedSqlTransaction``. That is an
    ``InternalError``, not an ``OperationalError``, so the acquire loop's
    handler did not catch it — it reached ``run()``, which had only a
    ``finally``, and the thread function returned.

    The supervisor restarted the thread, the restart re-entered registry load,
    hit the poisoned cursor again, and after five deaths in 300s the **database
    was quarantined**: every job on it stopped, with 47 chunks still to run.

    One bad row must fail its job — never the worker, and never the database.
    """

    def test_database_error_does_not_kill_the_thread(self):
        from psycopg2.errors import InFailedSqlTransaction

        from ..cli.worker import QueueWorker

        worker = QueueWorker(self.env.cr.dbname)
        # Fail at the first thing run() does with the cursor, which is the
        # cheapest faithful stand-in for "the transaction is already aborted".
        with patch.object(
            type(worker.db),
            "cursor",
            side_effect=InFailedSqlTransaction("current transaction is aborted"),
        ):
            # Must RETURN, not raise. A raise here is the thread dying, which is
            # what quarantined the database in production.
            worker.run()

    def test_operational_error_still_does_not_kill_the_thread(self):
        """The pre-existing transient-error path must keep working.

        ``OperationalError`` is a ``DatabaseError`` subclass, so widening the
        guard must not have narrowed this case.
        """
        from ..cli.worker import QueueWorker

        worker = QueueWorker(self.env.cr.dbname)
        with patch.object(
            type(worker.db),
            "cursor",
            side_effect=OperationalError("connection lost"),
        ):
            worker.run()
