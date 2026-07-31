from contextlib import closing
from unittest.mock import patch

from psycopg2 import DatabaseError, OperationalError

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
    """A database error must recover in place, never end the worker thread.

    Regression for the preprod incident of 2026-07-30. A duplicate-key
    ``UniqueViolation`` aborted a transaction; a later read on that poisoned
    cursor raised ``InFailedSqlTransaction`` — an ``InternalError``, not an
    ``OperationalError``, so none of the ``OperationalError`` guards caught it.
    It reached ``run()``, which had only a ``finally``, and the thread function
    returned. Five deaths in 300s later the **database was quarantined**: every
    job on it stopped, with 47 chunks still to run.

    Returning cleanly instead of raising would not have helped: the supervisor
    cannot tell the two apart (see
    ``TestDatabaseErrorsNeverQuarantineTheDatabase``). The thread has to stay
    alive.

    One bad row must fail its job — never the worker, and never the database.
    """

    def _worker(self, **kwargs):
        kwargs.setdefault("database_error_backoff_seconds", 0.05)
        kwargs.setdefault("database_error_backoff_cap_seconds", 0.05)
        return QueueWorker(self.env.cr.dbname, **kwargs)

    def _run_with_failing_cursor(self, worker, error, survive_rounds=3):
        """Run ``worker`` against a cursor that raises, and stop it afterwards.

        The stop condition matters: recovery is now a loop, so without one this
        would spin forever. Stopping after ``survive_rounds`` failures is also
        the assertion — reaching round N proves the loop kept going rather than
        bailing out on the first error.
        """
        attempts = []

        def _cursor(*args, **kwargs):
            attempts.append(1)
            if len(attempts) >= survive_rounds:
                worker.stop_event.set()
            raise error

        with patch.object(type(worker.db), "cursor", side_effect=_cursor):
            worker.run()
        return len(attempts)

    def test_database_error_recovers_in_place_without_ending_the_loop(self):
        """The production case: InFailedSqlTransaction, an InternalError."""
        from psycopg2.errors import InFailedSqlTransaction

        worker = self._worker()
        attempts = self._run_with_failing_cursor(
            worker,
            InFailedSqlTransaction("current transaction is aborted"),
        )
        # Retried rather than returning on the first error. Before the fix this
        # was 1: run() unwound and the thread function returned.
        self.assertEqual(attempts, 3)

    def test_interface_error_recovers_too(self):
        """``InterfaceError`` is a sibling of ``DatabaseError``, not a subclass.

        A guard written as ``except DatabaseError`` misses "connection already
        closed" entirely, so a dropped connection object would still end the
        thread. The guard is on ``psycopg2.Error`` for exactly this.
        """
        from psycopg2 import InterfaceError

        self.assertFalse(issubclass(InterfaceError, DatabaseError))
        worker = self._worker()
        attempts = self._run_with_failing_cursor(
            worker, InterfaceError("connection already closed")
        )
        self.assertEqual(attempts, 3)

    def test_operational_error_recovers_too(self):
        """``OperationalError`` reaching ``run()`` must recover, not end it.

        Not a preservation test: before this change an ``OperationalError``
        that escaped the pgcode-filtered handlers inside ``process_jobs`` /
        ``update_heartbeats`` ended the thread like any other. Those inner
        handlers are unchanged and are covered by ``TestWorkerDbTimeouts``;
        this covers the outer guard they fall through to.
        """
        worker = self._worker()
        attempts = self._run_with_failing_cursor(
            worker, OperationalError("connection lost")
        )
        self.assertEqual(attempts, 3)

    def test_recovery_is_visible_to_the_supervisor(self):
        """A worker stuck in recovery must be reportable as degraded.

        Nothing else can see it any more: the thread is alive, so
        ``_check_thread_health`` is happy, and no failure is recorded, so the
        quarantine set and ``_failure_timestamps`` stay empty. Without this
        signal a permanently wedged database would keep the container healthy
        while serving nothing.
        """
        from psycopg2.errors import InFailedSqlTransaction

        worker = self._worker()
        self.assertEqual(worker.seconds_in_database_error_recovery(), 0.0)

        self._run_with_failing_cursor(
            worker, InFailedSqlTransaction("current transaction is aborted")
        )
        self.assertGreater(worker.seconds_in_database_error_recovery(), 0.0)

    def test_a_healthy_cycle_clears_the_recovery_signal(self):
        """Otherwise one transient blip would mark the database degraded forever."""
        worker = self._worker()
        # Simulate a streak, then a completed cycle.
        worker._back_off_after_database_error()
        self.assertGreater(worker.seconds_in_database_error_recovery(), 0.0)

        worker.stop_event.set()
        with (
            patch.object(worker, "_check_registry"),
            patch.object(worker, "process_jobs"),
        ):
            worker._listen_and_serve()
        # stop_event was already set, so the body never ran and the streak must
        # survive — clearing it on entry rather than on a completed cycle is the
        # bug this pins.
        self.assertGreater(worker.seconds_in_database_error_recovery(), 0.0)

        worker.stop_event.clear()
        rounds = []

        def _stop_after_one_cycle():
            rounds.append(1)
            worker.stop_event.set()

        with (
            patch.object(worker, "_check_registry"),
            patch.object(worker, "process_jobs", side_effect=_stop_after_one_cycle),
        ):
            worker._listen_and_serve()
        self.assertEqual(len(rounds), 1)
        self.assertEqual(worker.seconds_in_database_error_recovery(), 0.0)

    def test_recovery_does_not_tear_down_the_execution_pool(self):
        """Pool teardown strands jobs whose rows are already 'started'.

        ``_acquire_job`` commits the row as ``started`` before submitting it,
        so ``shutdown(cancel_futures=True)`` cancels work the database already
        believes is running. Those rows then sit out the whole stale window and
        burn an attempt on reclaim — the ``attempts`` erosion the incident
        report describes. Recovering in place must not do that.
        """
        from psycopg2.errors import InFailedSqlTransaction

        worker = self._worker()
        with patch.object(worker._pool, "shutdown") as shutdown:
            self._run_with_failing_cursor(
                worker,
                InFailedSqlTransaction("current transaction is aborted"),
                survive_rounds=3,
            )
        # Exactly once, on the way out — not once per recovered error.
        shutdown.assert_called_once()
