"""The worker loop pauses on its upgrade gate.

While the gate is closed the worker must not reload its registry (which runs
every ``_register_hook``) or take new jobs, but it keeps heartbeating the jobs
it is already running and keeps advancing ``last_progress`` — a deliberate
pause is not a stall. When the gate reopens it picks up the upgraded registry
at once rather than up to ``registry_check_interval`` later.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

from psycopg2 import OperationalError

from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker


class _FakeGate:
    """Answers ``is_open`` from a script; the last answer repeats."""

    interval_seconds = 0.001

    def __init__(self, answers):
        self._answers = list(answers)
        self.calls = 0

    def is_open(self):
        self.calls += 1
        answer = self._answers.pop(0) if len(self._answers) > 1 else self._answers[0]
        if isinstance(answer, Exception):
            raise answer
        return answer


@tagged("post_install", "-at_install")
class TestWorkerUpgradeGate(TransactionCase):
    def _worker(self, gate, **kwargs):
        kwargs.setdefault("poll_timeout", 0)
        return QueueWorker(
            self.env.cr.dbname,
            stop_event=threading.Event(),
            upgrade_gate=gate,
            **kwargs,
        )

    def _run_cycles(self, worker, cycles):
        """Run ``_listen_and_serve`` for ``cycles`` iterations."""
        seen = []

        def heartbeat():
            seen.append(1)
            if len(seen) >= cycles:
                worker.stop_event.set()

        with patch.object(worker, "update_heartbeats", side_effect=heartbeat) as hb:
            worker._listen_and_serve()
        return hb

    def test_a_closed_gate_pauses_registry_reloads_and_jobs(self):
        worker = self._worker(_FakeGate([False]))
        before = worker.last_progress
        with (
            patch.object(worker, "_check_registry") as check_registry,
            patch.object(worker, "process_jobs") as process_jobs,
        ):
            heartbeats = self._run_cycles(worker, cycles=1)
        check_registry.assert_not_called()
        process_jobs.assert_not_called()
        # Running jobs keep their heartbeat, and the pause is not a stall.
        heartbeats.assert_called_once_with()
        self.assertGreater(worker.last_progress, before)

    def test_an_open_gate_runs_the_normal_cycle(self):
        worker = self._worker(_FakeGate([True]))
        with (
            patch.object(worker, "_check_registry") as check_registry,
            patch.object(worker, "process_jobs") as process_jobs,
        ):
            self._run_cycles(worker, cycles=1)
        check_registry.assert_called_once_with()
        process_jobs.assert_called_once_with()

    def test_reopening_reloads_the_registry_immediately(self):
        worker = self._worker(_FakeGate([False, True]), registry_check_interval=3600)
        # A registry check just happened, so without the reopen it would not
        # run again for an hour.
        worker._last_registry_check = time.monotonic()
        registry_class = MagicMock()
        with (
            patch("odoo.orm.registry.Registry", registry_class),
            patch.object(worker, "process_jobs"),
        ):
            self._run_cycles(worker, cycles=2)
        registry_class.assert_called_once_with(self.env.cr.dbname)
        registry_class.return_value.check_signaling.assert_called_once_with()

    def test_process_jobs_does_not_acquire_through_a_closed_gate(self):
        worker = self._worker(_FakeGate([False]))
        with patch.object(worker, "_acquire_job") as acquire:
            worker.process_jobs()
        acquire.assert_not_called()

    def test_process_jobs_stops_acquiring_when_the_gate_closes(self):
        worker = self._worker(_FakeGate([True, False]), concurrency=4)
        worker._pool = MagicMock(spec=ThreadPoolExecutor)
        with patch.object(worker, "_acquire_job", return_value=101) as acquire:
            worker.process_jobs()
        acquire.assert_called_once_with()
        worker._pool.submit.assert_called_once()

    def test_a_database_error_from_the_gate_takes_the_normal_recovery(self):
        # run() catches psycopg2.Error from _listen_and_serve and re-establishes
        # the session in place; the gate's probe errors must travel that path.
        worker = self._worker(_FakeGate([OperationalError("db unreachable")]))
        with self.assertRaises(OperationalError):
            worker._listen_and_serve()

    def test_without_a_gate_the_worker_is_unchanged(self):
        worker = QueueWorker(self.env.cr.dbname, stop_event=threading.Event())
        self.assertIsNone(worker.upgrade_gate)
        self.assertTrue(worker._upgrade_gate_is_open())
