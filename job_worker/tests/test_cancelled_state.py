from datetime import timedelta

from odoo import fields
from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestCancelledState(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def _make_job(self):
        return self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Cancel Test"}],
            kwargs={},
        )

    def test_button_cancelled_sets_state(self):
        job = self._make_job()
        self.assertEqual(job.state, "pending")
        job.button_cancelled()
        self.assertEqual(job.state, "cancelled")

    def test_button_cancelled_sets_cancelled_at(self):
        job = self._make_job()
        self.assertFalse(job.cancelled_at)
        before = fields.Datetime.now()
        job.button_cancelled()
        after = fields.Datetime.now()
        self.assertTrue(job.cancelled_at)
        cancelled_at = fields.Datetime.to_datetime(job.cancelled_at)
        self.assertTrue(before <= cancelled_at <= after)

    def test_cancelled_state_in_selection(self):
        """The 'cancelled' state is a valid selection value."""
        job = self._make_job()
        job.write({"state": "cancelled"})
        self.assertEqual(job.state, "cancelled")

    def test_cancelled_job_excluded_from_identity_check(self):
        """Cancelled jobs with identity keys allow new jobs with same key."""
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Dedup Cancel A"}],
            kwargs={},
            identity_key="cancel_dedup_key",
        )
        job1.button_cancelled()
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Dedup Cancel B"}],
            kwargs={},
            identity_key="cancel_dedup_key",
        )
        self.assertNotEqual(job1.id, job2.id)

    def test_requeue_from_cancelled(self):
        """Cancelled jobs can be requeued."""
        job = self._make_job()
        job.button_cancelled()
        self.assertEqual(job.state, "cancelled")
        job.button_requeue()
        self.assertEqual(job.state, "pending")

    def test_cancelled_children_on_parent_cancel(self):
        """Cancelling a parent cancels the children left waiting on it.

        REVERSED, deliberately. This previously asserted the child stayed in
        ``waiting`` and called that an explicit design choice. It is the bug:
        the child waits for its parent to complete, the parent never will, and
        nothing else ever moves it. ``waiting`` is not a terminal state, so the
        row is invisible to every failure surface and survives ``_gc_old_jobs``
        (which prunes only done/failed/cancelled) indefinitely.

        Cancelled rather than failed: the parent did not fail, it never ran.
        """
        from ..delay import chain as delay_chain

        a = self.env["res.partner"].create({"name": "Parent"})
        b = self.env["res.partner"].create({"name": "Child"})
        delay_chain(
            a.delayable().write({"name": "A done"}),
            b.delayable().write({"name": "B done"}),
        ).delay()
        jobs = self.Job.search([], order="id desc", limit=2).sorted("id")
        parent_job = jobs[0]
        child_job = jobs[1]
        parent_job.button_cancelled()
        child_job.invalidate_recordset()
        self.assertEqual(parent_job.state, "cancelled")
        self.assertEqual(
            child_job.state, "cancelled", "a child of a cancelled parent can never run"
        )


@tagged("post_install", "-at_install")
class TestStateTransitionEdgeCases(TransactionCase):
    """Probe state machine for invalid transitions and boundary conditions."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def _create_job(self, **overrides):
        defaults = {
            "model_name": "res.partner",
            "method_name": "write",
            "record_ids": [],
            "args": [{"name": "state transition"}],
            "kwargs": {},
            "channel": "state_edge",
        }
        defaults.update(overrides)
        return self.Job.enqueue(**defaults)

    def test_button_requeue_from_pending_state(self):
        """Requeueing a pending job should not corrupt state."""
        job = self._create_job()
        self.assertEqual(job.state, "pending")
        job.button_requeue()
        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts, 0)

    def test_button_requeue_from_started_state(self):
        """Requeueing a started job resets it to pending."""
        job = self._create_job()
        job.write({"state": "started", "heartbeat": fields.Datetime.now()})
        job.button_requeue()
        self.assertEqual(job.state, "pending")
        self.assertFalse(job.worker_id)
        self.assertFalse(job.heartbeat)

    def test_button_requeue_from_done_state(self):
        """Requeueing a done job should reset it properly."""
        job = self._create_job()
        job.write({"state": "done", "completed_at": fields.Datetime.now()})
        job.button_requeue()
        self.assertEqual(job.state, "pending")
        self.assertFalse(job.completed_at)

    def test_button_requeue_from_cancelled_state(self):
        """Requeueing a cancelled job should reset it properly."""
        job = self._create_job()
        job.write({"state": "cancelled", "cancelled_at": fields.Datetime.now()})
        job.button_requeue()
        self.assertEqual(job.state, "pending")
        self.assertFalse(job.cancelled_at)

    def test_button_set_to_done_from_pending(self):
        """Forcing done on a pending job should work."""
        job = self._create_job()
        job.button_set_to_done()
        self.assertEqual(job.state, "done")
        self.assertTrue(job.completed_at)

    def test_button_set_to_done_from_done(self):
        """Forcing done on an already-done job should be idempotent."""
        job = self._create_job()
        first_completed = fields.Datetime.now() - timedelta(hours=1)
        job.write({"state": "done", "completed_at": first_completed})
        job.button_set_to_done()
        self.assertEqual(job.state, "done")
        # completed_at gets updated to now
        self.assertNotEqual(
            fields.Datetime.to_datetime(job.completed_at),
            fields.Datetime.to_datetime(first_completed),
        )

    def test_button_set_to_failed_from_done(self):
        """Can a done job be forced to failed? This should work."""
        job = self._create_job()
        job.write({"state": "done", "completed_at": fields.Datetime.now()})
        job.button_set_to_failed()
        self.assertEqual(job.state, "failed")

    def test_button_cancelled_from_started(self):
        """Cancelling a started job should work."""
        job = self._create_job()
        job.write({"state": "started", "heartbeat": fields.Datetime.now()})
        job.button_cancelled()
        self.assertEqual(job.state, "cancelled")
        self.assertTrue(job.cancelled_at)

    def test_button_cancelled_from_done(self):
        """Cancelling an already-done job. System should allow it."""
        job = self._create_job()
        job.write({"state": "done", "completed_at": fields.Datetime.now()})
        job.button_cancelled()
        self.assertEqual(job.state, "cancelled")

    def test_duration_computed_on_set_to_done_with_started_at(self):
        """Duration should be correctly computed when started_at is set."""
        job = self._create_job()
        started = fields.Datetime.now() - timedelta(seconds=42)
        job.write({"state": "started", "started_at": started})
        job.button_set_to_done()
        self.assertEqual(job.state, "done")
        self.assertGreaterEqual(job.duration, 42)

    def test_duration_not_set_without_started_at(self):
        """Duration should not be set if started_at is missing."""
        job = self._create_job()
        job.button_set_to_done()
        self.assertFalse(job.duration)
