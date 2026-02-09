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
        """Cancelling a parent should not auto-cancel children."""
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
        self.assertEqual(parent_job.state, "cancelled")
        # Child remains in waiting - explicit design choice
        self.assertEqual(child_job.state, "waiting")
