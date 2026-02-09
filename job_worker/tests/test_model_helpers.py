from datetime import timedelta

from odoo import fields
from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestModelHelpers(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_eta_inverse_and_search_map_to_scheduled_at(self):
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
        )
        new_eta = fields.Datetime.now().replace(microsecond=0) + timedelta(minutes=7)
        job.eta = new_eta
        job.invalidate_recordset()

        self.assertEqual(fields.Datetime.to_datetime(job.scheduled_at), new_eta)
        found = self.Job.search([("eta", "=", new_eta)])
        self.assertIn(job, found)

    def test_retry_inverse_and_search_map_to_attempts(self):
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
        )
        job.retry = 4
        job.invalidate_recordset()

        self.assertEqual(job.attempts, 4)
        found = self.Job.search([("retry", "=", 4)])
        self.assertIn(job, found)

    def test_button_state_helpers(self):
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
        )
        job.button_set_to_done()
        self.assertEqual(job.state, "done")
        job.button_set_to_failed()
        self.assertEqual(job.state, "failed")
