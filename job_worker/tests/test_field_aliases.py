from odoo import fields
from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestFieldAliases(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def _make_job(self):
        return self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Alias Test"}],
            kwargs={},
        )

    def test_date_created_returns_create_date(self):
        job = self._make_job()
        self.assertTrue(job.create_date)
        self.assertEqual(
            fields.Datetime.to_datetime(job.date_created),
            fields.Datetime.to_datetime(job.create_date),
        )

    def test_date_started_returns_started_at(self):
        job = self._make_job()
        now = fields.Datetime.now()
        job.started_at = now
        job.invalidate_recordset()
        self.assertEqual(
            fields.Datetime.to_datetime(job.date_started),
            fields.Datetime.to_datetime(job.started_at),
        )

    def test_date_done_returns_completed_at(self):
        job = self._make_job()
        now = fields.Datetime.now()
        job.completed_at = now
        job.invalidate_recordset()
        self.assertEqual(
            fields.Datetime.to_datetime(job.date_done),
            fields.Datetime.to_datetime(job.completed_at),
        )

    def test_exec_time_returns_duration(self):
        job = self._make_job()
        job.duration = 42.5
        job.invalidate_recordset()
        self.assertEqual(job.exec_time, 42.5)

    def test_date_cancelled_returns_cancelled_at(self):
        job = self._make_job()
        now = fields.Datetime.now()
        job.cancelled_at = now
        job.invalidate_recordset()
        self.assertEqual(
            fields.Datetime.to_datetime(job.date_cancelled),
            fields.Datetime.to_datetime(job.cancelled_at),
        )

    def test_date_started_inverse(self):
        """Writing to date_started updates started_at."""
        job = self._make_job()
        now = fields.Datetime.now()
        job.date_started = now
        job.invalidate_recordset()
        self.assertEqual(
            fields.Datetime.to_datetime(job.started_at),
            fields.Datetime.to_datetime(now),
        )

    def test_date_done_inverse(self):
        """Writing to date_done updates completed_at."""
        job = self._make_job()
        now = fields.Datetime.now()
        job.date_done = now
        job.invalidate_recordset()
        self.assertEqual(
            fields.Datetime.to_datetime(job.completed_at),
            fields.Datetime.to_datetime(now),
        )

    def test_exec_time_inverse(self):
        """Writing to exec_time updates duration."""
        job = self._make_job()
        job.exec_time = 99.9
        job.invalidate_recordset()
        self.assertAlmostEqual(job.duration, 99.9)

    def test_date_started_search(self):
        """Searching by date_started delegates to started_at."""
        job = self._make_job()
        now = fields.Datetime.now()
        job.started_at = now
        found = self.Job.search([("date_started", "=", now)])
        self.assertIn(job, found)

    def test_date_done_search(self):
        """Searching by date_done delegates to completed_at."""
        job = self._make_job()
        now = fields.Datetime.now()
        job.completed_at = now
        found = self.Job.search([("date_done", "=", now)])
        self.assertIn(job, found)

    def test_exec_time_search(self):
        """Searching by exec_time delegates to duration."""
        job = self._make_job()
        job.duration = 42.5
        found = self.Job.search([("exec_time", "=", 42.5)])
        self.assertIn(job, found)

    def test_date_cancelled_search(self):
        """Searching by date_cancelled delegates to cancelled_at."""
        job = self._make_job()
        now = fields.Datetime.now()
        job.cancelled_at = now
        found = self.Job.search([("date_cancelled", "=", now)])
        self.assertIn(job, found)
