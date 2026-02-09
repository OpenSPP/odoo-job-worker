from odoo import fields
from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestQueueLimitHealth(TransactionCase):
    """Verify computed health fields on queue.limit."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]
        self.Limit = self.env["queue.limit"]

    def test_health_fields_reflect_job_states(self):
        channel_name = "health_test_channel"
        limit = self.Limit.create({"name": channel_name, "limit": 5, "rate_limit": 0})

        # Create jobs in various states
        for state in ["pending", "pending", "started", "failed"]:
            job = self.Job.enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", self.env.user.id)]],
                kwargs={},
                channel=channel_name,
            )
            vals = {"state": state}
            if state == "started":
                vals["worker_id"] = "test-worker"
                vals["heartbeat"] = fields.Datetime.now()
            job.write(vals)

        limit.invalidate_recordset()
        self.assertEqual(limit.current_running, 1)
        self.assertEqual(limit.current_pending, 2)
        self.assertEqual(limit.current_failed, 1)
        self.assertAlmostEqual(limit.utilization_percent, 20.0, places=1)

    def test_utilization_zero_when_limit_zero(self):
        channel_name = "health_zero_limit"
        limit = self.Limit.create({"name": channel_name, "limit": 0, "rate_limit": 0})
        limit.invalidate_recordset()
        self.assertEqual(limit.utilization_percent, 0.0)

    def test_health_with_no_jobs(self):
        channel_name = "health_empty_channel"
        limit = self.Limit.create({"name": channel_name, "limit": 3, "rate_limit": 0})
        limit.invalidate_recordset()
        self.assertEqual(limit.current_running, 0)
        self.assertEqual(limit.current_pending, 0)
        self.assertEqual(limit.current_failed, 0)
        self.assertEqual(limit.utilization_percent, 0.0)
