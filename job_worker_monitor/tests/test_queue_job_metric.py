from datetime import timedelta

from odoo import fields
from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestQueueJobMetric(TransactionCase):
    """Verify metric snapshot collection and rollup."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]
        self.Metric = self.env["queue.job.metric"]

    def test_collect_5min_snapshot_creates_records(self):
        """Running _collect_5min_snapshot creates metric rows."""
        # Create a completed job to have data
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="metric_test",
        )
        job.write(
            {
                "state": "done",
                "completed_at": fields.Datetime.now(),
                "duration": 1.5,
            }
        )

        self.Metric._collect_5min_snapshot()

        metrics = self.Metric.search([("channel", "=", "metric_test")])
        self.assertTrue(metrics)
        metric = metrics[0]
        self.assertEqual(metric.period, "5min")
        self.assertGreater(metric.jobs_completed, 0)

    def test_collect_5min_snapshot_counts_pending_as_queue_depth(self):
        """Pending jobs appear as queue_depth."""
        channel = "metric_depth_test"
        for _i in range(3):
            self.Job.enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", self.env.user.id)]],
                kwargs={},
                channel=channel,
            )

        self.Metric._collect_5min_snapshot()

        metrics = self.Metric.search([("channel", "=", channel)])
        self.assertTrue(metrics)
        self.assertGreaterEqual(metrics[0].queue_depth, 3)

    def test_collect_5min_snapshot_no_jobs_produces_no_metrics(self):
        """When no jobs exist for a channel, no metric record should
        be created for that channel."""
        # Use a unique channel name with no jobs
        channel = "metric_empty_channel_test"
        self.Metric._collect_5min_snapshot()
        metrics = self.Metric.search([("channel", "=", channel)])
        self.assertFalse(metrics)

    def test_collect_5min_snapshot_boundary_completed_at_equals_now(self):
        """A job whose completed_at equals the snapshot's 'now' should
        be included (the <= boundary)."""
        from datetime import datetime
        from unittest.mock import patch

        fixed = datetime(2026, 6, 15, 12, 0, 0)
        channel = "metric_boundary_test"
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel=channel,
        )
        job.write({"state": "done", "completed_at": fixed, "duration": 2.0})

        with patch("odoo.fields.Datetime.now", return_value=fixed):
            self.Metric._collect_5min_snapshot()

        metrics = self.Metric.search([("channel", "=", channel)])
        self.assertTrue(metrics, "Metric should be created for boundary timestamp")
        self.assertEqual(metrics[0].jobs_completed, 1)

    def test_rollup_hourly_aggregates_5min_rows(self):
        """_rollup_hourly aggregates 5-min rows into hourly."""
        now = fields.Datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        # Create some 5-min snapshots in the previous hour
        prev_hour_start = hour_start - timedelta(hours=1)
        for i in range(3):
            self.Metric.create(
                {
                    "snapshot_at": prev_hour_start + timedelta(minutes=i * 5),
                    "channel": "rollup_test",
                    "period": "5min",
                    "jobs_completed": 10,
                    "jobs_failed": 1,
                    "average_duration": 2.0,
                    "p95_duration": 5.0,
                    "queue_depth": 20,
                    "active_workers": 2,
                }
            )

        self.Metric._rollup_hourly()

        hourly = self.Metric.search(
            [("channel", "=", "rollup_test"), ("period", "=", "1hour")]
        )
        self.assertTrue(hourly)
        self.assertEqual(hourly[0].jobs_completed, 30)
        self.assertEqual(hourly[0].jobs_failed, 3)

    def test_rollup_daily_aggregates_hourly_rows(self):
        """_rollup_daily aggregates hourly rows into daily."""
        now = fields.Datetime.now()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        prev_day_start = day_start - timedelta(days=1)
        for i in range(4):
            self.Metric.create(
                {
                    "snapshot_at": prev_day_start + timedelta(hours=i),
                    "channel": "daily_test",
                    "period": "1hour",
                    "jobs_completed": 100,
                    "jobs_failed": 5,
                    "average_duration": 3.0,
                    "p95_duration": 8.0,
                    "queue_depth": 15,
                    "active_workers": 3,
                }
            )

        self.Metric._rollup_daily()

        daily = self.Metric.search(
            [("channel", "=", "daily_test"), ("period", "=", "1day")]
        )
        self.assertTrue(daily)
        self.assertEqual(daily[0].jobs_completed, 400)
        self.assertEqual(daily[0].jobs_failed, 20)
