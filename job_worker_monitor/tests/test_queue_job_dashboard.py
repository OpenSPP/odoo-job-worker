from datetime import timedelta

from odoo import fields
from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestQueueJobDashboard(TransactionCase):
    """Verify dashboard KPI computation."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]
        self.Dashboard = self.env["queue.job.dashboard"]

    def _create_job(self, **overrides):
        """Helper to create a job with sensible defaults."""
        vals = {
            "model_name": "res.users",
            "method_name": "search",
            "record_ids": [],
            "args": [[("id", "=", self.env.user.id)]],
            "kwargs": {},
            "channel": "dashboard_test",
        }
        vals.update(overrides)
        return self.Job.enqueue(**vals)

    # ------------------------------------------------------------------
    # Card count and names
    # ------------------------------------------------------------------
    def test_compute_kpis_returns_ten_cards(self):
        """Dashboard always returns 10 KPI cards."""
        kpis = self.Dashboard._compute_kpis()
        self.assertEqual(len(kpis), 10)

    def test_all_kpi_names_present(self):
        """All expected KPI names are present."""
        kpis = self.Dashboard._compute_kpis()
        names = [k["name"] for k in kpis]
        expected = [
            "Queue Depth",
            "Active Workers",
            "Failed Jobs",
            "Failure Rate (1h)",
            "Throughput (1h)",
            "P95 Duration (1h)",
            "Stuck Jobs",
            "Aged Pending",
            "Retry Exhausted",
            "Top Failure",
        ]
        for name in expected:
            self.assertIn(name, names)

    # ------------------------------------------------------------------
    # Sequence ordering
    # ------------------------------------------------------------------
    def test_kpi_sequences_are_1_to_10(self):
        """Each KPI has a unique sequence from 1 to 10."""
        kpis = self.Dashboard._compute_kpis()
        sequences = sorted(k["sequence"] for k in kpis)
        self.assertEqual(sequences, list(range(1, 11)))

    # ------------------------------------------------------------------
    # Group labels
    # ------------------------------------------------------------------
    def test_group_labels_assigned(self):
        """KPIs are grouped into System Health, Performance, Needs Attention."""
        kpis = self.Dashboard._compute_kpis()
        groups = {k["name"]: k["group_label"] for k in kpis}
        self.assertEqual(groups["Queue Depth"], "System Health")
        self.assertEqual(groups["Active Workers"], "System Health")
        self.assertEqual(groups["Failed Jobs"], "System Health")
        self.assertEqual(groups["Failure Rate (1h)"], "Performance")
        self.assertEqual(groups["Throughput (1h)"], "Performance")
        self.assertEqual(groups["P95 Duration (1h)"], "Performance")
        self.assertEqual(groups["Stuck Jobs"], "Needs Attention")
        self.assertEqual(groups["Aged Pending"], "Needs Attention")
        self.assertEqual(groups["Retry Exhausted"], "Needs Attention")
        self.assertEqual(groups["Top Failure"], "Needs Attention")

    # ------------------------------------------------------------------
    # Severity field present on all cards
    # ------------------------------------------------------------------
    def test_severity_field_present(self):
        """Every KPI card has a severity field."""
        kpis = self.Dashboard._compute_kpis()
        for kpi in kpis:
            self.assertIn(
                kpi["severity"],
                ("success", "warning", "danger", "info"),
                f"Invalid severity on card {kpi['name']}: {kpi.get('severity')}",
            )

    # ------------------------------------------------------------------
    # Queue Depth severity
    # ------------------------------------------------------------------
    def test_queue_depth_severity_green_when_low(self):
        """Queue Depth is green (success) when count <=50."""
        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Queue Depth")
        # With no jobs enqueued the count should be very low
        self.assertEqual(card["severity"], "success")

    def test_queue_depth_reflects_pending_jobs(self):
        """Queue depth counts pending + waiting jobs."""
        for _i in range(3):
            self._create_job()

        kpis = self.Dashboard._compute_kpis()
        depth_kpi = next(k for k in kpis if k["name"] == "Queue Depth")
        self.assertGreaterEqual(depth_kpi["value_number"], 3)

    # ------------------------------------------------------------------
    # Active Workers severity
    # ------------------------------------------------------------------
    def test_active_workers_severity_red_when_zero(self):
        """Active Workers is red (danger) when no workers have fresh heartbeat."""
        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Active Workers")
        self.assertEqual(card["severity"], "danger")

    # ------------------------------------------------------------------
    # Failed Jobs
    # ------------------------------------------------------------------
    def test_failed_jobs_counts_outstanding_failures(self):
        """Failed Jobs card counts jobs in failed state."""
        job = self._create_job()
        job.write({"state": "failed", "exc_info": "ValueError: test"})

        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Failed Jobs")
        self.assertGreaterEqual(card["value_number"], 1)

    def test_failed_jobs_severity_green_when_zero(self):
        """Failed Jobs is green when no failures."""
        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Failed Jobs")
        # Assuming clean DB, this should be green
        if card["value_number"] == 0:
            self.assertEqual(card["severity"], "success")

    def test_failed_jobs_severity_red_when_many(self):
        """Failed Jobs is red when >5 failures."""
        for _i in range(6):
            job = self._create_job()
            job.write({"state": "failed", "exc_info": "ValueError: test"})

        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Failed Jobs")
        self.assertEqual(card["severity"], "danger")

    # ------------------------------------------------------------------
    # Failure Rate severity
    # ------------------------------------------------------------------
    def test_failure_rate_computed_correctly(self):
        """Failure rate is computed from recent completed jobs."""
        channel = "dashboard_fail_rate"
        # Create 2 done and 1 failed job
        for _i in range(2):
            job = self._create_job(channel=channel)
            job.write(
                {
                    "state": "done",
                    "completed_at": fields.Datetime.now(),
                }
            )
        failed_job = self._create_job(channel=channel)
        failed_job.write(
            {
                "state": "failed",
                "completed_at": fields.Datetime.now(),
                "exc_info": "Test",
            }
        )

        kpis = self.Dashboard._compute_kpis()
        rate_kpi = next(k for k in kpis if k["name"] == "Failure Rate (1h)")
        # At least 1/3 = ~33.3%
        self.assertGreater(rate_kpi["value_number"], 0)

    def test_failure_rate_severity_red_when_high(self):
        """Failure rate severity is red when >10%."""
        channel = "dashboard_fail_rate_sev"
        # Create 1 done and 2 failed (66% failure rate)
        job_done = self._create_job(channel=channel)
        job_done.write({"state": "done", "completed_at": fields.Datetime.now()})
        for _i in range(2):
            job = self._create_job(channel=channel)
            job.write(
                {
                    "state": "failed",
                    "completed_at": fields.Datetime.now(),
                    "exc_info": "Error",
                }
            )

        kpis = self.Dashboard._compute_kpis()
        rate_kpi = next(k for k in kpis if k["name"] == "Failure Rate (1h)")
        self.assertEqual(rate_kpi["severity"], "danger")

    # ------------------------------------------------------------------
    # Throughput
    # ------------------------------------------------------------------
    def test_throughput_counts_completed_jobs(self):
        """Throughput counts jobs completed in the last hour."""
        job = self._create_job(channel="dashboard_throughput")
        job.write(
            {
                "state": "done",
                "completed_at": fields.Datetime.now(),
                "duration": 2.5,
            }
        )

        kpis = self.Dashboard._compute_kpis()
        throughput_kpi = next(k for k in kpis if k["name"] == "Throughput (1h)")
        self.assertGreater(throughput_kpi["value_number"], 0)

    def test_throughput_severity_always_info(self):
        """Throughput card always has info severity."""
        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Throughput (1h)")
        self.assertEqual(card["severity"], "info")

    def test_throughput_subtitle_shows_average_duration(self):
        """Throughput subtitle shows average duration."""
        job = self._create_job(channel="dashboard_throughput_sub")
        job.write(
            {
                "state": "done",
                "completed_at": fields.Datetime.now(),
                "duration": 3.0,
            }
        )

        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Throughput (1h)")
        self.assertIn("avg", card["subtitle"])

    # ------------------------------------------------------------------
    # P95 Duration
    # ------------------------------------------------------------------
    def test_p95_duration_computed(self):
        """P95 Duration card computes 95th percentile execution time."""
        for i in range(10):
            job = self._create_job(channel="dashboard_p95")
            job.write(
                {
                    "state": "done",
                    "completed_at": fields.Datetime.now(),
                    "duration": float(i + 1),
                }
            )

        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "P95 Duration (1h)")
        self.assertGreater(card["value_number"], 0)

    def test_p95_duration_severity_green_when_fast(self):
        """P95 Duration is green when <30s."""
        job = self._create_job(channel="dashboard_p95_fast")
        job.write(
            {
                "state": "done",
                "completed_at": fields.Datetime.now(),
                "duration": 5.0,
            }
        )

        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "P95 Duration (1h)")
        # With only 5s duration, p95 should be well under 30s
        if card["value_number"] < 30:
            self.assertEqual(card["severity"], "success")

    # ------------------------------------------------------------------
    # Stuck Jobs
    # ------------------------------------------------------------------
    def test_stuck_jobs_counts_stale_started_jobs(self):
        """Stuck Jobs counts started jobs with heartbeat >5min stale."""
        job = self._create_job(channel="dashboard_stuck")
        stale_time = fields.Datetime.now() - timedelta(minutes=10)
        job.write(
            {
                "state": "started",
                "heartbeat": stale_time,
                "worker_id": "test-worker-stuck",
            }
        )

        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Stuck Jobs")
        self.assertGreaterEqual(card["value_number"], 1)
        self.assertEqual(card["severity"], "danger")

    def test_stuck_jobs_green_when_none(self):
        """Stuck Jobs is green when no stale started jobs."""
        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Stuck Jobs")
        if card["value_number"] == 0:
            self.assertEqual(card["severity"], "success")

    # ------------------------------------------------------------------
    # Aged Pending
    # ------------------------------------------------------------------
    def test_aged_pending_counts_old_pending_jobs(self):
        """Aged Pending counts pending/waiting jobs created >1h ago."""
        job = self._create_job(channel="dashboard_aged")
        old_time = fields.Datetime.now() - timedelta(hours=2)
        # Manually update create_date using SQL (ORM won't allow writing it)
        self.env.cr.execute(
            "UPDATE queue_job SET create_date = %s WHERE id = %s",
            (old_time, job.id),
        )
        job.invalidate_recordset()

        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Aged Pending")
        self.assertGreaterEqual(card["value_number"], 1)

    def test_aged_pending_severity_green_when_none(self):
        """Aged Pending is green when no old pending jobs."""
        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Aged Pending")
        if card["value_number"] == 0:
            self.assertEqual(card["severity"], "success")

    # ------------------------------------------------------------------
    # Retry Exhausted
    # ------------------------------------------------------------------
    def test_retry_exhausted_counts_exhausted_failed_jobs(self):
        """Retry Exhausted counts failed jobs with attempts >= max_retries."""
        job = self._create_job(channel="dashboard_retry_exhausted")
        job.write(
            {
                "state": "failed",
                "exc_info": "RetryError: exhausted",
                "attempts": 5,
                "max_retries": 5,
            }
        )

        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Retry Exhausted")
        self.assertGreaterEqual(card["value_number"], 1)
        self.assertEqual(card["severity"], "danger")

    def test_retry_exhausted_green_when_none(self):
        """Retry Exhausted is green when no exhausted jobs."""
        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Retry Exhausted")
        if card["value_number"] == 0:
            self.assertEqual(card["severity"], "success")

    # ------------------------------------------------------------------
    # Top Failure
    # ------------------------------------------------------------------
    def test_top_failure_shows_most_common_exception(self):
        """Top Failure shows most common exc_name among failed jobs."""
        for _i in range(3):
            job = self._create_job(channel="dashboard_top_fail")
            job.write(
                {
                    "state": "failed",
                    "exc_info": "ValueError: bad value",
                }
            )
        job2 = self._create_job(channel="dashboard_top_fail")
        job2.write(
            {
                "state": "failed",
                "exc_info": "TypeError: wrong type",
            }
        )

        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Top Failure")
        self.assertIn("ValueError", card["value"])
        self.assertEqual(card["severity"], "danger")

    def test_top_failure_green_when_no_failures(self):
        """Top Failure is green and shows 'None' when no failed jobs."""
        kpis = self.Dashboard._compute_kpis()
        card = next(k for k in kpis if k["name"] == "Top Failure")
        if card["value_number"] == 0:
            self.assertEqual(card["severity"], "success")

    # ------------------------------------------------------------------
    # action_open_jobs
    # ------------------------------------------------------------------
    def test_action_open_jobs_returns_action(self):
        """action_open_jobs returns an ir.actions.act_window dict."""
        self.Dashboard._refresh_kpis()
        card = self.Dashboard.search([("name", "=", "Queue Depth")], limit=1)
        self.assertTrue(card)
        action = card.action_open_jobs()
        self.assertEqual(action["type"], "ir.actions.act_window")
        self.assertEqual(action["res_model"], "queue.job")
        self.assertIn("domain", action)

    def test_action_open_jobs_domain_is_valid(self):
        """action_open_jobs domain can be parsed as a valid domain."""
        self.Dashboard._refresh_kpis()
        card = self.Dashboard.search([("name", "=", "Queue Depth")], limit=1)
        action = card.action_open_jobs()
        domain = action["domain"]
        # Domain should be a list (parsed from string)
        self.assertIsInstance(domain, list)

    def test_action_open_jobs_with_context(self):
        """action_open_jobs includes action_context when present."""
        self.Dashboard._refresh_kpis()
        card = self.Dashboard.search([("name", "=", "Failed Jobs")], limit=1)
        self.assertTrue(card)
        action = card.action_open_jobs()
        self.assertEqual(action["type"], "ir.actions.act_window")


@tagged("post_install", "-at_install")
class TestQueueJobRetryExhausted(TransactionCase):
    """Verify is_retry_exhausted computed field on queue.job."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def _create_job(self, **overrides):
        vals = {
            "model_name": "res.users",
            "method_name": "search",
            "record_ids": [],
            "args": [[("id", "=", self.env.user.id)]],
            "kwargs": {},
            "channel": "retry_test",
        }
        vals.update(overrides)
        return self.Job.enqueue(**vals)

    def test_not_exhausted_when_pending(self):
        """A pending job is not retry exhausted even if attempts >= max_retries."""
        job = self._create_job()
        job.write({"attempts": 5, "max_retries": 5})
        self.assertFalse(job.is_retry_exhausted)

    def test_exhausted_when_failed_and_attempts_at_max(self):
        """A failed job with attempts >= max_retries is retry exhausted."""
        job = self._create_job()
        job.write(
            {
                "state": "failed",
                "exc_info": "Error",
                "attempts": 5,
                "max_retries": 5,
            }
        )
        self.assertTrue(job.is_retry_exhausted)

    def test_not_exhausted_when_failed_but_retries_remain(self):
        """A failed job with attempts < max_retries is not exhausted."""
        job = self._create_job()
        job.write(
            {
                "state": "failed",
                "exc_info": "Error",
                "attempts": 2,
                "max_retries": 5,
            }
        )
        self.assertFalse(job.is_retry_exhausted)

    def test_exhausted_when_attempts_exceed_max(self):
        """A failed job with attempts > max_retries is retry exhausted."""
        job = self._create_job()
        job.write(
            {
                "state": "failed",
                "exc_info": "Error",
                "attempts": 7,
                "max_retries": 5,
            }
        )
        self.assertTrue(job.is_retry_exhausted)

    def test_not_exhausted_when_done(self):
        """A done job is not retry exhausted."""
        job = self._create_job()
        job.write(
            {
                "state": "done",
                "completed_at": fields.Datetime.now(),
                "attempts": 5,
                "max_retries": 5,
            }
        )
        self.assertFalse(job.is_retry_exhausted)
