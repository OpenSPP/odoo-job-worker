import json

from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestDemoEnqueue(TransactionCase):
    """Integration tests for demo.task button actions creating queue.job records."""

    def setUp(self):
        super().setUp()
        self.task = self.env["demo.task"].create({"name": "Enqueue Test"})
        self.Job = self.env["queue.job"]

    def _get_last_job(self):
        """Return the most recently created queue.job."""
        return self.Job.search([], order="id desc", limit=1)

    def _get_job_payload(self, job):
        """Extract and return the payload dict from a queue.job."""
        payload = job.payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        return payload

    def test_action_enqueue_basic_creates_job_on_demo_channel(self):
        self.task.action_enqueue_basic()
        job = self._get_last_job()
        payload = self._get_job_payload(job)
        self.assertEqual(job.channel, "demo")
        self.assertEqual(payload["model"], "demo.task")
        self.assertEqual(payload["method"], "process_basic")
        self.assertEqual(payload["ids"], self.task.ids)
        self.assertEqual(self.task.state, "queued")

    def test_action_enqueue_throttled_creates_job_on_throttled_channel(self):
        self.task.action_enqueue_throttled()
        job = self._get_last_job()
        payload = self._get_job_payload(job)
        self.assertEqual(job.channel, "demo_throttled")
        self.assertEqual(payload["method"], "process_slow")

    def test_action_enqueue_high_priority(self):
        self.task.action_enqueue_high_priority()
        job = self._get_last_job()
        self.assertEqual(job.channel, "demo")
        self.assertEqual(job.priority, 1)

    def test_action_enqueue_low_priority(self):
        self.task.action_enqueue_low_priority()
        job = self._get_last_job()
        self.assertEqual(job.channel, "demo")
        self.assertEqual(job.priority, 20)

    def test_action_enqueue_delayed(self):
        self.task.action_enqueue_delayed()
        job = self._get_last_job()
        self.assertEqual(job.channel, "demo")
        self.assertTrue(job.scheduled_at, "Delayed job should have scheduled_at set")

    def test_action_enqueue_deduplicated_returns_same_job(self):
        self.task.action_enqueue_deduplicated()
        first_job = self._get_last_job()
        self.assertTrue(first_job.identity_key)
        expected_key = f"demo_task_{self.task.id}"
        self.assertEqual(first_job.identity_key, expected_key)

        # Second call should return the same job (deduplication)
        self.task.action_enqueue_deduplicated()
        second_job = self._get_last_job()
        self.assertEqual(first_job.id, second_job.id)

    def test_action_enqueue_retry_demo(self):
        self.task.action_enqueue_retry_demo()
        job = self._get_last_job()
        payload = self._get_job_payload(job)
        self.assertEqual(job.channel, "demo")
        self.assertEqual(job.max_retries, 5)
        self.assertEqual(payload["method"], "process_unreliable")

    def test_action_enqueue_always_fails(self):
        self.task.action_enqueue_always_fails()
        job = self._get_last_job()
        payload = self._get_job_payload(job)
        self.assertEqual(job.channel, "demo")
        self.assertEqual(job.max_retries, 2)
        self.assertEqual(payload["method"], "process_always_fails")

    def test_action_enqueue_chain_creates_two_jobs(self):
        self.task.action_enqueue_chain()
        jobs = self.Job.search(
            [("channel", "=", "demo")],
            order="id desc",
            limit=2,
        )
        self.assertEqual(len(jobs), 2)
        payloads = [self._get_job_payload(j) for j in jobs]
        methods = {p["method"] for p in payloads}
        self.assertIn("process_step_one", methods)
        self.assertIn("process_step_two", methods)

    def test_action_create_bulk_kitchen_sink(self):
        before_tasks = self.env["demo.task"].search_count([])
        before_jobs = self.Job.search_count([])
        self.task.action_create_bulk()
        after_tasks = self.env["demo.task"].search_count([])
        after_jobs = self.Job.search_count([])
        # Should create many tasks and jobs across channels/priorities/types
        self.assertGreater(after_tasks - before_tasks, 15)
        self.assertGreater(after_jobs - before_jobs, 20)
        # Verify a mix of channels
        demo_jobs = self.Job.search([("channel", "=", "demo")])
        throttled_jobs = self.Job.search([("channel", "=", "demo_throttled")])
        bulk_jobs = self.Job.search([("channel", "=", "demo_bulk")])
        self.assertTrue(len(demo_jobs) > 0)
        self.assertTrue(len(throttled_jobs) > 0)
        self.assertTrue(len(bulk_jobs) > 0)
        # Verify waiting jobs exist (from chains)
        waiting_jobs = self.Job.search([("state", "=", "waiting")])
        self.assertTrue(len(waiting_jobs) > 0)
        # Verify graph_uuid is set on some jobs
        graph_jobs = self.Job.search([("graph_uuid", "!=", False)])
        self.assertTrue(len(graph_jobs) > 0)
