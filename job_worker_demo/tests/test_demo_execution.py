import json
from unittest.mock import patch

from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestDemoExecution(TransactionCase):
    """E2E tests using run_now() to execute jobs in the current transaction."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]
        self.task = self.env["demo.task"].create({"name": "E2E Test"})
        # Mock sleep so e2e tests don't actually wait
        patcher = patch("time.sleep")
        patcher.start()
        self.addCleanup(patcher.stop)

    def _get_job_payload(self, job):
        """Extract and return the payload dict from a queue.job."""
        payload = job.payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        return payload

    def test_basic_job_updates_task_state_and_log(self):
        self.task.action_enqueue_basic()
        job = self.Job.search([], order="id desc", limit=1)
        self.assertEqual(job.state, "pending")

        job.run_now()

        self.assertEqual(job.state, "done")
        self.task.invalidate_recordset()
        self.assertEqual(self.task.state, "done")
        self.assertTrue(self.task.completed_at)
        self.assertIn("process_basic", self.task.log)

    def test_always_fails_job_marks_failed_with_exception(self):
        self.task.action_enqueue_always_fails()
        job = self.Job.search([], order="id desc", limit=1)

        try:
            job.run_now()
            self.fail("run_now() should raise for always-failing job")
        except RuntimeError:
            pass

        job.invalidate_recordset()
        self.assertEqual(job.state, "failed")
        self.assertIn("RuntimeError", job.exc_info or "")
        self.assertIn("always fails", job.exc_info or "")

    def test_basic_job_captures_result(self):
        self.task.action_enqueue_basic()
        job = self.Job.search([], order="id desc", limit=1)
        job.run_now()
        self.assertIsNotNone(job.result)
        self.assertEqual(job.result.get("status"), "completed")

    def test_chain_jobs_have_parent_child_relationship(self):
        self.task.action_enqueue_chain()
        jobs = self.Job.search(
            [("channel", "=", "demo")],
            order="id asc",
            limit=2,
        )
        self.assertEqual(len(jobs), 2)
        step_one = jobs[0]
        step_two = jobs[1]

        # step_two should be child of step_one
        self.assertEqual(step_two.parent_id.id, step_one.id)
        self.assertIn(step_two.id, step_one.child_ids.ids)
        # Both share the same graph_uuid
        self.assertTrue(step_one.graph_uuid)
        self.assertEqual(step_one.graph_uuid, step_two.graph_uuid)

    def test_chain_step_two_starts_waiting_becomes_pending(self):
        self.task.action_enqueue_chain()
        jobs = self.Job.search(
            [("channel", "=", "demo")],
            order="id asc",
            limit=2,
        )
        step_one = jobs[0]
        step_two = jobs[1]

        self.assertEqual(step_one.state, "pending")
        self.assertEqual(step_two.state, "waiting")

        # Execute step_one — step_two should become pending
        step_one.run_now()
        step_two.invalidate_recordset()
        self.assertEqual(step_two.state, "pending")

    def test_chain_both_jobs_execute_and_update_log(self):
        self.task.action_enqueue_chain()
        jobs = self.Job.search(
            [("channel", "=", "demo")],
            order="id asc",
            limit=2,
        )
        self.assertEqual(len(jobs), 2)

        # Execute step_one first to release step_two
        jobs[0].run_now()
        jobs[1].invalidate_recordset()
        jobs[1].run_now()

        self.task.invalidate_recordset()
        self.assertEqual(self.task.state, "done")
        self.assertIn("step_one", self.task.log)
        self.assertIn("step_two", self.task.log)

    def test_chain_result_populated_after_execution(self):
        self.task.action_enqueue_chain()
        jobs = self.Job.search(
            [("channel", "=", "demo")],
            order="id asc",
            limit=2,
        )
        step_one = jobs[0]
        step_two = jobs[1]

        step_one.run_now()
        self.assertIsNotNone(step_one.result)
        self.assertEqual(step_one.result.get("step"), 1)

        step_two.invalidate_recordset()
        step_two.run_now()
        self.assertIsNotNone(step_two.result)
        self.assertEqual(step_two.result.get("step"), 2)

    def test_slow_job_completes_with_mocked_sleep(self):
        self.task.action_enqueue_throttled()
        job = self.Job.search([], order="id desc", limit=1)
        payload = self._get_job_payload(job)
        self.assertEqual(payload["method"], "process_slow")

        with patch("time.sleep"):
            job.run_now()

        self.assertEqual(job.state, "done")
        self.task.invalidate_recordset()
        self.assertEqual(self.task.state, "done")
        self.assertIn("process_slow", self.task.log)
