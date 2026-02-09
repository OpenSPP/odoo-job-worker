from unittest.mock import patch

from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestDemoTask(TransactionCase):
    """Unit tests for demo.task queued methods (called directly, no queue)."""

    def setUp(self):
        super().setUp()
        self.task = self.env["demo.task"].create({"name": "Test Task"})
        # Mock sleep so unit tests don't actually wait
        patcher = patch("time.sleep")
        self.mock_sleep = patcher.start()
        self.addCleanup(patcher.stop)

    def test_process_basic_sets_done_and_log(self):
        self.task.process_basic()
        self.assertEqual(self.task.state, "done")
        self.assertTrue(self.task.completed_at)
        self.assertIn("process_basic", self.task.log)

    def test_process_basic_returns_result(self):
        result = self.task.process_basic()
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["task_id"], self.task.id)

    def test_process_step_one_returns_result(self):
        result = self.task.process_step_one()
        self.assertEqual(result["step"], 1)
        self.assertEqual(result["task_id"], self.task.id)

    def test_process_step_two_returns_result(self):
        result = self.task.process_step_two()
        self.assertEqual(result["step"], 2)
        self.assertEqual(result["task_id"], self.task.id)

    def test_process_basic_appends_to_existing_log(self):
        self.task.write({"log": "previous entry\n"})
        self.task.process_basic()
        self.assertIn("previous entry", self.task.log)
        self.assertIn("process_basic", self.task.log)

    def test_process_slow_sets_done(self):
        self.mock_sleep.reset_mock()
        self.task.process_slow()
        self.mock_sleep.assert_any_call(3)
        self.assertEqual(self.task.state, "done")
        self.assertTrue(self.task.completed_at)
        self.assertIn("process_slow", self.task.log)

    def test_process_slow_custom_duration(self):
        self.mock_sleep.reset_mock()
        self.task.process_slow(duration=5)
        self.mock_sleep.assert_any_call(5)
        self.assertEqual(self.task.state, "done")

    def test_process_unreliable_succeeds_when_random_high(self):
        with patch("random.random", return_value=0.8):
            self.task.process_unreliable()
        self.assertEqual(self.task.state, "done")
        self.assertTrue(self.task.completed_at)
        self.assertIn("process_unreliable", self.task.log)

    def test_process_unreliable_raises_when_random_low(self):
        with patch("random.random", return_value=0.3):
            with self.assertRaises(RuntimeError) as ctx:
                self.task.process_unreliable()
            self.assertIn("simulated", str(ctx.exception).lower())
        self.assertNotEqual(self.task.state, "done")

    def test_process_unreliable_boundary_at_0_7(self):
        # Exactly 0.7 should succeed (>= 0.7)
        with patch("random.random", return_value=0.7):
            self.task.process_unreliable()
        self.assertEqual(self.task.state, "done")

    def test_process_always_fails_raises_runtime_error(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.task.process_always_fails()
        self.assertIn("always fails", str(ctx.exception).lower())

    def test_process_step_one_writes_log_but_not_done(self):
        self.task.process_step_one()
        self.assertNotEqual(self.task.state, "done")
        self.assertIn("step_one", self.task.log)

    def test_process_step_two_sets_done(self):
        self.task.process_step_two()
        self.assertEqual(self.task.state, "done")
        self.assertTrue(self.task.completed_at)
        self.assertIn("step_two", self.task.log)

    def test_default_name_auto_generated(self):
        """Creating a task without a name should auto-generate one from sequence."""
        task = self.env["demo.task"].create({})
        self.assertTrue(task.name)
        self.assertIn("DT/", task.name)

    def test_default_name_can_be_overridden(self):
        """Explicit name should override the auto-generated default."""
        task = self.env["demo.task"].create({"name": "My Custom Task"})
        self.assertEqual(task.name, "My Custom Task")

    def test_default_name_increments(self):
        """Each new task should get a unique sequential name."""
        task_1 = self.env["demo.task"].create({})
        task_2 = self.env["demo.task"].create({})
        self.assertNotEqual(task_1.name, task_2.name)

    def test_action_reset_to_draft_clears_state(self):
        self.task.process_basic()
        self.assertEqual(self.task.state, "done")
        self.assertTrue(self.task.completed_at)
        self.assertTrue(self.task.log)

        self.task.action_reset_to_draft()
        self.assertEqual(self.task.state, "draft")
        self.assertFalse(self.task.completed_at)
        self.assertFalse(self.task.log)
