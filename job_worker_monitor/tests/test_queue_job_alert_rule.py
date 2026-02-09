from odoo import fields
from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestQueueJobAlertRule(TransactionCase):
    """Verify alert rule evaluation and notification."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]
        self.AlertRule = self.env["queue.job.alert.rule"]

    def test_evaluate_failed_count_metric(self):
        """Alert triggers when failed job count exceeds threshold."""
        # Create a failed job with recent completed_at
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="alert_test",
        )
        job.write(
            {
                "state": "failed",
                "completed_at": fields.Datetime.now(),
                "exc_info": "Test failure",
            }
        )

        rule = self.AlertRule.create(
            {
                "name": "Test Failed Count Alert",
                "metric": "failed_count",
                "operator": ">",
                "threshold": 0,
                "evaluation_window_minutes": 5,
                "cooldown_minutes": 1,
                "notification_user_ids": [(4, self.env.user.id)],
            }
        )

        value = rule._evaluate_metric()
        self.assertGreater(value, 0)
        self.assertTrue(rule._check_threshold(value))

    def test_evaluate_queue_depth_metric(self):
        """Queue depth metric counts pending + waiting jobs."""
        channel = "alert_depth_test"
        for _i in range(5):
            self.Job.enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", self.env.user.id)]],
                kwargs={},
                channel=channel,
            )

        rule = self.AlertRule.create(
            {
                "name": "Test Queue Depth Alert",
                "metric": "queue_depth",
                "channel": channel,
                "operator": ">=",
                "threshold": 3,
                "evaluation_window_minutes": 5,
                "cooldown_minutes": 1,
            }
        )

        value = rule._evaluate_metric()
        self.assertGreaterEqual(value, 5)
        self.assertTrue(rule._check_threshold(value))

    def test_evaluate_all_rules_triggers_notification(self):
        """_evaluate_all_rules posts a message when triggered."""
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="alert_all_rules",
        )
        job.write(
            {
                "state": "failed",
                "completed_at": fields.Datetime.now(),
                "exc_info": "Failure",
            }
        )

        rule = self.AlertRule.create(
            {
                "name": "Integration Alert Test",
                "metric": "failed_count",
                "operator": ">",
                "threshold": 0,
                "evaluation_window_minutes": 5,
                "cooldown_minutes": 0,
                "notification_user_ids": [(4, self.env.user.id)],
            }
        )

        self.AlertRule._evaluate_all_rules()

        rule.invalidate_recordset()
        self.assertTrue(rule.last_triggered_at)
        # Check that a message was posted
        messages = self.env["mail.message"].search(
            [("res_id", "=", rule.id), ("model", "=", "queue.job.alert.rule")]
        )
        self.assertTrue(messages)

    def test_cooldown_prevents_repeated_alerts(self):
        """Alert does not re-trigger within cooldown period."""
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="alert_cooldown",
        )
        job.write(
            {
                "state": "failed",
                "completed_at": fields.Datetime.now(),
                "exc_info": "Failure",
            }
        )

        rule = self.AlertRule.create(
            {
                "name": "Cooldown Test",
                "metric": "failed_count",
                "operator": ">",
                "threshold": 0,
                "evaluation_window_minutes": 5,
                "cooldown_minutes": 60,
                "notification_user_ids": [(4, self.env.user.id)],
            }
        )

        # First evaluation triggers
        self.AlertRule._evaluate_all_rules()
        rule.invalidate_recordset()
        first_triggered = rule.last_triggered_at
        self.assertTrue(first_triggered)

        # Count messages
        messages_before = self.env["mail.message"].search_count(
            [("res_id", "=", rule.id), ("model", "=", "queue.job.alert.rule")]
        )

        # Second evaluation within cooldown does NOT add message
        self.AlertRule._evaluate_all_rules()
        messages_after = self.env["mail.message"].search_count(
            [("res_id", "=", rule.id), ("model", "=", "queue.job.alert.rule")]
        )
        self.assertEqual(messages_before, messages_after)

    def test_threshold_operators(self):
        """Various operators are evaluated correctly."""
        rule = self.AlertRule.create(
            {
                "name": "Operator Test",
                "metric": "queue_depth",
                "operator": ">",
                "threshold": 100,
                "evaluation_window_minutes": 5,
                "cooldown_minutes": 1,
            }
        )

        self.assertTrue(rule._check_threshold(101))
        self.assertFalse(rule._check_threshold(100))
        self.assertFalse(rule._check_threshold(99))

        rule.operator = ">="
        self.assertTrue(rule._check_threshold(100))
        self.assertFalse(rule._check_threshold(99))

        rule.operator = "<"
        self.assertTrue(rule._check_threshold(99))
        self.assertFalse(rule._check_threshold(100))

        rule.operator = "<="
        self.assertTrue(rule._check_threshold(100))
        self.assertFalse(rule._check_threshold(101))

    def test_action_test_rule_returns_notification(self):
        """action_test_rule returns a display_notification action."""
        rule = self.AlertRule.create(
            {
                "name": "Test Button",
                "metric": "queue_depth",
                "operator": ">",
                "threshold": 999999,
                "evaluation_window_minutes": 5,
                "cooldown_minutes": 1,
            }
        )

        result = rule.action_test_rule()
        self.assertEqual(result["type"], "ir.actions.client")
        self.assertEqual(result["tag"], "display_notification")
