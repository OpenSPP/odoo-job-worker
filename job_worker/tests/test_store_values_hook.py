from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestJobStoreValuesHook(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_default_hook_returns_empty_dict(self):
        """The base _job_store_values returns an empty dict."""
        partner = self.env["res.partner"].create({"name": "Hook Test"})
        result = partner._job_store_values({})
        self.assertEqual(result, {})

    def test_hook_values_applied_to_job(self):
        """Values returned by _job_store_values are written to the job."""
        partner = self.env["res.partner"].create({"name": "Apply Test"})
        original = type(partner)._job_store_values

        def custom_store_values(self_rec, job_vals):
            return {"priority": 99}

        type(partner)._job_store_values = custom_store_values
        try:
            job = partner.with_delay().write({"name": "Custom Priority"})
            self.assertEqual(job.priority, 99)
        finally:
            type(partner)._job_store_values = original

    def test_hook_receives_job_vals(self):
        """The hook receives the vals dict that will be used for create."""
        partner = self.env["res.partner"].create({"name": "Vals Test"})
        captured_vals = {}
        original = type(partner)._job_store_values

        def capture_store_values(self_rec, job_vals):
            captured_vals.update(job_vals)
            return {}

        type(partner)._job_store_values = capture_store_values
        try:
            partner.with_delay(channel="test_channel").write({"name": "Check Vals"})
            self.assertEqual(captured_vals.get("channel"), "test_channel")
            self.assertIn("payload", captured_vals)
        finally:
            type(partner)._job_store_values = original
