from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestQueueJobExtendedFields(TransactionCase):
    """Verify model_name, method_name, exc_name computed fields."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_model_name_and_method_name_computed_from_payload(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=[],
            args=[{"name": "Test"}],
            kwargs={},
            channel="monitor_test",
        )
        self.assertEqual(job.model_name, "res.partner")
        self.assertEqual(job.method_name, "write")

    def test_exc_name_computed_from_exc_info(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="method_does_not_exist",
            record_ids=[],
            args=[],
            kwargs={},
            channel="monitor_exc",
        )
        try:
            job.run_now()
        except AttributeError:
            pass

        job.invalidate_recordset()
        self.assertTrue(job.exc_name)
        self.assertIn("AttributeError", job.exc_name)

    def test_exc_name_empty_when_no_exception(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "No Error"}],
            kwargs={},
            channel="monitor_noexc",
        )
        self.assertFalse(job.exc_name)

    def test_model_name_stored_and_searchable(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Searchable"}],
            kwargs={},
            channel="monitor_search",
        )
        found = self.Job.search(
            [("model_name", "=", "res.partner"), ("id", "=", job.id)]
        )
        self.assertEqual(found, job)
