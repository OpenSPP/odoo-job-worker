from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestComputedPayloadFields(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_model_name_extracted_from_payload(self):
        partner = self.env["res.partner"].create({"name": "Compute Test"})
        job = partner.with_delay().write({"name": "Done"})
        self.assertEqual(job.model_name, "res.partner")

    def test_method_name_extracted_from_payload(self):
        partner = self.env["res.partner"].create({"name": "Method Test"})
        job = partner.with_delay().write({"name": "Done"})
        self.assertEqual(job.method_name, "write")

    def test_record_ids_extracted_from_payload(self):
        partner = self.env["res.partner"].create({"name": "IDs Test"})
        job = partner.with_delay().write({"name": "Done"})
        self.assertEqual(job.record_ids, partner.ids)

    def test_func_string_format(self):
        partner = self.env["res.partner"].create({"name": "Func Test"})
        job = partner.with_delay().write({"name": "Done"})
        expected = f"res.partner.write({partner.ids})"
        self.assertEqual(job.func_string, expected)

    def test_name_populated_from_description(self):
        partner = self.env["res.partner"].create({"name": "Desc Test"})
        job = partner.with_delay(description="Update partner name").write(
            {"name": "Done"}
        )
        self.assertEqual(job.name, "Update partner name")

    def test_name_empty_when_no_description(self):
        partner = self.env["res.partner"].create({"name": "No Desc"})
        job = partner.with_delay().write({"name": "Done"})
        self.assertFalse(job.name)

    def test_computed_fields_searchable(self):
        partner = self.env["res.partner"].create({"name": "Search Test"})
        job = partner.with_delay().write({"name": "Done"})
        found = self.Job.search([("model_name", "=", "res.partner")])
        self.assertIn(job, found)
        found = self.Job.search([("method_name", "=", "write")])
        self.assertIn(job, found)

    def test_func_string_without_ids(self):
        """func_string with empty record_ids."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Created"}],
            kwargs={},
        )
        self.assertEqual(job.func_string, "res.partner.create([])")

    def test_computed_fields_for_enqueue_without_ids(self):
        """Computed fields work for jobs without record ids."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", 1)]],
            kwargs={},
        )
        self.assertEqual(job.model_name, "res.partner")
        self.assertEqual(job.method_name, "search")
        self.assertFalse(job.record_ids)
        self.assertEqual(job.func_string, "res.partner.search([])")
