import json

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


@tagged("post_install", "-at_install")
class TestComputedFieldEdgeCases(TransactionCase):
    """Probe computed fields for unexpected inputs."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_display_name_with_non_dict_payload_falls_back(self):
        """_compute_display_name gracefully falls back to "Job #{id}"
        when the payload is not a valid dict."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Display Name Test"}],
            kwargs={},
            channel="display",
        )
        # Force payload to a non-dict JSON value via SQL
        self.env.cr.execute(
            "UPDATE queue_job SET payload = %s WHERE id = %s",
            (json.dumps("not a dict"), job.id),
        )
        job.invalidate_recordset()
        self.assertIn(f"Job #{job.id}", job.display_name)

    def test_duration_bucket_with_zero_duration(self):
        """Zero duration should be categorized as instant."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "zero duration"}],
            kwargs={},
            channel="duration_zero",
        )
        job.write({"duration": 0})
        self.assertEqual(job.duration_bucket, "instant")

    def test_duration_bucket_with_negative_duration(self):
        """Negative duration should be categorized as instant."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "negative duration"}],
            kwargs={},
            channel="duration_neg",
        )
        job.write({"duration": -5})
        self.assertEqual(job.duration_bucket, "instant")

    def test_duration_bucket_boundary_values(self):
        """Test exact boundary values for duration buckets."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "boundary"}],
            kwargs={},
            channel="duration_boundary",
        )

        test_cases = [
            (0.99, "instant"),
            (1, "fast"),
            (9.99, "fast"),
            (10, "moderate"),
            (59.99, "moderate"),
            (60, "slow"),
            (299.99, "slow"),
            (300, "very_slow"),
            (999999, "very_slow"),
        ]
        for duration, expected_bucket in test_cases:
            job.write({"duration": duration})
            self.assertEqual(
                job.duration_bucket,
                expected_bucket,
                f"Duration {duration} should be "
                f"'{expected_bucket}', got '{job.duration_bucket}'",
            )

    def test_payload_fields_with_missing_keys_payload(self):
        """Computed payload fields default to empty strings when
        standard keys (model, method, ids) are missing from a dict
        payload. func_string becomes '.([])' which is truthy."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "minimal payload"}],
            kwargs={},
            channel="minimal_payload",
        )
        # Write a payload that is valid JSON but missing expected keys
        job.write({"payload": {"unexpected": "data"}})
        job.invalidate_recordset()
        # Empty strings from dict.get() defaults
        self.assertEqual(job.model_name, "")
        self.assertEqual(job.method_name, "")
        # func_string is truthy but meaningless
        self.assertEqual(job.func_string, ".([])")

    def test_payload_fields_recompute_on_orm_write(self):
        """Stored computed fields recompute when payload is updated
        through the ORM."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "recompute payload"}],
            kwargs={},
            channel="recompute_payload",
        )
        self.assertEqual(job.model_name, "res.partner")
        self.assertEqual(job.method_name, "create")
        # Update payload through ORM to trigger recomputation
        job.write({"payload": {"model": "res.users", "method": "write", "ids": [1]}})
        self.assertEqual(job.model_name, "res.users")
        self.assertEqual(job.method_name, "write")

    def test_result_display_empty_when_no_result(self):
        """result_display should be empty string when result is falsy."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "no result"}],
            kwargs={},
            channel="no_result",
        )
        self.assertEqual(job.result_display, "")


@tagged("post_install", "-at_install")
class TestComputedFieldResilience(TransactionCase):
    """Verify all computed fields gracefully handle malformed payloads."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def _create_job_with_sql_payload(self, raw_json):
        """Create a normal job then overwrite its payload via SQL.

        PostgreSQL validates the JSON type, so raw_json must be valid JSON.
        Use json.dumps() to wrap arbitrary values as JSON strings.
        """
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "resilience"}],
            kwargs={},
            channel="resilience",
        )
        self.env.cr.execute(
            "UPDATE queue_job SET payload = %s::json WHERE id = %s",
            (raw_json, job.id),
        )
        job.invalidate_recordset()
        return job

    def test_payload_display_with_json_string_value(self):
        """_compute_payload_display should handle a JSON string
        (not a dict/list) without crashing."""
        # A JSON string like '"hello"' is valid JSON but not a dict
        job = self._create_job_with_sql_payload(json.dumps("not a dict"))
        # Should not raise; renders the JSON string
        self.assertTrue(job.payload_display)

    def test_payload_display_with_non_dict_json(self):
        """_compute_payload_display should handle non-dict JSON values."""
        job = self._create_job_with_sql_payload(json.dumps([1, 2, 3]))
        # json.dumps with indent=2 pretty-prints, so check individual values
        self.assertIn("1", job.payload_display)
        self.assertIn("2", job.payload_display)
        self.assertIn("3", job.payload_display)

    def test_payload_fields_with_non_dict_payload(self):
        """_compute_payload_fields should not crash on a non-dict payload.
        Uses ORM write to trigger stored computed field recomputation."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "resilience"}],
            kwargs={},
            channel="resilience_fields",
        )
        self.assertEqual(job.model_name, "res.partner")
        # Write a non-dict payload via ORM to trigger recomputation
        job.write({"payload": [1, 2, 3]})
        self.assertFalse(job.func_string)

    def test_result_display_with_non_dict_result(self):
        """_compute_result_display should handle a non-dict JSON result."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "bad result"}],
            kwargs={},
            channel="bad_result",
        )
        # Write a JSON string value (valid JSON, not a dict)
        self.env.cr.execute(
            "UPDATE queue_job SET result = %s::json WHERE id = %s",
            (json.dumps("unexpected string result"), job.id),
        )
        job.invalidate_recordset()
        # Should not raise; renders the JSON string
        self.assertTrue(job.result_display)
