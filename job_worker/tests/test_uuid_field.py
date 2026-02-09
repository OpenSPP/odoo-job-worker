import uuid

from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestUuidField(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_uuid_populated_on_create(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "UUID Test"}],
            kwargs={},
        )
        self.assertTrue(job.uuid)
        # Should be a valid UUID
        uuid.UUID(job.uuid)

    def test_uuid_unique_across_jobs(self):
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "UUID A"}],
            kwargs={},
        )
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "UUID B"}],
            kwargs={},
        )
        self.assertNotEqual(job1.uuid, job2.uuid)

    def test_uuid_searchable(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "UUID Search"}],
            kwargs={},
        )
        found = self.Job.search([("uuid", "=", job.uuid)])
        self.assertEqual(found.id, job.id)

    def test_uuid_not_copied(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "UUID Copy"}],
            kwargs={},
        )
        copied = job.copy()
        self.assertTrue(copied.uuid)
        self.assertNotEqual(job.uuid, copied.uuid)
