from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestOpenRelatedAction(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_single_record_opens_form(self):
        partner = self.env["res.partner"].create({"name": "Related Test"})
        job = partner.with_delay().write({"name": "Done"})
        action = job.open_related_action()
        self.assertEqual(action["type"], "ir.actions.act_window")
        self.assertEqual(action["res_model"], "res.partner")
        self.assertEqual(action["res_id"], partner.id)
        self.assertEqual(action["view_mode"], "form")

    def test_multiple_records_opens_list(self):
        partners = self.env["res.partner"].create(
            [{"name": f"Related {i}"} for i in range(3)]
        )
        job = partners.with_delay().write({"name": "Done"})
        action = job.open_related_action()
        self.assertEqual(action["type"], "ir.actions.act_window")
        self.assertEqual(action["res_model"], "res.partner")
        self.assertEqual(action["domain"], [("id", "in", partners.ids)])
        self.assertEqual(action["view_mode"], "list,form")

    def test_no_records_returns_false(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "No Record"}],
            kwargs={},
        )
        result = job.open_related_action()
        self.assertFalse(result)

    def test_ensure_one_enforced(self):
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Multi A"}],
            kwargs={},
        )
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Multi B"}],
            kwargs={},
        )
        with self.assertRaises(ValueError):
            (job1 | job2).open_related_action()


@tagged("post_install", "-at_install")
class TestRelatedActionEdgeCases(TransactionCase):
    """Probe the open_related_action method for edge cases."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_open_related_action_single_record(self):
        """open_related_action with one record should return form view."""
        partner = self.env["res.partner"].create({"name": "Related Action"})
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "After"}],
            kwargs={},
            channel="related",
        )
        action = job.open_related_action()
        self.assertEqual(action["res_model"], "res.partner")
        self.assertEqual(action["res_id"], partner.id)
        self.assertEqual(action["view_mode"], "form")

    def test_open_related_action_multiple_records(self):
        """open_related_action with multiple records should return list view."""
        p1 = self.env["res.partner"].create({"name": "Related 1"})
        p2 = self.env["res.partner"].create({"name": "Related 2"})
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=[p1.id, p2.id],
            args=[{"name": "After"}],
            kwargs={},
            channel="related_multi",
        )
        action = job.open_related_action()
        self.assertEqual(action["view_mode"], "list,form")
        self.assertEqual(action["domain"], [("id", "in", [p1.id, p2.id])])

    def test_open_related_action_no_record_ids(self):
        """open_related_action with no record IDs should return False."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "No IDs"}],
            kwargs={},
            channel="related_empty",
        )
        result = job.open_related_action()
        self.assertFalse(result)

    def test_open_related_action_with_empty_payload(self):
        """open_related_action when payload is an empty dict should return False."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Empty Payload"}],
            kwargs={},
            channel="related_empty_payload",
        )
        job.write({"payload": {}})
        result = job.open_related_action()
        self.assertFalse(result)
