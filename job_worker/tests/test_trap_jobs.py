from odoo.tests.common import TransactionCase, tagged

from .common import trap_jobs


@tagged("post_install", "-at_install")
class TestTrapJobs(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_trap_captures_enqueued_job(self):
        partner = self.env["res.partner"].create({"name": "Trap Test"})
        with trap_jobs(self.env) as trap:
            partner.with_delay().write({"name": "Trapped"})
            self.assertEqual(len(trap.enqueued_jobs), 1)

    def test_trap_assert_jobs_count(self):
        partner = self.env["res.partner"].create({"name": "Count Test"})
        with trap_jobs(self.env) as trap:
            partner.with_delay().write({"name": "A"})
            partner.with_delay().write({"name": "B"})
            trap.assert_jobs_count(2)

    def test_trap_assert_jobs_count_with_filter(self):
        partner = self.env["res.partner"].create({"name": "Filter Test"})
        user = self.env["res.users"].search([], limit=1)
        with trap_jobs(self.env) as trap:
            partner.with_delay().write({"name": "Partner"})
            user.with_delay().write({"name": "User"})
            trap.assert_jobs_count(1, model="res.partner")
            trap.assert_jobs_count(1, model="res.users")
            trap.assert_jobs_count(2)

    def test_trap_assert_jobs_count_fails_on_mismatch(self):
        partner = self.env["res.partner"].create({"name": "Mismatch Test"})
        with trap_jobs(self.env) as trap:
            partner.with_delay().write({"name": "One"})
            with self.assertRaises(AssertionError):
                trap.assert_jobs_count(2)

    def test_trap_assert_enqueued_job_found(self):
        partner = self.env["res.partner"].create({"name": "Found Test"})
        with trap_jobs(self.env) as trap:
            partner.with_delay().write({"name": "Found"})
            spec = trap.assert_enqueued_job("res.partner", "write")
            self.assertEqual(spec["model_name"], "res.partner")
            self.assertEqual(spec["method_name"], "write")

    def test_trap_assert_enqueued_job_not_found(self):
        partner = self.env["res.partner"].create({"name": "NotFound Test"})
        with trap_jobs(self.env) as trap:
            partner.with_delay().write({"name": "X"})
            with self.assertRaises(AssertionError):
                trap.assert_enqueued_job("res.partner", "create")

    def test_trap_assert_enqueued_job_with_args(self):
        partner = self.env["res.partner"].create({"name": "Args Test"})
        with trap_jobs(self.env) as trap:
            partner.with_delay().write({"name": "WithArgs"})
            spec = trap.assert_enqueued_job(
                "res.partner", "write", args=[{"name": "WithArgs"}]
            )
            self.assertIsNotNone(spec)

    def test_trap_perform_enqueued_jobs(self):
        partner = self.env["res.partner"].create({"name": "Perform Test"})
        with trap_jobs(self.env) as trap:
            partner.with_delay().write({"name": "Performed"})
            trap.assert_jobs_count(1)
            trap.perform_enqueued_jobs()
        partner.invalidate_recordset()
        self.assertEqual(partner.name, "Performed")

    def test_trap_jobs_still_creates_records(self):
        """Trapped jobs still exist in the database."""
        before_count = self.Job.search_count([])
        partner = self.env["res.partner"].create({"name": "DB Test"})
        with trap_jobs(self.env):
            partner.with_delay().write({"name": "InDB"})
            self.assertEqual(self.Job.search_count([]), before_count + 1)

    def test_trap_multiple_jobs_perform_all(self):
        partners = self.env["res.partner"].create(
            [{"name": f"Multi {i}"} for i in range(3)]
        )
        with trap_jobs(self.env) as trap:
            for partner in partners:
                partner.with_delay().write({"name": f"{partner.name} Done"})
            trap.assert_jobs_count(3)
            trap.perform_enqueued_jobs()
        partners.invalidate_recordset()
        for partner in partners:
            self.assertTrue(partner.name.endswith("Done"))
