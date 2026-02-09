import uuid
from unittest.mock import patch

from odoo import SUPERUSER_ID
from odoo.tests.common import TransactionCase, tagged

from ..cli import worker as worker_module
from ..cli.worker import QueueWorker
from .common import external_env


@tagged("post_install", "-at_install")
class TestExecutionContext(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_worker_executes_job_as_job_user(self):
        with external_env(self.env, uid=SUPERUSER_ID) as (cr, env):
            system_group = env.ref("base.group_system")
            company = env.company
            suffix = uuid.uuid4().hex[:8]
            run_user = (
                env["res.users"]
                .with_context(no_reset_password=True)
                .create(
                    {
                        "name": "Queue Context User",
                        "login": f"queue_context_user_{suffix}",
                        "email": f"queue_context_user_{suffix}@example.com",
                        "company_id": company.id,
                        "company_ids": [(6, 0, [company.id])],
                        "group_ids": [(6, 0, [system_group.id])],
                    }
                )
            )
            job = env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", run_user.id)]],
                kwargs={},
                channel="ctx_user",
            )
            job.write({"user_id": run_user.id})
            worker = QueueWorker(cr.dbname)
            original_environment = worker_module.api.Environment
            seen_calls = []

            def _capture_environment(cr_arg, uid_arg, context_arg):
                seen_calls.append((uid_arg, dict(context_arg or {})))
                return original_environment(cr_arg, uid_arg, context_arg)

            with patch.object(
                worker_module.api, "Environment", side_effect=_capture_environment
            ):
                worker.execute_job(cr, job.id)

            self.assertTrue(any(uid == run_user.id for uid, _ in seen_calls))

    def test_worker_executes_job_with_job_company_context(self):
        with external_env(self.env, uid=SUPERUSER_ID) as (cr, env):
            system_group = env.ref("base.group_system")
            company_a = env.company
            company_b = env["res.company"].create({"name": "Queue Worker Company B"})
            suffix = uuid.uuid4().hex[:8]
            run_user = (
                env["res.users"]
                .with_context(no_reset_password=True)
                .create(
                    {
                        "name": "Queue Company User",
                        "login": f"queue_company_user_{suffix}",
                        "email": f"queue_company_user_{suffix}@example.com",
                        "company_id": company_a.id,
                        "company_ids": [(6, 0, [company_a.id, company_b.id])],
                        "group_ids": [(6, 0, [system_group.id])],
                    }
                )
            )
            job = env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", run_user.id)]],
                kwargs={},
                channel="ctx_company",
            )
            job.write({"user_id": run_user.id, "company_id": company_b.id})
            worker = QueueWorker(cr.dbname)
            original_environment = worker_module.api.Environment
            seen_calls = []

            def _capture_environment(cr_arg, uid_arg, context_arg):
                seen_calls.append((uid_arg, dict(context_arg or {})))
                return original_environment(cr_arg, uid_arg, context_arg)

            with patch.object(
                worker_module.api, "Environment", side_effect=_capture_environment
            ):
                worker.execute_job(cr, job.id)

            user_calls = [ctx for uid, ctx in seen_calls if uid == run_user.id]
            self.assertTrue(user_calls)
            self.assertTrue(
                any(ctx.get("company_id") == company_b.id for ctx in user_calls)
            )
            self.assertTrue(
                any(
                    company_b.id in (ctx.get("allowed_company_ids") or [])
                    for ctx in user_calls
                )
            )

    def test_worker_executes_job_with_user_lang_and_tz_in_context(self):
        with external_env(self.env, uid=SUPERUSER_ID) as (cr, env):
            system_group = env.ref("base.group_system")
            company = env.company
            suffix = uuid.uuid4().hex[:8]
            run_user = (
                env["res.users"]
                .with_context(no_reset_password=True)
                .create(
                    {
                        "name": "Queue Locale User",
                        "login": f"queue_locale_user_{suffix}",
                        "email": f"queue_locale_user_{suffix}@example.com",
                        "lang": "en_US",
                        "tz": "Europe/Brussels",
                        "company_id": company.id,
                        "company_ids": [(6, 0, [company.id])],
                        "group_ids": [(6, 0, [system_group.id])],
                    }
                )
            )
            job = env["queue.job"].enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", run_user.id)]],
                kwargs={},
                channel="ctx_locale",
            )
            job.write({"user_id": run_user.id, "company_id": company.id})
            worker = QueueWorker(cr.dbname)
            original_environment = worker_module.api.Environment
            seen_calls = []

            def _capture_environment(cr_arg, uid_arg, context_arg):
                seen_calls.append((uid_arg, dict(context_arg or {})))
                return original_environment(cr_arg, uid_arg, context_arg)

            with patch.object(
                worker_module.api, "Environment", side_effect=_capture_environment
            ):
                worker.execute_job(cr, job.id)

            user_calls = [ctx for uid, ctx in seen_calls if uid == run_user.id]
            self.assertTrue(user_calls)
            self.assertTrue(any(ctx.get("lang") == run_user.lang for ctx in user_calls))
            self.assertTrue(any(ctx.get("tz") == run_user.tz for ctx in user_calls))

    def test_enqueue_from_sudo_keeps_original_user(self):
        company = self.env.company
        suffix = uuid.uuid4().hex[:8]
        run_user = (
            self.env["res.users"]
            .with_context(no_reset_password=True)
            .create(
                {
                    "name": "Queue Sudo User",
                    "login": f"queue_sudo_user_{suffix}",
                    "email": f"queue_sudo_user_{suffix}@example.com",
                    "company_id": company.id,
                    "company_ids": [(6, 0, [company.id])],
                }
            )
        )
        user_env = self.env(user=run_user.id)
        job = (
            user_env["queue.job"]
            .sudo()
            .enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", run_user.id)]],
                kwargs={},
                channel="ctx_sudo_user",
            )
        )

        self.assertEqual(job.user_id.id, run_user.id)
