import uuid
from unittest.mock import patch

from odoo import SUPERUSER_ID
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker
from .common import external_env


@tagged("post_install", "-at_install")
class TestWorkerSecurityContext(TransactionCase):
    def test_low_privilege_user_access_error_retries_then_fails(self):
        with external_env(self.env, uid=SUPERUSER_ID) as (cr, env):
            portal_group = env.ref("base.group_portal")
            company = env.company
            suffix = uuid.uuid4().hex[:8]
            portal_user = (
                env["res.users"]
                .with_context(no_reset_password=True)
                .create(
                    {
                        "name": "Queue Portal User",
                        "login": f"queue_portal_user_{suffix}",
                        "email": f"queue_portal_user_{suffix}@example.com",
                        "company_id": company.id,
                        "company_ids": [(6, 0, [company.id])],
                        "group_ids": [(6, 0, [portal_group.id])],
                    }
                )
            )

            job = env["queue.job"].enqueue(
                model_name="ir.config_parameter",
                method_name="search",
                record_ids=[],
                args=[[("key", "=", "database.uuid")]],
                kwargs={},
                max_retries=1,
                channel="permissions",
            )
            job.write({"user_id": portal_user.id})
            worker = QueueWorker(cr.dbname)

            worker.execute_job(cr, job.id)
            env.invalidate_all()
            first = env["queue.job"].browse(job.id)
            self.assertEqual(first.state, "pending")
            self.assertEqual(first.attempts, 1)

            worker.execute_job(cr, job.id)
            env.invalidate_all()
            second = env["queue.job"].browse(job.id)
            self.assertEqual(second.state, "failed")
            self.assertEqual(second.attempts, 2)
            self.assertTrue(
                "access" in (second.exc_info or "").lower()
                or "permission" in (second.exc_info or "").lower()
            )

    def test_company_context_selects_company_specific_sequence(self):
        with external_env(self.env, uid=SUPERUSER_ID) as (cr, env):
            company_a = env.company
            company_b = env["res.company"].create(
                {"name": f"Queue Seq Co {uuid.uuid4().hex[:6]}"}
            )
            code = f"queue.ctx.seq.{uuid.uuid4().hex[:8]}"
            partner = env["res.partner"].create({"name": "Sequence Pending"})

            seq_a = env["ir.sequence"].create(
                {
                    "name": "Queue Context Sequence A",
                    "code": code,
                    "prefix": "A-",
                    "padding": 4,
                    "company_id": company_a.id,
                    "number_next": 1,
                }
            )
            env["ir.sequence"].create(
                {
                    "name": "Queue Context Sequence B",
                    "code": code,
                    "prefix": "B-",
                    "padding": 4,
                    "company_id": company_b.id,
                    "number_next": 1,
                }
            )
            env.user.write({"company_ids": [(4, company_b.id)]})

            def _consume_sequence(records, sequence_code):
                records.ensure_one()
                value = records.env["ir.sequence"].next_by_code(sequence_code) or ""
                records.write({"name": value})
                return value

            with patch.object(
                type(env["res.partner"]),
                "queue_consume_sequence",
                _consume_sequence,
                create=True,
            ):
                job = env["queue.job"].enqueue(
                    model_name="res.partner",
                    method_name="queue_consume_sequence",
                    record_ids=partner.ids,
                    args=[code],
                    kwargs={},
                    channel="company_ctx_seq",
                )
                job.write({"company_id": company_b.id, "user_id": env.user.id})
                worker = QueueWorker(cr.dbname)
                worker.execute_job(cr, job.id)

            env.invalidate_all()
            self.assertEqual(env["ir.sequence"].browse(seq_a.id).number_next, 1)
            self.assertTrue(
                env["res.partner"].browse(partner.id).name.startswith("B-"),
                "Expected sequence call to use company B context",
            )
