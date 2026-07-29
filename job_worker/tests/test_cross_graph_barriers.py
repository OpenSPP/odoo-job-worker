"""Group barriers must release even when their dependencies sit in another graph.

``Delayable._enqueue`` returns an *existing* in-flight job when an
``identity_key`` matches.  That existing job keeps the ``graph_uuid`` of the
run that created it, while ``DelayableGroup.delay()`` mints a fresh
``graph_uuid`` for the barrier it is about to build.  The barrier therefore
legitimately depends on jobs belonging to a different graph.

Scoping the release/cascade statements by ``graph_uuid`` makes those
decrements miss, and the barrier stays in ``waiting`` forever.
``dependency_job_ids @> [id]`` already identifies exactly the right rows, so
the graph predicate can only ever exclude correct matches.
"""

from contextlib import contextmanager

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged

from ..cli.worker import QueueWorker


@tagged("post_install", "-at_install")
class TestCrossGraphBarriers(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    @contextmanager
    def _external_env(self):
        with self.env.registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            yield cr, env

    def _member(self, env, name, graph_uuid):
        """A trivial job that succeeds, belonging to ``graph_uuid``."""
        partner = env["res.partner"].create({"name": name})
        return env["queue.job"].enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": name + " done"}],
            kwargs={},
            channel="xgraph",
            graph_uuid=graph_uuid,
        )

    def _barrier(self, env, graph_uuid, dependency_job_ids, run_on_failure=False):
        partner = env["res.partner"].create({"name": "barrier target"})
        return env["queue.job"].enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "barrier ran"}],
            kwargs={},
            channel="xgraph",
            graph_uuid=graph_uuid,
            dependency_job_ids=dependency_job_ids,
            run_on_failure=run_on_failure,
        )

    def test_worker_releases_barrier_from_a_different_graph(self):
        """The worker must decrement a barrier that lives in another graph."""
        with self._external_env() as (cr, env):
            member = self._member(env, "xgraph member", "graph-members")
            barrier = self._barrier(env, "graph-barrier", [member.id])
            self.assertEqual(barrier.state, "waiting")
            self.assertEqual(barrier.pending_dependency_count, 1)

            QueueWorker(cr.dbname).execute_job(cr, member.id)

            barrier.invalidate_recordset()
            self.assertEqual(
                barrier.pending_dependency_count,
                0,
                "barrier in a different graph was never decremented",
            )
            self.assertEqual(
                barrier.state,
                "pending",
                "barrier in a different graph stayed stuck in 'waiting'",
            )

    def test_worker_release_still_ignores_unrelated_barriers(self):
        """Dropping the graph predicate must not release unrelated barriers."""
        with self._external_env() as (cr, env):
            member = self._member(env, "xgraph member 2", "graph-members-2")
            other = self._member(env, "xgraph other", "graph-members-2")
            unrelated = self._barrier(env, "graph-barrier-2", [other.id])

            QueueWorker(cr.dbname).execute_job(cr, member.id)

            unrelated.invalidate_recordset()
            self.assertEqual(unrelated.state, "waiting")
            self.assertEqual(unrelated.pending_dependency_count, 1)

    def test_worker_cascades_failure_across_graphs(self):
        """A permanently failed member must fail its cross-graph barrier."""
        with self._external_env() as (cr, env):
            # handle_exception increments attempts *before* comparing, and
            # max_retries=0 means "retry forever" — so exhaust the budget up
            # front to get a permanent failure out of this single execution.
            partner = env["res.partner"].create({"name": "boom"})
            member = env["queue.job"].enqueue(
                model_name="res.partner",
                method_name="no_such_method_for_tests",
                record_ids=partner.ids,
                args=[],
                kwargs={},
                channel="xgraph",
                graph_uuid="graph-members-3",
                max_retries=1,
            )
            member.attempts = 1
            barrier = self._barrier(env, "graph-barrier-3", [member.id])

            QueueWorker(cr.dbname).execute_job(cr, member.id)

            member.invalidate_recordset()
            barrier.invalidate_recordset()
            self.assertEqual(member.state, "failed")
            self.assertEqual(
                barrier.state,
                "failed",
                "cross-graph barrier was not cascaded on member failure",
            )

    def test_orm_release_dependents_crosses_graphs(self):
        """The ORM path has the same graph predicate and the same bug."""
        member = self._member(self.env, "orm xgraph member", "orm-graph-members")
        barrier = self._barrier(self.env, "orm-graph-barrier", [member.id])
        self.assertEqual(barrier.state, "waiting")

        member._release_dependents()

        barrier.invalidate_recordset()
        self.assertEqual(barrier.pending_dependency_count, 0)
        self.assertEqual(barrier.state, "pending")

    def test_orm_fail_dependents_crosses_graphs(self):
        member = self._member(self.env, "orm xgraph member 2", "orm-graph-members-2")
        barrier = self._barrier(self.env, "orm-graph-barrier-2", [member.id])

        member._fail_dependents()

        barrier.invalidate_recordset()
        self.assertEqual(barrier.state, "failed")

    def test_orm_release_works_when_completing_job_has_no_graph(self):
        """A member with no graph_uuid at all must still release its barrier.

        The old implementation skipped such jobs outright via
        ``if not job.graph_uuid: continue``.
        """
        member = self._member(self.env, "orm no graph member", None)
        barrier = self._barrier(self.env, "orm-graph-barrier-3", [member.id])

        member._release_dependents()

        barrier.invalidate_recordset()
        self.assertEqual(barrier.pending_dependency_count, 0)
        self.assertEqual(barrier.state, "pending")
