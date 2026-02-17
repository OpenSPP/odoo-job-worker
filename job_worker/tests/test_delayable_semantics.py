from odoo.tests.common import TransactionCase, tagged

from ..delay import DelayableChain, DelayableGroup, chain, group


@tagged("post_install", "-at_install")
class TestDelayableSemantics(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_delayable_set_rejects_unknown_property(self):
        partner = self.env["res.partner"].create({"name": "Delay Set"})
        with self.assertRaisesRegex(ValueError, "No property"):
            partner.delayable().set(non_existing_property=1)

    def test_delayable_on_done_enqueues_follow_up(self):
        a = self.env["res.partner"].create({"name": "A"})
        b = self.env["res.partner"].create({"name": "B"})
        before = self.Job.search_count([])

        a.delayable().write({"name": "A done"}).on_done(
            b.delayable().write({"name": "B done"})
        ).delay()

        self.assertEqual(self.Job.search_count([]), before + 2)

    def test_group_on_done_enqueues_all_members_and_follow_up(self):
        a = self.env["res.partner"].create({"name": "GA"})
        b = self.env["res.partner"].create({"name": "GB"})
        c = self.env["res.partner"].create({"name": "GC"})
        before = self.Job.search_count([])

        group(
            a.delayable().write({"name": "GA done"}),
            b.delayable().write({"name": "GB done"}),
        ).on_done(c.delayable().write({"name": "GC done"})).delay()

        self.assertEqual(self.Job.search_count([]), before + 3)

    def test_chain_on_done_enqueues_all_members_and_follow_up(self):
        a = self.env["res.partner"].create({"name": "CA"})
        b = self.env["res.partner"].create({"name": "CB"})
        c = self.env["res.partner"].create({"name": "CC"})
        before = self.Job.search_count([])

        chain(
            a.delayable().write({"name": "CA done"}),
            b.delayable().write({"name": "CB done"}),
        ).on_done(c.delayable().write({"name": "CC done"})).delay()

        self.assertEqual(self.Job.search_count([]), before + 3)

    def test_on_done_sets_parent_id(self):
        """Child job has parent_id pointing to the parent."""
        a = self.env["res.partner"].create({"name": "Parent"})
        b = self.env["res.partner"].create({"name": "Child"})

        d_a = a.delayable().write({"name": "Parent done"})
        d_b = b.delayable().write({"name": "Child done"})
        d_a.on_done(d_b)
        parent_job = d_a.delay()

        child_job = self.Job.search(
            [("id", "!=", parent_job.id)],
            order="id desc",
            limit=1,
        )
        self.assertEqual(child_job.parent_id.id, parent_job.id)

    def test_on_done_child_starts_in_waiting(self):
        """Child created by on_done() starts in waiting state."""
        a = self.env["res.partner"].create({"name": "WaitParent"})
        b = self.env["res.partner"].create({"name": "WaitChild"})

        d_a = a.delayable().write({"name": "WaitParent done"})
        d_b = b.delayable().write({"name": "WaitChild done"})
        d_a.on_done(d_b)
        parent_job = d_a.delay()

        child_job = self.Job.search(
            [("id", "!=", parent_job.id)],
            order="id desc",
            limit=1,
        )
        self.assertEqual(parent_job.state, "pending")
        self.assertEqual(child_job.state, "waiting")

    def test_on_done_sets_graph_uuid(self):
        """Both jobs in an on_done chain share the same graph_uuid."""
        a = self.env["res.partner"].create({"name": "GraphA"})
        b = self.env["res.partner"].create({"name": "GraphB"})

        d_a = a.delayable().write({"name": "GraphA done"})
        d_b = b.delayable().write({"name": "GraphB done"})
        d_a.on_done(d_b)
        parent_job = d_a.delay()

        child_job = self.Job.search(
            [("id", "!=", parent_job.id)],
            order="id desc",
            limit=1,
        )
        self.assertTrue(parent_job.graph_uuid)
        self.assertEqual(parent_job.graph_uuid, child_job.graph_uuid)

    def test_standalone_job_has_no_graph_uuid(self):
        """A standalone job (no on_done) should not have graph_uuid."""
        a = self.env["res.partner"].create({"name": "Standalone"})
        job = a.delayable().write({"name": "Standalone done"}).delay()
        self.assertFalse(job.graph_uuid)

    def test_chain_sets_parent_id_between_steps(self):
        """DelayableChain links each step with parent_id."""
        a = self.env["res.partner"].create({"name": "ChainA"})
        b = self.env["res.partner"].create({"name": "ChainB"})
        c = self.env["res.partner"].create({"name": "ChainC"})

        chain(
            a.delayable().write({"name": "ChainA done"}),
            b.delayable().write({"name": "ChainB done"}),
            c.delayable().write({"name": "ChainC done"}),
        ).delay()

        jobs = self.Job.search([], order="id asc", limit=3)
        # Filter to just the chain jobs (last 3 created)
        jobs = self.Job.search([], order="id desc", limit=3).sorted("id")
        self.assertEqual(len(jobs), 3)
        self.assertFalse(jobs[0].parent_id)
        self.assertEqual(jobs[1].parent_id.id, jobs[0].id)
        self.assertEqual(jobs[2].parent_id.id, jobs[1].id)

    def test_chain_children_start_in_waiting(self):
        """Non-first steps in a chain start in waiting state."""
        a = self.env["res.partner"].create({"name": "ChainWaitA"})
        b = self.env["res.partner"].create({"name": "ChainWaitB"})

        chain(
            a.delayable().write({"name": "ChainWaitA done"}),
            b.delayable().write({"name": "ChainWaitB done"}),
        ).delay()

        jobs = self.Job.search([], order="id desc", limit=2).sorted("id")
        self.assertEqual(jobs[0].state, "pending")
        self.assertEqual(jobs[1].state, "waiting")

    def test_group_shares_graph_uuid(self):
        """All group members share the same graph_uuid."""
        a = self.env["res.partner"].create({"name": "GroupA"})
        b = self.env["res.partner"].create({"name": "GroupB"})

        group(
            a.delayable().write({"name": "GroupA done"}),
            b.delayable().write({"name": "GroupB done"}),
        ).delay()

        jobs = self.Job.search([], order="id desc", limit=2)
        self.assertTrue(jobs[0].graph_uuid)
        self.assertEqual(jobs[0].graph_uuid, jobs[1].graph_uuid)

    def test_group_on_done_does_not_set_parent_id(self):
        """Group dependents have no parent_id (Many2one limitation)."""
        a = self.env["res.partner"].create({"name": "GrpDoneA"})
        b = self.env["res.partner"].create({"name": "GrpDoneB"})
        c = self.env["res.partner"].create({"name": "GrpDoneC"})

        group(
            a.delayable().write({"name": "GrpDoneA done"}),
            b.delayable().write({"name": "GrpDoneB done"}),
        ).on_done(c.delayable().write({"name": "GrpDoneC done"})).delay()

        jobs = self.Job.search([], order="id desc", limit=3).sorted("id")
        # The last job (c) is the dependent — should NOT have parent_id
        dependent = jobs[2]
        self.assertFalse(dependent.parent_id)
        # But all share graph_uuid
        self.assertTrue(jobs[0].graph_uuid)
        self.assertEqual(jobs[0].graph_uuid, jobs[1].graph_uuid)
        self.assertEqual(jobs[0].graph_uuid, jobs[2].graph_uuid)

    def test_child_ids_inverse(self):
        """Parent's child_ids contains the dependent."""
        a = self.env["res.partner"].create({"name": "InvA"})
        b = self.env["res.partner"].create({"name": "InvB"})

        d_a = a.delayable().write({"name": "InvA done"})
        d_b = b.delayable().write({"name": "InvB done"})
        d_a.on_done(d_b)
        parent_job = d_a.delay()

        self.assertEqual(len(parent_job.child_ids), 1)
        self.assertEqual(parent_job.child_ids[0].parent_id.id, parent_job.id)

    def test_circular_on_done_does_not_recurse(self):
        """Circular on_done guard prevents infinite recursion."""
        a = self.env["res.partner"].create({"name": "CircA"})
        b = self.env["res.partner"].create({"name": "CircB"})

        d_a = a.delayable().write({"name": "CircA done"})
        d_b = b.delayable().write({"name": "CircB done"})
        d_a.on_done(d_b)
        d_b.on_done(d_a)
        # Should not raise RecursionError
        d_a.delay()

    def test_group_and_chain_factory_types(self):
        partner = self.env["res.partner"].create({"name": "Type Check"})
        d = partner.delayable().write({"name": "Type Check Done"})
        self.assertIsInstance(group(d), DelayableGroup)
        self.assertIsInstance(chain(d), DelayableChain)


@tagged("post_install", "-at_install")
class TestChainGraphEdgeCases(TransactionCase):
    """Probe job chain/graph edge cases."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_child_fails_when_parent_fails_via_run_now(self):
        """When parent fails via run_now(), children should be cascaded
        to failed state too."""
        parent = self.Job.enqueue(
            model_name="res.partner",
            method_name="method_does_not_exist",
            record_ids=[],
            args=[],
            kwargs={},
            max_retries=3,
            channel="chain_retry",
        )
        child = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "child"}],
            kwargs={},
            parent_id=parent.id,
            channel="chain_retry",
        )
        self.assertEqual(child.state, "waiting")

        try:
            parent.run_now()
        except AttributeError:
            pass
        parent.invalidate_recordset()
        child.invalidate_recordset()
        # run_now() always marks as failed (no retry logic)
        self.assertEqual(parent.state, "failed")
        self.assertEqual(child.state, "failed")

    def test_orphaned_child_when_parent_cancelled(self):
        """If parent is cancelled, children should remain in waiting state
        (cancelling doesn't cascade)."""
        parent = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "parent cancel"}],
            kwargs={},
            channel="cancel_chain",
        )
        child = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "child cancel"}],
            kwargs={},
            parent_id=parent.id,
            channel="cancel_chain",
        )
        self.assertEqual(child.state, "waiting")

        parent.button_cancelled()
        child.invalidate_recordset()
        # Cancellation does not cascade — child remains orphaned in waiting
        self.assertEqual(child.state, "waiting")

    def test_deep_chain_does_not_stackoverflow(self):
        """A deeply nested chain should not cause a stack overflow
        during execution."""
        depth = 50
        jobs = []
        for i in range(depth):
            parent_id = jobs[-1].id if jobs else None
            job = self.Job.enqueue(
                model_name="res.partner",
                method_name="create",
                record_ids=[],
                args=[{"name": f"chain_{i}"}],
                kwargs={},
                parent_id=parent_id,
                channel="deep_chain",
            )
            jobs.append(job)

        # First job should be pending, all others waiting
        self.assertEqual(jobs[0].state, "pending")
        for job in jobs[1:]:
            self.assertEqual(job.state, "waiting")

        # Execute first job
        jobs[0].run_now()
        jobs[0].invalidate_recordset()
        jobs[1].invalidate_recordset()
        self.assertEqual(jobs[0].state, "done")
        self.assertEqual(jobs[1].state, "pending")

    def test_child_with_no_parent_starts_pending(self):
        """A job created with parent_id=None starts as pending."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "no parent"}],
            kwargs={},
            parent_id=None,
            channel="no_parent",
        )
        self.assertEqual(job.state, "pending")

    def test_multiple_children_all_released_on_parent_done(self):
        """All waiting children should be released when parent completes."""
        partner = self.env["res.partner"].create({"name": "Multi Child Parent"})
        parent = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "Parent Done"}],
            kwargs={},
            channel="multi_child",
        )
        children = []
        for i in range(5):
            child = self.Job.enqueue(
                model_name="res.partner",
                method_name="create",
                record_ids=[],
                args=[{"name": f"child_{i}"}],
                kwargs={},
                parent_id=parent.id,
                channel="multi_child",
            )
            children.append(child)

        parent.run_now()
        for child in children:
            child.invalidate_recordset()
            self.assertEqual(child.state, "pending")

    def test_multiple_children_all_failed_on_parent_failure(self):
        """All waiting children should fail when parent fails via run_now().
        run_now() always marks failures as terminal regardless of max_retries."""
        parent = self.Job.enqueue(
            model_name="res.partner",
            method_name="method_does_not_exist",
            record_ids=[],
            args=[],
            kwargs={},
            max_retries=5,
            channel="multi_child_fail",
        )
        children = []
        for i in range(3):
            child = self.Job.enqueue(
                model_name="res.partner",
                method_name="create",
                record_ids=[],
                args=[{"name": f"fail_child_{i}"}],
                kwargs={},
                parent_id=parent.id,
                channel="multi_child_fail",
            )
            children.append(child)

        try:
            parent.run_now()
        except AttributeError:
            pass
        parent.invalidate_recordset()
        self.assertEqual(parent.state, "failed")
        for child in children:
            child.invalidate_recordset()
            self.assertEqual(child.state, "failed")
