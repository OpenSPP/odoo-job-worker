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

    def test_group_on_done_creates_barrier(self):
        """Group on_done callback uses dependency_job_ids for multi-parent barrier."""
        a = self.env["res.partner"].create({"name": "GrpDoneA"})
        b = self.env["res.partner"].create({"name": "GrpDoneB"})
        c = self.env["res.partner"].create({"name": "GrpDoneC"})

        group(
            a.delayable().write({"name": "GrpDoneA done"}),
            b.delayable().write({"name": "GrpDoneB done"}),
        ).on_done(c.delayable().write({"name": "GrpDoneC done"})).delay()

        jobs = self.Job.search([], order="id desc", limit=3).sorted("id")
        member_a, member_b, callback = jobs[0], jobs[1], jobs[2]

        # Callback starts in waiting state with dependency tracking
        self.assertEqual(callback.state, "waiting")
        self.assertFalse(callback.parent_id)
        self.assertIn(member_a.id, callback.dependency_job_ids)
        self.assertIn(member_b.id, callback.dependency_job_ids)
        self.assertEqual(callback.pending_dependency_count, 2)

        # All share graph_uuid
        self.assertTrue(member_a.graph_uuid)
        self.assertEqual(member_a.graph_uuid, member_b.graph_uuid)
        self.assertEqual(member_a.graph_uuid, callback.graph_uuid)

        # Members start in pending state
        self.assertEqual(member_a.state, "pending")
        self.assertEqual(member_b.state, "pending")

    def test_group_on_done_barrier_released_when_all_done(self):
        """Callback transitions to pending only after all group members complete."""
        a = self.env["res.partner"].create({"name": "BarrierA"})
        b = self.env["res.partner"].create({"name": "BarrierB"})
        c = self.env["res.partner"].create({"name": "BarrierC"})

        group(
            a.delayable().write({"name": "BarrierA done"}),
            b.delayable().write({"name": "BarrierB done"}),
        ).on_done(c.delayable().write({"name": "BarrierC done"})).delay()

        jobs = self.Job.search([], order="id desc", limit=3).sorted("id")
        member_a, member_b, callback = jobs[0], jobs[1], jobs[2]

        # Complete first member — callback should stay waiting
        member_a.run_now()
        callback.invalidate_recordset()
        self.assertEqual(callback.state, "waiting")
        self.assertEqual(callback.pending_dependency_count, 1)

        # Complete second member — callback should transition to pending
        member_b.run_now()
        callback.invalidate_recordset()
        self.assertEqual(callback.state, "pending")
        self.assertEqual(callback.pending_dependency_count, 0)

    def test_group_on_done_cascade_failure(self):
        """Callback fails when any group member fails permanently."""
        a = self.env["res.partner"].create({"name": "FailA"})
        b = self.env["res.partner"].create({"name": "FailB"})
        c = self.env["res.partner"].create({"name": "FailC"})

        group(
            a.delayable().method_does_not_exist(),
            b.delayable().write({"name": "FailB done"}),
        ).on_done(c.delayable().write({"name": "FailC done"})).delay()

        jobs = self.Job.search([], order="id desc", limit=3).sorted("id")
        member_a, _member_b, callback = jobs[0], jobs[1], jobs[2]

        # Fail the first member
        try:
            member_a.run_now()
        except AttributeError:
            pass

        member_a.invalidate_recordset()
        callback.invalidate_recordset()
        self.assertEqual(member_a.state, "failed")
        self.assertEqual(callback.state, "failed")
        self.assertIn("Parent job", callback.exc_info)

    def test_group_on_done_button_set_to_done(self):
        """button_set_to_done releases multi-parent dependents."""
        a = self.env["res.partner"].create({"name": "BtnA"})
        b = self.env["res.partner"].create({"name": "BtnB"})
        c = self.env["res.partner"].create({"name": "BtnC"})

        group(
            a.delayable().write({"name": "BtnA done"}),
            b.delayable().write({"name": "BtnB done"}),
        ).on_done(c.delayable().write({"name": "BtnC done"})).delay()

        jobs = self.Job.search([], order="id desc", limit=3).sorted("id")
        member_a, member_b, callback = jobs[0], jobs[1], jobs[2]

        # Manually complete first member — callback stays waiting
        member_a.button_set_to_done()
        callback.invalidate_recordset()
        self.assertEqual(callback.state, "waiting")
        self.assertEqual(callback.pending_dependency_count, 1)

        # Manually complete second member — callback transitions to pending
        member_b.button_set_to_done()
        callback.invalidate_recordset()
        self.assertEqual(callback.state, "pending")
        self.assertEqual(callback.pending_dependency_count, 0)

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
class TestOnErrorSemantics(TransactionCase):
    """on_error() callback runs when dependencies fail and is cancelled
    when they succeed.

    The mechanism backs cleanup callbacks that must clear locks or post
    failure chatter even when an async pipeline fails."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_on_error_flag_persists_through_enqueue(self):
        a = self.env["res.partner"].create({"name": "ErrFlagA"})
        b = self.env["res.partner"].create({"name": "ErrFlagB"})

        d_a = a.delayable().write({"name": "ErrFlagA done"})
        d_b = b.delayable().write({"name": "ErrFlagB cleanup"})
        d_a.on_error(d_b)
        parent_job = d_a.delay()

        callback = self.Job.search(
            [("id", "!=", parent_job.id)], order="id desc", limit=1
        )
        self.assertTrue(callback.run_on_failure)
        self.assertEqual(callback.state, "waiting")

    def test_on_error_runs_when_parent_fails_via_run_now(self):
        """Single-parent on_error: child promotes to pending on parent failure."""
        b = self.env["res.partner"].create({"name": "ErrCleanup"})
        parent = self.Job.enqueue(
            model_name="res.partner",
            method_name="method_does_not_exist",
            record_ids=[],
            args=[],
            kwargs={},
            channel="on_error_test",
        )
        child = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=b.ids,
            args=[{"name": "ErrCleanup ran"}],
            kwargs={},
            parent_id=parent.id,
            run_on_failure=True,
            channel="on_error_test",
        )
        self.assertEqual(child.state, "waiting")

        try:
            parent.run_now()
        except AttributeError:
            pass
        parent.invalidate_recordset()
        child.invalidate_recordset()
        self.assertEqual(parent.state, "failed")
        self.assertEqual(child.state, "pending")

    def test_on_error_cancelled_when_parent_succeeds_via_run_now(self):
        """Single-parent on_error: child is cancelled (not run) on parent success."""
        partner = self.env["res.partner"].create({"name": "OkParent"})
        cleanup = self.env["res.partner"].create({"name": "Cleanup"})
        parent = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "OkParent done"}],
            kwargs={},
            channel="on_error_test",
        )
        child = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=cleanup.ids,
            args=[{"name": "Cleanup ran"}],
            kwargs={},
            parent_id=parent.id,
            run_on_failure=True,
            channel="on_error_test",
        )
        self.assertEqual(child.state, "waiting")

        parent.run_now()
        parent.invalidate_recordset()
        child.invalidate_recordset()
        self.assertEqual(parent.state, "done")
        self.assertEqual(child.state, "cancelled")
        self.assertTrue(child.cancelled_at)

    def test_on_error_runs_when_button_set_to_failed(self):
        """Manual fail-out: on_error child promotes to pending."""
        cleanup = self.env["res.partner"].create({"name": "Cleanup btn"})
        parent = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "btn parent"}],
            kwargs={},
            channel="on_error_test",
        )
        child = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=cleanup.ids,
            args=[{"name": "Cleanup ran btn"}],
            kwargs={},
            parent_id=parent.id,
            run_on_failure=True,
            channel="on_error_test",
        )

        parent.button_set_to_failed()
        child.invalidate_recordset()
        self.assertEqual(child.state, "pending")

    def test_group_on_error_runs_when_one_member_fails(self):
        """Group on_error: failure handler fires on first member failure."""
        a = self.env["res.partner"].create({"name": "GErrA"})
        b = self.env["res.partner"].create({"name": "GErrB"})
        c = self.env["res.partner"].create({"name": "GErrCleanup"})

        group(
            a.delayable().method_does_not_exist(),
            b.delayable().write({"name": "GErrB done"}),
        ).on_error(c.delayable().write({"name": "GErrCleanup ran"})).delay()

        jobs = self.Job.search([], order="id desc", limit=3).sorted("id")
        member_a, _member_b, callback = jobs[0], jobs[1], jobs[2]
        self.assertTrue(callback.run_on_failure)
        self.assertEqual(callback.state, "waiting")

        try:
            member_a.run_now()
        except AttributeError:
            pass

        member_a.invalidate_recordset()
        callback.invalidate_recordset()
        self.assertEqual(member_a.state, "failed")
        self.assertEqual(callback.state, "pending")

    def test_group_on_error_cancelled_when_all_members_succeed(self):
        """Group on_error: failure handler is cancelled when all deps succeed."""
        a = self.env["res.partner"].create({"name": "GOkA"})
        b = self.env["res.partner"].create({"name": "GOkB"})
        c = self.env["res.partner"].create({"name": "GOkCleanup"})

        group(
            a.delayable().write({"name": "GOkA done"}),
            b.delayable().write({"name": "GOkB done"}),
        ).on_error(c.delayable().write({"name": "GOkCleanup ran"})).delay()

        jobs = self.Job.search([], order="id desc", limit=3).sorted("id")
        member_a, member_b, callback = jobs[0], jobs[1], jobs[2]
        self.assertTrue(callback.run_on_failure)

        member_a.run_now()
        member_b.run_now()
        callback.invalidate_recordset()
        self.assertEqual(callback.state, "cancelled")
        self.assertTrue(callback.cancelled_at)

    def test_group_on_done_and_on_error_paired_success(self):
        """When both on_done and on_error are wired, success runs on_done
        and cancels on_error."""
        a = self.env["res.partner"].create({"name": "PairOkA"})
        b = self.env["res.partner"].create({"name": "PairOkB"})
        c = self.env["res.partner"].create({"name": "PairOkOk"})
        d = self.env["res.partner"].create({"name": "PairOkErr"})

        group(
            a.delayable().write({"name": "PairOkA done"}),
            b.delayable().write({"name": "PairOkB done"}),
        ).on_done(c.delayable().write({"name": "PairOkOk ran"})).on_error(
            d.delayable().write({"name": "PairOkErr ran"})
        ).delay()

        jobs = self.Job.search([], order="id desc", limit=4).sorted("id")
        member_a, member_b, ok_cb, err_cb = jobs[0], jobs[1], jobs[2], jobs[3]
        self.assertFalse(ok_cb.run_on_failure)
        self.assertTrue(err_cb.run_on_failure)

        member_a.run_now()
        member_b.run_now()
        ok_cb.invalidate_recordset()
        err_cb.invalidate_recordset()
        self.assertEqual(ok_cb.state, "pending")
        self.assertEqual(err_cb.state, "cancelled")

    def test_group_on_done_and_on_error_paired_failure(self):
        """When a member fails: on_done cascades to failed, on_error promotes."""
        a = self.env["res.partner"].create({"name": "PairFailA"})
        b = self.env["res.partner"].create({"name": "PairFailB"})
        c = self.env["res.partner"].create({"name": "PairFailOk"})
        d = self.env["res.partner"].create({"name": "PairFailErr"})

        group(
            a.delayable().method_does_not_exist(),
            b.delayable().write({"name": "PairFailB done"}),
        ).on_done(c.delayable().write({"name": "PairFailOk ran"})).on_error(
            d.delayable().write({"name": "PairFailErr ran"})
        ).delay()

        jobs = self.Job.search([], order="id desc", limit=4).sorted("id")
        member_a, _member_b, ok_cb, err_cb = jobs[0], jobs[1], jobs[2], jobs[3]

        try:
            member_a.run_now()
        except AttributeError:
            pass
        ok_cb.invalidate_recordset()
        err_cb.invalidate_recordset()
        self.assertEqual(ok_cb.state, "failed")
        self.assertEqual(err_cb.state, "pending")


@tagged("post_install", "-at_install")
class TestDeepCascadeSemantics(TransactionCase):
    """Cascades walk the full dependency chain — grandchildren must not
    stay stuck in 'waiting' when an ancestor terminates without running."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_failure_cascade_recurses_into_grandchildren(self):
        """A → B → C: when A fails, both B and C transition to failed."""
        partner = self.env["res.partner"].create({"name": "DeepFail"})
        a = self.Job.enqueue(
            model_name="res.partner",
            method_name="method_does_not_exist",
            record_ids=[],
            args=[],
            kwargs={},
            channel="deep_cascade",
        )
        b = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "B"}],
            kwargs={},
            parent_id=a.id,
            channel="deep_cascade",
        )
        c = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "C"}],
            kwargs={},
            parent_id=b.id,
            channel="deep_cascade",
        )
        self.assertEqual(b.state, "waiting")
        self.assertEqual(c.state, "waiting")

        try:
            a.run_now()
        except AttributeError:
            pass
        a.invalidate_recordset()
        b.invalidate_recordset()
        c.invalidate_recordset()
        self.assertEqual(a.state, "failed")
        self.assertEqual(b.state, "failed")
        self.assertEqual(c.state, "failed", "Grandchild must not stay in waiting")

    def test_failure_cascade_promotes_on_error_grandchild(self):
        """A → B → (C is on_error): A fails → B fails → C runs (run_on_failure)."""
        partner = self.env["res.partner"].create({"name": "DeepFailErr"})
        a = self.Job.enqueue(
            model_name="res.partner",
            method_name="method_does_not_exist",
            record_ids=[],
            args=[],
            kwargs={},
            channel="deep_cascade",
        )
        b = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "B"}],
            kwargs={},
            parent_id=a.id,
            channel="deep_cascade",
        )
        c = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "C ran"}],
            kwargs={},
            parent_id=b.id,
            run_on_failure=True,
            channel="deep_cascade",
        )

        try:
            a.run_now()
        except AttributeError:
            pass
        b.invalidate_recordset()
        c.invalidate_recordset()
        self.assertEqual(b.state, "failed")
        self.assertEqual(c.state, "pending", "on_error grandchild must run")

    def test_success_cascade_cancels_on_error_subtree(self):
        """A → (B is on_error) → C: A succeeds → B cancelled → C also cancelled."""
        a_partner = self.env["res.partner"].create({"name": "SuccA"})
        b_partner = self.env["res.partner"].create({"name": "SuccB"})
        c_partner = self.env["res.partner"].create({"name": "SuccC"})

        a = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=a_partner.ids,
            args=[{"name": "A done"}],
            kwargs={},
            channel="deep_cascade",
        )
        b = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=b_partner.ids,
            args=[{"name": "B"}],
            kwargs={},
            parent_id=a.id,
            run_on_failure=True,
            channel="deep_cascade",
        )
        c = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=c_partner.ids,
            args=[{"name": "C"}],
            kwargs={},
            parent_id=b.id,
            channel="deep_cascade",
        )

        a.run_now()
        b.invalidate_recordset()
        c.invalidate_recordset()
        self.assertEqual(b.state, "cancelled")
        self.assertEqual(
            c.state,
            "cancelled",
            "Grandchild of cancelled parent must not stay in waiting",
        )


@tagged("post_install", "-at_install")
class TestNestedDelayables(TransactionCase):
    """Nested DelayableGroup / DelayableChain inherit graph context and
    execution flags from their outer scheduler."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_nested_group_inherits_graph_uuid(self):
        """A group passed as on_done(...) shares its outer graph_uuid."""
        outer_a = self.env["res.partner"].create({"name": "OutA"})
        outer_b = self.env["res.partner"].create({"name": "OutB"})
        inner_a = self.env["res.partner"].create({"name": "InA"})
        inner_b = self.env["res.partner"].create({"name": "InB"})

        group(
            outer_a.delayable().write({"name": "OutA done"}),
            outer_b.delayable().write({"name": "OutB done"}),
        ).on_done(
            group(
                inner_a.delayable().write({"name": "InA done"}),
                inner_b.delayable().write({"name": "InB done"}),
            )
        ).delay()

        jobs = self.Job.search([], order="id desc", limit=4)
        graph_uuids = {j.graph_uuid for j in jobs}
        self.assertEqual(
            len(graph_uuids),
            1,
            f"All nested jobs must share one graph_uuid, got {graph_uuids}",
        )
        self.assertTrue(next(iter(graph_uuids)))

    def test_nested_group_in_on_error_propagates_run_on_failure(self):
        """A group passed as on_error(...) marks every member as run_on_failure."""
        outer = self.env["res.partner"].create({"name": "OuterFailing"})
        inner_a = self.env["res.partner"].create({"name": "InnerA"})
        inner_b = self.env["res.partner"].create({"name": "InnerB"})

        outer.delayable().method_does_not_exist().on_error(
            group(
                inner_a.delayable().write({"name": "InnerA cleanup"}),
                inner_b.delayable().write({"name": "InnerB cleanup"}),
            )
        ).delay()

        jobs = self.Job.search([], order="id desc", limit=3)
        # Outer is the parent; the two inner members are dependents
        inner_jobs = jobs.filtered(lambda j: j.run_on_failure)
        self.assertEqual(
            len(inner_jobs), 2, "Both nested members must inherit run_on_failure=True"
        )

    def test_nested_chain_inherits_graph_uuid(self):
        """A chain passed as on_done(...) shares its outer graph_uuid."""
        outer = self.env["res.partner"].create({"name": "ChainOut"})
        a = self.env["res.partner"].create({"name": "ChainA"})
        b = self.env["res.partner"].create({"name": "ChainB"})

        outer.delayable().write({"name": "ChainOut done"}).on_done(
            chain(
                a.delayable().write({"name": "ChainA done"}),
                b.delayable().write({"name": "ChainB done"}),
            )
        ).delay()

        jobs = self.Job.search([], order="id desc", limit=3)
        graph_uuids = {j.graph_uuid for j in jobs}
        self.assertEqual(
            len(graph_uuids), 1, "Chain members must share outer graph_uuid"
        )


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
