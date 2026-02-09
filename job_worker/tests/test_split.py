from odoo.tests.common import TransactionCase, tagged

from ..delay import DelayableChain, DelayableGroup


@tagged("post_install", "-at_install")
class TestDelayableSplit(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_split_returns_group_by_default(self):
        partners = self.env["res.partner"].create([{"name": f"P{i}"} for i in range(6)])
        delayable = partners.delayable(priority=3).write({"name": "done"})
        result = delayable.split(2)
        self.assertIsInstance(result, DelayableGroup)

    def test_split_returns_chain_when_requested(self):
        partners = self.env["res.partner"].create([{"name": f"P{i}"} for i in range(6)])
        delayable = partners.delayable(priority=3).write({"name": "done"})
        result = delayable.split(2, chain=True)
        self.assertIsInstance(result, DelayableChain)

    def test_split_creates_correct_number_of_chunks(self):
        partners = self.env["res.partner"].create([{"name": f"P{i}"} for i in range(5)])
        before_count = self.Job.search_count([])
        delayable = partners.delayable().write({"name": "split"})
        delayable.split(2).delay()
        # 5 records / 2 = 3 chunks (2, 2, 1)
        self.assertEqual(self.Job.search_count([]) - before_count, 3)

    def test_split_preserves_properties(self):
        partners = self.env["res.partner"].create([{"name": f"P{i}"} for i in range(4)])
        delayable = partners.delayable(
            priority=7, channel="exports", max_retries=10
        ).write({"name": "props"})
        grp = delayable.split(2)
        for sub in grp._delayables:
            self.assertEqual(sub.priority, 7)
            self.assertEqual(sub.channel, "exports")
            self.assertEqual(sub.max_retries, 10)
            self.assertEqual(sub._job_method, "write")

    def test_split_chunks_cover_all_records(self):
        partners = self.env["res.partner"].create([{"name": f"P{i}"} for i in range(5)])
        delayable = partners.delayable().write({"name": "check"})
        grp = delayable.split(2)
        all_ids = []
        for sub in grp._delayables:
            all_ids.extend(sub.recordset.ids)
        self.assertEqual(sorted(all_ids), sorted(partners.ids))

    def test_split_single_chunk_when_size_ge_records(self):
        partners = self.env["res.partner"].create([{"name": f"P{i}"} for i in range(3)])
        delayable = partners.delayable().write({"name": "big"})
        grp = delayable.split(10)
        self.assertEqual(len(grp._delayables), 1)
        self.assertEqual(grp._delayables[0].recordset.ids, partners.ids)

    def test_split_with_chain_sets_parent_ids(self):
        partners = self.env["res.partner"].create([{"name": f"P{i}"} for i in range(4)])
        delayable = partners.delayable().write({"name": "chain"})
        delayable.split(2, chain=True).delay()
        jobs = self.Job.search([], order="id desc", limit=2).sorted("id")
        # Second job should have first job as parent
        self.assertEqual(jobs[1].parent_id.id, jobs[0].id)

    def test_split_empty_recordset_returns_empty_group(self):
        partners = self.env["res.partner"].browse([])
        delayable = partners.delayable().write({"name": "empty"})
        grp = delayable.split(2)
        self.assertIsInstance(grp, DelayableGroup)
        self.assertEqual(len(grp._delayables), 0)
