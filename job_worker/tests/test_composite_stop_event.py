import importlib.util
import os
import threading
import unittest

# Load the runner module with stubbed Odoo dependencies.
_helpers_path = os.path.join(os.path.dirname(__file__), "_runner_test_helpers.py")
_spec = importlib.util.spec_from_file_location("_runner_test_helpers", _helpers_path)
_helpers = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_helpers)
_runner = _helpers.load_runner()
CompositeStopEvent = _runner.CompositeStopEvent


class TestCompositeStopEvent(unittest.TestCase):
    """Tests for CompositeStopEvent, which combines a global and local event."""

    def _make_composite(self, global_event=None, local_event=None):
        global_event = global_event or threading.Event()
        local_event = local_event or threading.Event()
        return CompositeStopEvent(global_event, local_event), global_event, local_event

    def test_is_set_false_when_neither_event_set(self):
        composite, _global, _local = self._make_composite()
        self.assertFalse(composite.is_set())

    def test_is_set_true_when_global_event_set(self):
        composite, global_event, _local = self._make_composite()
        global_event.set()
        self.assertTrue(composite.is_set())

    def test_is_set_true_when_local_event_set(self):
        composite, _global, local_event = self._make_composite()
        local_event.set()
        self.assertTrue(composite.is_set())

    def test_set_sets_all_constituent_events(self):
        composite, global_event, local_event = self._make_composite()
        composite.set()
        self.assertTrue(global_event.is_set())
        self.assertTrue(local_event.is_set())

    def test_wait_returns_immediately_when_already_set(self):
        composite, global_event, _local = self._make_composite()
        global_event.set()
        result = composite.wait(timeout=0)
        self.assertTrue(result)

    def test_wait_returns_false_on_timeout(self):
        composite, _global, _local = self._make_composite()
        result = composite.wait(timeout=0.01)
        self.assertFalse(result)

    def test_wait_unblocks_when_event_set_from_another_thread(self):
        composite, global_event, _local = self._make_composite()
        unblocked = threading.Event()

        def waiter():
            composite.wait(timeout=5)
            unblocked.set()

        thread = threading.Thread(target=waiter)
        thread.start()
        global_event.set()
        self.assertTrue(
            unblocked.wait(timeout=2), "wait() should unblock when global event is set"
        )
        thread.join(timeout=2)
