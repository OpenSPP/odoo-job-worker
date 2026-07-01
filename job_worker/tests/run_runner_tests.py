#!/usr/bin/env python3
"""Run the runner-related tests without requiring Odoo.

These tests are pure Python / unittest and only exercise orchestration
logic (CompositeStopEvent, discovery helpers, QueueJobRunner).  They
mock all Odoo and psycopg2 interactions.

Usage:
    python job_worker/tests/run_runner_tests.py [-v]
"""

import importlib.util
import os
import sys
import unittest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

_RUNNER_TEST_FILES = [
    "test_composite_stop_event.py",
    "test_runner_discovery.py",
    "test_runner_lifecycle.py",
    "test_runner_thread_management.py",
    "test_heartbeat.py",
]


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    verbosity = 2 if "-v" in sys.argv else 1
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for filename in _RUNNER_TEST_FILES:
        filepath = os.path.join(_TESTS_DIR, filename)
        if not os.path.exists(filepath):
            continue
        mod = _load_module(filename.removesuffix(".py"), filepath)
        suite.addTests(loader.loadTestsFromModule(mod))
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
