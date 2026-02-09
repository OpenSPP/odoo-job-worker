import importlib.util
import os
import unittest
from unittest.mock import MagicMock, patch

# Load runner module with stubbed Odoo dependencies.
_helpers_path = os.path.join(os.path.dirname(__file__), "_runner_test_helpers.py")
_spec = importlib.util.spec_from_file_location("_runner_test_helpers", _helpers_path)
_helpers = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_helpers)
_runner = _helpers.load_runner()

_get_database_names = _runner._get_database_names
_database_has_module = _runner._database_has_module
_try_advisory_lock = _runner._try_advisory_lock
PG_ADVISORY_LOCK_ID = _runner.PG_ADVISORY_LOCK_ID


class TestGetDatabaseNames(unittest.TestCase):
    """Tests for _get_database_names()."""

    @patch.object(_runner, "odoo")
    def test_get_database_names_from_config_single(self, mock_odoo):
        mock_odoo.tools.config = {"db_name": "mydb"}
        result = _get_database_names()
        self.assertEqual(result, ["mydb"])

    @patch.object(_runner, "odoo")
    def test_get_database_names_from_config_multiple(self, mock_odoo):
        mock_odoo.tools.config = {"db_name": "db1,db2"}
        result = _get_database_names()
        self.assertEqual(result, ["db1", "db2"])

    @patch.object(_runner, "odoo")
    def test_get_database_names_strips_whitespace(self, mock_odoo):
        mock_odoo.tools.config = {"db_name": " db1 , db2 "}
        result = _get_database_names()
        self.assertEqual(result, ["db1", "db2"])

    @patch.object(_runner, "odoo")
    def test_get_database_names_falls_back_to_list_dbs(self, mock_odoo):
        mock_odoo.tools.config = {"db_name": ""}
        mock_odoo.service.db.list_dbs.return_value = ["alpha", "beta"]
        result = _get_database_names()
        mock_odoo.service.db.list_dbs.assert_called_once_with(True)
        self.assertEqual(result, ["alpha", "beta"])


class TestDatabaseHasModule(unittest.TestCase):
    """Tests for _database_has_module()."""

    def _mock_cursor(self, fetchone_sequence):
        """Build a mock connection whose cursor yields fetchone values in order."""
        cursor = MagicMock()
        cursor.fetchone = MagicMock(side_effect=fetchone_sequence)
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    @patch.object(_runner, "psycopg2")
    @patch.object(_runner, "odoo")
    def test_database_has_module_true_when_installed(self, mock_odoo, mock_psycopg2):
        mock_odoo.sql_db.connection_info_for.return_value = ({"dbname": "testdb"},)
        conn, cursor = self._mock_cursor([(1,), (1,)])
        mock_psycopg2.connect.return_value = conn
        self.assertTrue(_database_has_module("testdb"))

    @patch.object(_runner, "psycopg2")
    @patch.object(_runner, "odoo")
    def test_database_has_module_false_for_non_odoo_database(
        self, mock_odoo, mock_psycopg2
    ):
        mock_odoo.sql_db.connection_info_for.return_value = ({"dbname": "testdb"},)
        conn, cursor = self._mock_cursor([None])
        mock_psycopg2.connect.return_value = conn
        self.assertFalse(_database_has_module("testdb"))

    @patch.object(_runner, "psycopg2")
    @patch.object(_runner, "odoo")
    def test_database_has_module_false_when_not_installed(
        self, mock_odoo, mock_psycopg2
    ):
        mock_odoo.sql_db.connection_info_for.return_value = ({"dbname": "testdb"},)
        conn, cursor = self._mock_cursor([(1,), None])
        mock_psycopg2.connect.return_value = conn
        self.assertFalse(_database_has_module("testdb"))

    @patch.object(_runner, "psycopg2")
    @patch.object(_runner, "odoo")
    def test_database_has_module_closes_connection_on_success(
        self, mock_odoo, mock_psycopg2
    ):
        mock_odoo.sql_db.connection_info_for.return_value = ({"dbname": "testdb"},)
        conn, cursor = self._mock_cursor([(1,), (1,)])
        mock_psycopg2.connect.return_value = conn
        _database_has_module("testdb")
        conn.close.assert_called_once()

    @patch.object(_runner, "psycopg2")
    @patch.object(_runner, "odoo")
    def test_database_has_module_closes_connection_on_error(
        self, mock_odoo, mock_psycopg2
    ):
        mock_odoo.sql_db.connection_info_for.return_value = ({"dbname": "testdb"},)
        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("query failed")
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor
        mock_psycopg2.connect.return_value = conn
        self.assertFalse(_database_has_module("testdb"))
        conn.close.assert_called_once()

    @patch.object(_runner, "psycopg2")
    @patch.object(_runner, "odoo")
    def test_database_has_module_handles_connection_failure(
        self, mock_odoo, mock_psycopg2
    ):
        mock_odoo.sql_db.connection_info_for.return_value = ({"dbname": "testdb"},)
        mock_psycopg2.OperationalError = type("OperationalError", (Exception,), {})
        mock_psycopg2.connect.side_effect = mock_psycopg2.OperationalError("refused")
        self.assertFalse(_database_has_module("testdb"))


class TestAdvisoryLock(unittest.TestCase):
    """Tests for _try_advisory_lock()."""

    @patch.object(_runner, "psycopg2")
    @patch.object(_runner, "odoo")
    def test_advisory_lock_success_returns_connection(self, mock_odoo, mock_psycopg2):
        mock_odoo.sql_db.connection_info_for.return_value = ({"dbname": "testdb"},)
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (True,)
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor
        mock_psycopg2.connect.return_value = conn
        result = _try_advisory_lock("testdb")
        self.assertIs(result, conn)

    @patch.object(_runner, "psycopg2")
    @patch.object(_runner, "odoo")
    def test_advisory_lock_failure_returns_none(self, mock_odoo, mock_psycopg2):
        mock_odoo.sql_db.connection_info_for.return_value = ({"dbname": "testdb"},)
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (False,)
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor
        mock_psycopg2.connect.return_value = conn
        result = _try_advisory_lock("testdb")
        self.assertIsNone(result)
        conn.close.assert_called_once()

    @patch.object(_runner, "psycopg2")
    @patch.object(_runner, "odoo")
    def test_advisory_lock_uses_correct_lock_identifier(self, mock_odoo, mock_psycopg2):
        mock_odoo.sql_db.connection_info_for.return_value = ({"dbname": "testdb"},)
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (True,)
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor
        mock_psycopg2.connect.return_value = conn
        _try_advisory_lock("testdb")
        cursor.execute.assert_called_once()
        call_args = cursor.execute.call_args
        self.assertIn("pg_try_advisory_lock", call_args[0][0])
        self.assertEqual(call_args[0][1], (PG_ADVISORY_LOCK_ID,))

    @patch.object(_runner, "psycopg2")
    @patch.object(_runner, "odoo")
    def test_advisory_lock_handles_connection_failure(self, mock_odoo, mock_psycopg2):
        mock_odoo.sql_db.connection_info_for.return_value = ({"dbname": "testdb"},)
        mock_psycopg2.OperationalError = type("OperationalError", (Exception,), {})
        mock_psycopg2.connect.side_effect = mock_psycopg2.OperationalError("refused")
        result = _try_advisory_lock("testdb")
        self.assertIsNone(result)
