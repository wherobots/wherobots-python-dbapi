"""Tests for Cursor class behavior.

These tests verify that:
1. SQL queries containing literal percent signs (e.g., LIKE '%good') work
   correctly regardless of whether parameters are provided.
2. Pyformat parameter substitution (%(name)s) works correctly with
   type-aware SQL quoting.
3. Unknown parameter keys raise ProgrammingError.
4. Fetches only ever observe the most recent execution's result set, and
   re-executing never cancels an already-completed statement (WBC-922).
"""

from datetime import date

import pandas
import pytest
from unittest.mock import MagicMock

from wherobots.db.cursor import Cursor, _substitute_parameters, _quote_value
from wherobots.db.errors import OperationalError, ProgrammingError
from wherobots.db.models import (
    ExecutionResult,
    StorageFormat,
    Store,
    StoreResult,
)


def _make_cursor():
    """Create a Cursor with a mock exec_fn that captures the SQL sent."""
    captured = {}

    def mock_exec_fn(sql, handler, store):
        captured["sql"] = sql
        return "exec-1"

    mock_cancel_fn = MagicMock()
    cursor = Cursor(mock_exec_fn, mock_cancel_fn)
    return cursor, captured


# ---------------------------------------------------------------------------
# _quote_value unit tests
# ---------------------------------------------------------------------------


class TestQuoteValue:
    """Unit tests for the _quote_value helper."""

    def test_none(self):
        assert _quote_value(None) == "NULL"

    def test_bool_true(self):
        assert _quote_value(True) == "TRUE"

    def test_bool_false(self):
        assert _quote_value(False) == "FALSE"

    def test_int(self):
        assert _quote_value(42) == "42"

    def test_negative_int(self):
        assert _quote_value(-7) == "-7"

    def test_float(self):
        assert _quote_value(3.14) == "3.14"

    def test_string(self):
        assert _quote_value("hello") == "'hello'"

    def test_string_with_single_quote(self):
        assert _quote_value("it's") == "'it''s'"

    def test_string_with_multiple_quotes(self):
        assert _quote_value("a'b'c") == "'a''b''c'"

    def test_empty_string(self):
        assert _quote_value("") == "''"

    def test_bytes(self):
        assert _quote_value(b"\xde\xad") == "X'dead'"

    def test_empty_bytes(self):
        assert _quote_value(b"") == "X''"

    def test_non_primitive_uses_str(self):
        """Non-primitive types fall through to str() and get quoted as strings."""
        assert _quote_value(date(2024, 1, 15)) == "'2024-01-15'"

    def test_nan_raises(self):
        with pytest.raises(ProgrammingError, match="Cannot convert float"):
            _quote_value(float("nan"))

    def test_inf_raises(self):
        with pytest.raises(ProgrammingError, match="Cannot convert float"):
            _quote_value(float("inf"))

    def test_negative_inf_raises(self):
        with pytest.raises(ProgrammingError, match="Cannot convert float"):
            _quote_value(float("-inf"))


# ---------------------------------------------------------------------------
# cursor.execute() end-to-end tests
# ---------------------------------------------------------------------------


class TestCursorExecuteParameterSubstitution:
    """Tests for pyformat parameter substitution in cursor.execute()."""

    def test_like_percent_without_parameters(self):
        """A query with a LIKE '%...' pattern and no parameters should not
        raise from Python's % string formatting."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM table WHERE name LIKE '%good'"
        cursor.execute(sql)
        assert captured["sql"] == sql

    def test_like_percent_at_end_without_parameters(self):
        """A query with a trailing percent in LIKE should work without parameters."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM table WHERE name LIKE 'good%'"
        cursor.execute(sql)
        assert captured["sql"] == sql

    def test_like_double_percent_without_parameters(self):
        """A query with percent on both sides in LIKE should work without parameters."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM table WHERE name LIKE '%good%'"
        cursor.execute(sql)
        assert captured["sql"] == sql

    def test_multiple_percent_patterns_without_parameters(self):
        """A query with multiple LIKE clauses containing percents should work."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM t WHERE a LIKE '%foo%' AND b LIKE '%bar'"
        cursor.execute(sql)
        assert captured["sql"] == sql

    def test_parameters_none_with_percent_in_query(self):
        """Explicitly passing parameters=None with a percent-containing query
        should not raise."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM table WHERE name LIKE '%good'"
        cursor.execute(sql, parameters=None)
        assert captured["sql"] == sql

    def test_empty_parameters_with_percent_in_query(self):
        """Passing an empty dict as parameters with a percent-containing query
        should not raise."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM table WHERE name LIKE '%good'"
        cursor.execute(sql, parameters={})
        assert captured["sql"] == sql

    def test_parameter_substitution_works(self):
        """Named pyformat parameter substitution should work correctly."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM table WHERE id = %(id)s"
        cursor.execute(sql, parameters={"id": 42})
        assert captured["sql"] == "SELECT * FROM table WHERE id = 42"

    def test_multiple_parameters(self):
        """Multiple named parameters should all be substituted with proper quoting."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM t WHERE id = %(id)s AND name = %(name)s"
        cursor.execute(sql, parameters={"id": 1, "name": "alice"})
        assert captured["sql"] == "SELECT * FROM t WHERE id = 1 AND name = 'alice'"

    def test_like_with_parameters(self):
        """A LIKE expression with literal percent signs should work alongside
        named parameters without requiring %% escaping."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM table WHERE name LIKE '%good%' AND id = %(id)s"
        cursor.execute(sql, parameters={"id": 42})
        assert captured["sql"] == (
            "SELECT * FROM table WHERE name LIKE '%good%' AND id = 42"
        )

    def test_string_parameter_is_quoted(self):
        """String parameters should be single-quoted in the output SQL."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM t WHERE category = %(cat)s"
        cursor.execute(sql, parameters={"cat": "restaurant"})
        assert captured["sql"] == "SELECT * FROM t WHERE category = 'restaurant'"

    def test_none_parameter_becomes_null(self):
        """None parameters should become SQL NULL."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM t WHERE deleted_at = %(val)s"
        cursor.execute(sql, parameters={"val": None})
        assert captured["sql"] == "SELECT * FROM t WHERE deleted_at = NULL"

    def test_bool_parameter(self):
        """Boolean parameters should become TRUE/FALSE."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM t WHERE active = %(flag)s"
        cursor.execute(sql, parameters={"flag": True})
        assert captured["sql"] == "SELECT * FROM t WHERE active = TRUE"

    def test_string_with_quote_is_escaped(self):
        """Single quotes in string parameters should be escaped."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM t WHERE name = %(name)s"
        cursor.execute(sql, parameters={"name": "O'Brien"})
        assert captured["sql"] == "SELECT * FROM t WHERE name = 'O''Brien'"

    def test_plain_query_without_parameters(self):
        """A simple query with no percent signs and no parameters should work."""
        cursor, captured = _make_cursor()
        sql = "SELECT * FROM table"
        cursor.execute(sql)
        assert captured["sql"] == sql

    def test_unknown_parameter_raises(self):
        """Referencing a parameter key not in the dict should raise ProgrammingError."""
        cursor, _ = _make_cursor()
        sql = "SELECT * FROM table WHERE id = %(missing)s"
        with pytest.raises(ProgrammingError, match="missing"):
            cursor.execute(sql, parameters={"id": 42})


# ---------------------------------------------------------------------------
# Result isolation and cancellation tests (WBC-922)
# ---------------------------------------------------------------------------


def _make_async_cursor():
    """Create a Cursor whose exec_fn records each execution's handler.

    Tests deliver results by invoking a recorded handler, mimicking the
    connection's asynchronous result callbacks.
    """
    handlers = []

    def exec_fn(sql, handler, store):
        handlers.append(handler)
        return f"exec-{len(handlers)}"

    cancel_fn = MagicMock()
    return Cursor(exec_fn, cancel_fn), handlers, cancel_fn


class TestCursorResultIsolation:
    """Fetches must only observe the most recent execution's result set."""

    def test_unfetched_result_does_not_leak_into_next_execute(self):
        cursor, handlers, _ = _make_async_cursor()

        cursor.execute("SELECT 1")
        handlers[0](ExecutionResult(results=pandas.DataFrame({"x": [1]})))

        # Re-execute without fetching the first result.
        cursor.execute("SELECT 2")
        handlers[1](ExecutionResult(results=pandas.DataFrame({"x": [2]})))

        assert cursor.fetchall()["x"].tolist() == [2]

    def test_late_result_from_superseded_execution_is_ignored(self):
        cursor, handlers, _ = _make_async_cursor()

        cursor.execute("SELECT 1")
        # First query still in flight when the second is executed.
        cursor.execute("SELECT 2")

        # The first query's result arrives late (e.g. the empty result the
        # connection delivers for a cancelled query), then the second's.
        handlers[0](ExecutionResult(results=pandas.DataFrame()))
        handlers[1](ExecutionResult(results=pandas.DataFrame({"x": [2]})))

        assert cursor.fetchall()["x"].tolist() == [2]

    def test_fetch_after_fetch_returns_same_results(self):
        cursor, handlers, _ = _make_async_cursor()

        cursor.execute("SELECT 1")
        handlers[0](ExecutionResult(results=pandas.DataFrame({"x": [1]})))

        assert cursor.fetchall()["x"].tolist() == [1]
        assert cursor.fetchall()["x"].tolist() == [1]

    def test_get_store_result_is_idempotent(self):
        """A second get_store_result() must not block on the drained queue."""
        cursor, handlers, _ = _make_async_cursor()

        cursor.execute("SELECT 1", store=Store(format=StorageFormat.PARQUET))
        handlers[0](
            ExecutionResult(store_result=StoreResult(result_uri="s3://r/1", size=42))
        )

        assert cursor.get_store_result().result_uri == "s3://r/1"
        assert cursor.get_store_result().result_uri == "s3://r/1"

    def test_fetch_after_error_reraises(self):
        """Repeated fetches of a failed execution re-raise its error instead
        of blocking on the drained queue."""
        cursor, handlers, _ = _make_async_cursor()

        cursor.execute("SELECT broken")
        handlers[0](ExecutionResult(error=OperationalError("boom")))

        with pytest.raises(OperationalError, match="boom"):
            cursor.fetchall()
        with pytest.raises(OperationalError, match="boom"):
            cursor.fetchall()


class TestCursorCancellation:
    """Only genuinely in-flight executions may be cancelled."""

    def test_execute_cancels_in_flight_previous_query(self):
        cursor, _, cancel_fn = _make_async_cursor()

        cursor.execute("SELECT 1")
        cursor.execute("SELECT 2")

        cancel_fn.assert_called_once_with("exec-1")

    def test_execute_does_not_cancel_completed_previous_query(self):
        cursor, handlers, cancel_fn = _make_async_cursor()

        cursor.execute("MERGE INTO t USING s ON t.id = s.id ...")
        handlers[0](ExecutionResult(results=pandas.DataFrame()))

        # The DML completed (result queued, not fetched); executing another
        # statement must not attempt to cancel it.
        cursor.execute("SELECT 1")

        cancel_fn.assert_not_called()

    def test_close_cancels_in_flight_query(self):
        cursor, _, cancel_fn = _make_async_cursor()

        cursor.execute("SELECT 1")
        cursor.close()

        cancel_fn.assert_called_once_with("exec-1")

    def test_close_does_not_cancel_completed_query(self):
        cursor, handlers, cancel_fn = _make_async_cursor()

        cursor.execute("SELECT 1")
        handlers[0](ExecutionResult(results=pandas.DataFrame({"x": [1]})))
        cursor.close()

        cancel_fn.assert_not_called()

    def test_close_without_execute_does_not_cancel(self):
        cursor, _, cancel_fn = _make_async_cursor()

        cursor.close()

        cancel_fn.assert_not_called()

    def test_execute_does_not_cancel_completed_store_query(self):
        """Store-backed executions never populate __results; completion must
        still be recognized so they aren't cancelled (WBC-922 review)."""
        cursor, handlers, cancel_fn = _make_async_cursor()

        cursor.execute("SELECT * FROM t", store=Store(format=StorageFormat.PARQUET))
        handlers[0](
            ExecutionResult(store_result=StoreResult(result_uri="s3://r/1", size=42))
        )
        assert cursor.get_store_result().result_uri == "s3://r/1"

        cursor.execute("SELECT 1")

        cancel_fn.assert_not_called()

    def test_close_does_not_cancel_completed_store_query(self):
        cursor, handlers, cancel_fn = _make_async_cursor()

        cursor.execute("SELECT * FROM t", store=Store(format=StorageFormat.PARQUET))
        handlers[0](
            ExecutionResult(store_result=StoreResult(result_uri="s3://r/1", size=42))
        )
        cursor.get_store_result()
        cursor.close()

        cancel_fn.assert_not_called()

    def test_execute_does_not_cancel_completed_empty_result_query(self):
        """An execution that completes with neither rows nor a store result
        (e.g. store configured but empty result set) must not be cancelled."""
        cursor, handlers, cancel_fn = _make_async_cursor()

        cursor.execute(
            "SELECT 1 WHERE 1 = 0", store=Store(format=StorageFormat.PARQUET)
        )
        handlers[0](ExecutionResult())
        assert cursor.get_store_result() is None

        cursor.execute("SELECT 2")

        cancel_fn.assert_not_called()

    def test_execute_does_not_cancel_failed_query(self):
        """A failed execution is terminal; re-executing must not cancel it."""
        cursor, handlers, cancel_fn = _make_async_cursor()

        cursor.execute("SELECT broken")
        handlers[0](ExecutionResult(error=OperationalError("boom")))
        with pytest.raises(OperationalError):
            cursor.fetchall()

        cursor.execute("SELECT 1")

        cancel_fn.assert_not_called()


# ---------------------------------------------------------------------------
# _substitute_parameters unit tests
# ---------------------------------------------------------------------------


class TestSubstituteParameters:
    """Unit tests for the _substitute_parameters helper directly."""

    def test_no_parameters_returns_operation_unchanged(self):
        sql = "SELECT * FROM t WHERE name LIKE '%test%'"
        assert _substitute_parameters(sql, None) == sql

    def test_empty_dict_returns_operation_unchanged(self):
        sql = "SELECT * FROM t WHERE name LIKE '%test%'"
        assert _substitute_parameters(sql, {}) == sql

    def test_substitutes_named_param(self):
        sql = "SELECT * FROM t WHERE id = %(id)s"
        assert _substitute_parameters(sql, {"id": 99}) == (
            "SELECT * FROM t WHERE id = 99"
        )

    def test_preserves_literal_percent_with_params(self):
        sql = "SELECT * FROM t WHERE name LIKE '%foo%' AND id = %(id)s"
        assert _substitute_parameters(sql, {"id": 1}) == (
            "SELECT * FROM t WHERE name LIKE '%foo%' AND id = 1"
        )

    def test_unknown_key_raises_programming_error(self):
        sql = "SELECT * FROM t WHERE id = %(nope)s"
        with pytest.raises(ProgrammingError, match="nope"):
            _substitute_parameters(sql, {"id": 1})

    def test_repeated_param_substituted_everywhere(self):
        sql = "SELECT * FROM t WHERE a = %(v)s OR b = %(v)s"
        assert _substitute_parameters(sql, {"v": 7}) == (
            "SELECT * FROM t WHERE a = 7 OR b = 7"
        )

    def test_bare_percent_s_not_treated_as_param(self):
        """A bare %s (format-style, not pyformat) should be left untouched."""
        sql = "SELECT * FROM t WHERE id = %s"
        assert _substitute_parameters(sql, {"id": 1}) == sql

    def test_string_param_is_quoted(self):
        sql = "SELECT * FROM t WHERE name = %(name)s"
        assert _substitute_parameters(sql, {"name": "alice"}) == (
            "SELECT * FROM t WHERE name = 'alice'"
        )

    def test_string_param_escapes_quotes(self):
        sql = "SELECT * FROM t WHERE name = %(name)s"
        assert _substitute_parameters(sql, {"name": "it's"}) == (
            "SELECT * FROM t WHERE name = 'it''s'"
        )

    def test_none_param_becomes_null(self):
        sql = "SELECT * FROM t WHERE val = %(v)s"
        assert _substitute_parameters(sql, {"v": None}) == (
            "SELECT * FROM t WHERE val = NULL"
        )
