"""Tests for Cursor class behavior.

These tests verify that:
1. SQL queries containing literal percent signs (e.g., LIKE '%good') work
   correctly regardless of whether parameters are provided.
2. Pyformat parameter substitution (%(name)s) works correctly with
   type-aware SQL quoting.
3. Unknown parameter keys raise ProgrammingError.
"""

import pytest
from unittest.mock import MagicMock

from wherobots.db.cursor import Cursor, _substitute_parameters, _quote_value
from wherobots.db.errors import ProgrammingError


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
        from datetime import date

        assert _quote_value(date(2024, 1, 15)) == "'2024-01-15'"


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
