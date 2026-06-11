"""Tests for SQL literal redaction used in query logging."""

import pytest

from wherobots.db.redaction import get_statement_type, redact_sql


def test_redacts_single_string_literal() -> None:
    assert redact_sql("SELECT * FROM t WHERE name = 'alice'") == (
        "SELECT * FROM t WHERE name = ?"
    )


def test_redacts_string_with_escaped_quote() -> None:
    # Doubled single-quote is an escaped quote inside the literal; the whole
    # literal must collapse to one placeholder (no leakage of the second half).
    assert redact_sql("SELECT * FROM t WHERE name = 'O''Brien'") == (
        "SELECT * FROM t WHERE name = ?"
    )


def test_redacts_string_with_backslash_escaped_quote() -> None:
    assert redact_sql(r"SELECT * FROM t WHERE name = 'a\'b'") == (
        "SELECT * FROM t WHERE name = ?"
    )


def test_redacts_numeric_literals() -> None:
    assert redact_sql("SELECT * FROM t WHERE age = 42 AND score > 3.14") == (
        "SELECT * FROM t WHERE age = ? AND score > ?"
    )


def test_redacts_scientific_notation() -> None:
    assert redact_sql("SELECT * FROM t WHERE x < 1.5e10") == (
        "SELECT * FROM t WHERE x < ?"
    )


def test_double_quoted_identifier_left_intact() -> None:
    # Double-quoted identifiers are column/table names, not value literals.
    assert redact_sql('SELECT "user id", count(*) FROM "my table" WHERE x = 1') == (
        'SELECT "user id", count(*) FROM "my table" WHERE x = ?'
    )


def test_backtick_identifier_left_intact() -> None:
    assert redact_sql("SELECT `col` FROM `db`.`tbl` WHERE n = 5") == (
        "SELECT `col` FROM `db`.`tbl` WHERE n = ?"
    )


def test_show_tblproperties_statement() -> None:
    # SHOW TBLPROPERTIES with a quoted property key: the single-quoted literal
    # still redacts, while the table name (an identifier) stays intact.
    assert redact_sql("SHOW TBLPROPERTIES my_db.my_table ('comment')") == (
        "SHOW TBLPROPERTIES my_db.my_table (?)"
    )


def test_select_where_secret_redacted() -> None:
    statement = "SELECT id FROM users WHERE ssn = '123-45-6789'"
    redacted = redact_sql(statement)
    assert "123-45-6789" not in redacted
    assert redacted == "SELECT id FROM users WHERE ssn = ?"


def test_identifier_with_digits_not_redacted() -> None:
    # Column names containing digits must not be treated as numeric literals.
    assert redact_sql("SELECT col1, t2.col3 FROM tbl4") == (
        "SELECT col1, t2.col3 FROM tbl4"
    )


def test_unterminated_string_does_not_leak() -> None:
    redacted = redact_sql("SELECT * FROM t WHERE x = 'unterminated secret")
    assert "secret" not in redacted
    assert redacted == "SELECT * FROM t WHERE x = ?"


def test_multiple_literals_mixed() -> None:
    statement = "INSERT INTO t (a, b) VALUES ('x', 10), ('y', 20)"
    assert redact_sql(statement) == "INSERT INTO t (a, b) VALUES (?, ?), (?, ?)"


def test_line_comment_preserved_literal_in_comment_kept() -> None:
    # We only redact value positions; a literal inside a comment is left as-is
    # because comments are preserved verbatim (structure-preserving).
    assert redact_sql("SELECT 1 -- note: keep this\n") == (
        "SELECT ? -- note: keep this\n"
    )


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        ("SELECT 1", "SELECT"),
        ("  show tables", "SHOW"),
        ("describe t", "DESCRIBE"),
        ("/* c */ SELECT 1", "UNKNOWN"),
        ("", "UNKNOWN"),
    ],
)
def test_get_statement_type(statement: str, expected: str) -> None:
    assert get_statement_type(statement) == expected
