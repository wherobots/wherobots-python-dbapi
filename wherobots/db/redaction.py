"""Redaction helpers for SQL statements logged by this driver.

The driver logs SQL statements for observability (e.g. ``Executing SQL query
<id>: ...``). Raw statements can embed literal PII (for example ``WHERE ssn =
'123-45-6789'``), and because this library is embedded by other services its log
output ends up in their log streams. This module replaces literal *values*
(string and numeric literals) with a ``?`` placeholder while preserving the
statement structure: keywords, function names, and identifiers (including
double-quoted/back-quoted identifiers) are kept intact so the redacted form is
still useful for debugging and aggregation.

The implementation is a single-pass tokenizer rather than a full SQL parser:
``sqlglot``/``sqlparse`` are not dependencies of this driver and parsing every
dialect Sedona accepts would be brittle. The tokenizer is deliberately
conservative -- when in doubt it leaves text untouched -- and is written to
avoid catastrophic backtracking by scanning character-by-character.

This logic is duplicated from the ``sql-session`` service (PR #197); the two
repositories do not share a package, so the implementation is intentionally
replicated here rather than imported.

Limitations:
- Dollar-quoted strings and other exotic literal syntaxes are not recognized and
  would pass through unredacted. Sedona/Spark SQL does not use them, so this is
  acceptable for the statements this driver runs.
- This is a best-effort redaction for *logging*. It is not a security boundary
  and must not be relied on to sanitize untrusted input for any other purpose.
"""

import re

REDACTED_PLACEHOLDER = "?"

# Leading SQL keyword -> statement type. Used purely for observability tagging.
_STATEMENT_TYPE_RE = re.compile(r"\s*([A-Za-z]+)")

# A numeric literal: integers, decimals, scientific notation. A leading sign is
# intentionally excluded so that ``a-1`` redacts the ``1`` and keeps the
# operator. Matched only when not part of a larger identifier (handled by the
# tokenizer, which only tests this at value positions).
_NUMERIC_LITERAL_RE = re.compile(
    r"""
    (?<![\w.])          # not preceded by an identifier char or a dot
    \d+                 # integer part
    (?:\.\d+)?          # optional fractional part
    (?:[eE][+-]?\d+)?   # optional exponent
    (?![\w.])           # not followed by an identifier char or a dot
    """,
    re.VERBOSE,
)


def get_statement_type(statement: str) -> str:
    """Return the upper-cased leading keyword of a statement (e.g. ``SELECT``).

    Returns ``"UNKNOWN"`` when the statement does not begin with a word.
    """
    match = _STATEMENT_TYPE_RE.match(statement)
    if match is None:
        return "UNKNOWN"
    return match.group(1).upper()


def _consume_quoted(statement: str, start: int, quote: str) -> tuple[str, int]:
    """Consume a quoted run beginning at ``start`` (which points at ``quote``).

    Handles doubled-quote escaping (``''`` / ``""``) and backslash escaping. The
    returned string is the full literal *including* its surrounding quotes.
    Returns the consumed text and the index just past the closing quote (or the
    end of string for an unterminated literal).
    """
    i = start + 1
    n = len(statement)
    while i < n:
        ch = statement[i]
        if ch == "\\":
            # Skip the escaped character (if any).
            i += 2
            continue
        if ch == quote:
            if i + 1 < n and statement[i + 1] == quote:
                # Doubled quote -> escaped quote, stays inside the literal.
                i += 2
                continue
            # Closing quote.
            return statement[start : i + 1], i + 1
        i += 1
    # Unterminated literal: consume to end of string.
    return statement[start:], n


def redact_sql(statement: str) -> str:
    """Return ``statement`` with string and numeric literals replaced by ``?``.

    Keywords, identifiers, and quoted identifiers (``"col"`` / `` `col` ``) are
    preserved. Single-quoted string literals are replaced wholesale with a single
    ``?`` placeholder, as are numeric literals.
    """
    result: list[str] = []
    i = 0
    n = len(statement)

    while i < n:
        ch = statement[i]

        # Single-quoted string literal -> redact to a placeholder.
        if ch == "'":
            _, end = _consume_quoted(statement, i, "'")
            result.append(REDACTED_PLACEHOLDER)
            i = end
            continue

        # Double-quoted or back-quoted identifier -> preserve verbatim. These are
        # identifiers (column/table names) in Spark SQL, not value literals.
        if ch in ('"', "`"):
            text, end = _consume_quoted(statement, i, ch)
            result.append(text)
            i = end
            continue

        # Line comment: preserve to end of line.
        if statement.startswith("--", i):
            end = statement.find("\n", i)
            if end == -1:
                end = n
            result.append(statement[i:end])
            i = end
            continue

        # Block comment: preserve verbatim.
        if statement.startswith("/*", i):
            end = statement.find("*/", i + 2)
            end = n if end == -1 else end + 2
            result.append(statement[i:end])
            i = end
            continue

        # Numeric literal at this position -> redact.
        match = _NUMERIC_LITERAL_RE.match(statement, i)
        if match is not None:
            result.append(REDACTED_PLACEHOLDER)
            i = match.end()
            continue

        result.append(ch)
        i += 1

    return "".join(result)
