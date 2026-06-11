"""Redaction helpers for SQL statements logged by this driver.

The driver logs SQL statements for observability (e.g. ``Executing SQL query
<id>: ...``). Raw statements can embed literal PII (for example ``WHERE ssn =
'123-45-6789'``), and because this library is embedded by other services its log
output ends up in their log streams. This module replaces literal *values*
(single-quoted string literals and numeric literals) with a ``?`` placeholder
while preserving the statement structure: keywords, function names, and
identifiers (including double-quoted/back-quoted identifiers) are kept intact so
the redacted form is still useful for debugging and aggregation.

The implementation tokenizes with ``sqlparse``, a lenient, pure-Python tokenizer
with zero transitive dependencies. It is dialect-agnostic, never raises on
malformed input, and classifies literals by token type, which lets us redact
precisely the value-bearing tokens while copying everything else (keywords,
identifiers, operators, comments, whitespace) through verbatim.

This logic is duplicated from the ``sql-session`` service (PR #197); the two
repositories do not share a package, so the implementation is intentionally
replicated here rather than imported.

This is a best-effort redaction for *logging*. It is not a security boundary and
must not be relied on to sanitize untrusted input for any other purpose.
"""

import re

import sqlparse
from sqlparse import tokens as T

REDACTED_PLACEHOLDER = "?"

# Leading SQL keyword -> statement type. Used purely for observability tagging.
# Intentionally a regex (not sqlparse's ``Statement.get_type()``) because
# ``get_type()`` returns "UNKNOWN" for SHOW/DESCRIBE/SET, which would regress the
# observability tagging this module exists to support.
_STATEMENT_TYPE_RE = re.compile(r"\s*([A-Za-z]+)")


def get_statement_type(statement: str) -> str:
    """Return the upper-cased leading keyword of a statement (e.g. ``SELECT``).

    Returns ``"UNKNOWN"`` when the statement does not begin with a word.
    """
    match = _STATEMENT_TYPE_RE.match(statement)
    if match is None:
        return "UNKNOWN"
    return match.group(1).upper()


def redact_sql(statement: str) -> str:
    """Return ``statement`` with string and numeric literals replaced by ``?``.

    Single-quoted string literals (``String.Single``) and numeric literals
    (``Number.*``) are each collapsed to a single ``?`` placeholder. Everything
    else -- keywords, function names, plain and quoted identifiers (``"col"`` /
    `` `col` ``), comments, whitespace, and operators -- is preserved verbatim.
    """
    out: list[str] = []
    for stmt in sqlparse.parse(statement):
        # sqlparse ships no type stubs, so ``flatten`` is untyped under strict.
        for token in stmt.flatten():  # type: ignore[no-untyped-call]
            ttype = token.ttype
            if ttype in T.String.Single or ttype in T.Number:
                out.append(REDACTED_PLACEHOLDER)
            elif ttype in T.Error and token.value in ("'", '"', "`"):
                # An unterminated quote: sqlparse emits the lone opener as an
                # Error token and tokenizes the trailing characters as ordinary
                # text. Redact the opener and drop the remainder of this
                # statement so the unterminated value cannot leak.
                out.append(REDACTED_PLACEHOLDER)
                break
            else:
                out.append(token.value)
    return "".join(out)
