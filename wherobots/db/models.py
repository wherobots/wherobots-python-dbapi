from dataclasses import dataclass, field
import re
from typing import Any, Dict

import pandas

from .constants import DEFAULT_STORAGE_FORMAT
from .errors import ProgrammingError
from .types import StorageFormat


@dataclass(frozen=True)
class ExecutionOwnership:
    """A claim from the coordinator for a preallocated SQL execution ID."""

    execution_id: str
    generation: int
    signature: str = field(repr=False)

    def __post_init__(self) -> None:
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or not 1 <= self.generation <= 2**63 - 1
        ):
            raise ProgrammingError(
                "Ownership generation must be a positive 64-bit integer"
            )
        if not isinstance(self.signature, str) or not re.fullmatch(
            r"[0-9a-fA-F]{128}", self.signature
        ):
            raise ProgrammingError(
                "Ownership signature must contain 128 hex characters"
            )
        if not isinstance(self.execution_id, str) or not re.fullmatch(
            r"owned_[0-9a-f]{64}", self.execution_id
        ):
            raise ProgrammingError(
                "Protected executions require an owned_ ID with 64 lowercase hex characters"
            )

    def claim(self) -> dict[str, str | int]:
        return {"generation": self.generation, "signature": self.signature}


@dataclass(frozen=True)
class StoreResult:
    """Result information when a query's results are stored to cloud storage.

    Attributes:
        result_uri: The URI or presigned URL of the stored result.
        size: The size of the stored result in bytes, or None if not available.
    """

    result_uri: str
    size: int | None = None


@dataclass
class Store:
    """Configuration for storing query results to cloud storage.

    When passed to cursor.execute(), query results will be written to cloud
    storage instead of being returned directly over the WebSocket connection.

    Attributes:
        format: The storage format (parquet, csv, or geojson). Defaults to parquet.
        single: If True, store as a single file. If False, store as multiple files.
        generate_presigned_url: If True, generate a presigned URL for the result.
            Requires single=True.
        options: Optional dict of format-specific Spark DataFrameWriter options
            (e.g. ``{"header": "false", "delimiter": "|"}`` for CSV). These are
            applied after the server's default options, so they can override them.
            An empty dict is normalized to None.
    """

    format: StorageFormat
    single: bool = False
    generate_presigned_url: bool = False
    options: dict[str, str] | None = None

    def __post_init__(self) -> None:
        if self.generate_presigned_url and not self.single:
            raise ValueError("Presigned URL can only be generated when single=True")
        # Normalize empty options to None and defensively copy.
        if self.options:
            self.options = dict(self.options)
        else:
            self.options = None

    @classmethod
    def for_download(
        cls,
        format: StorageFormat | None = None,
        options: dict[str, str] | None = None,
    ) -> "Store":
        """Create a configuration for downloading results via a presigned URL.

        This is a convenience method that creates a configuration with
        single file mode and presigned URL generation enabled.

        Args:
            format: The storage format.
            options: Optional format-specific Spark DataFrameWriter options.

        Returns:
            A Store configured for single-file download with presigned URL.
        """
        return cls(
            format=format or DEFAULT_STORAGE_FORMAT,
            single=True,
            generate_presigned_url=True,
            options=options,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize this Store to a dict for the WebSocket request.

        Returns a dict suitable for inclusion as the ``"store"`` field in an
        ``execute_sql`` request.  The ``options`` key is omitted when there
        are no user-supplied options (backward compatible).
        """
        d: Dict[str, Any] = {
            "format": self.format.value,
            "single": str(self.single).lower(),
            "generate_presigned_url": str(self.generate_presigned_url).lower(),
        }
        if self.options:
            d["options"] = self.options
        return d


@dataclass
class ExecutionResult:
    """Result of a query execution.

    This class encapsulates all possible outcomes of a query execution:
    a DataFrame result, an error, or a store result (when results are
    written to cloud storage).

    Attributes:
        results: The query results as a pandas DataFrame, or None if an error occurred.
        error: The error that occurred during execution, or None if successful.
        store_result: The store result if results were written to cloud storage.
    """

    results: pandas.DataFrame | None = None
    error: Exception | None = None
    store_result: StoreResult | None = None


@dataclass(frozen=True)
class ProgressInfo:
    """Progress information for a running query.

    Mirrors the ``execution_progress`` event sent by the SQL session.
    """

    execution_id: str
    tasks_total: int
    tasks_completed: int
    tasks_active: int
