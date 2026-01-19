from dataclasses import dataclass

import pandas

from .constants import DEFAULT_STORAGE_FORMAT
from .types import StorageFormat


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
    """

    format: StorageFormat
    single: bool = False
    generate_presigned_url: bool = False

    def __post_init__(self) -> None:
        if self.generate_presigned_url and not self.single:
            raise ValueError("Presigned URL can only be generated when single=True")

    @classmethod
    def for_download(cls, format: StorageFormat | None = None) -> "Store":
        """Create a configuration for downloading results via a presigned URL.

        This is a convenience method that creates a configuration with
        single file mode and presigned URL generation enabled.

        Args:
            format: The storage format.

        Returns:
            A Store configured for single-file download with presigned URL.
        """
        return cls(
            format=format or DEFAULT_STORAGE_FORMAT,
            single=True,
            generate_presigned_url=True,
        )


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
