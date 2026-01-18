from dataclasses import dataclass
from enum import auto
from strenum import LowercaseStrEnum


class StorageFormat(LowercaseStrEnum):
    """Storage formats for storing query results to cloud storage."""

    PARQUET = auto()
    CSV = auto()
    GEOJSON = auto()


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

    format: StorageFormat | None = None
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
            format: The storage format. Defaults to parquet if not specified.

        Returns:
            A Store configured for single-file download with presigned URL.
        """
        return cls(format=format, single=True, generate_presigned_url=True)
