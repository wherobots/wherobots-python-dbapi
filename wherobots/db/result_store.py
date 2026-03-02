"""Result storage configuration for Wherobots DB queries.

Provides the :class:`StorageFormat` enum and :class:`Store` dataclass to configure
how query results are stored in cloud storage (e.g. S3) instead of being returned
directly over the WebSocket connection.
"""

from dataclasses import dataclass, field
from enum import auto
from typing import Dict, Optional

from strenum import LowercaseStrEnum


class StorageFormat(LowercaseStrEnum):
    """Supported formats for storing query results."""

    PARQUET = auto()
    CSV = auto()
    GEOJSON = auto()


DEFAULT_STORAGE_FORMAT = StorageFormat.PARQUET


@dataclass(frozen=True)
class Store:
    """Configuration for storing query results to cloud storage.

    When a :class:`Store` is provided on a cursor's ``execute()`` call, the query results
    are written to cloud storage in the specified format rather than being returned inline
    over the WebSocket connection.

    :param format: The storage format (parquet, csv, geojson). Defaults to parquet.
    :param single: Whether to coalesce results into a single file.
    :param generate_presigned_url: Whether to generate a presigned download URL.
        Only valid when ``single=True``.
    :param options: Optional format-specific Spark DataFrameWriter options,
        e.g. ``{"ignoreNullFields": "false"}`` for GeoJSON or ``{"header": "false"}`` for CSV.
        These are applied after the server's default options and can override them.
    """

    format: StorageFormat = DEFAULT_STORAGE_FORMAT
    single: bool = False
    generate_presigned_url: bool = False
    options: Optional[Dict[str, str]] = field(default=None)

    def __post_init__(self):
        if self.generate_presigned_url and not self.single:
            raise ValueError("Can only generate a presigned URL when single=True")
        # Normalize empty options to None
        if self.options is not None and len(self.options) == 0:
            object.__setattr__(self, "options", None)
        # Defensive copy: make options immutable
        if self.options is not None:
            object.__setattr__(self, "options", dict(self.options))

    def to_dict(self) -> Dict:
        """Serialize to a dict suitable for the WebSocket protocol."""
        d = {
            "format": self.format.value,
            "single": self.single,
            "generate_presigned_url": self.generate_presigned_url,
        }
        if self.options is not None:
            d["options"] = dict(self.options)
        return d

    @staticmethod
    def for_download(
        format: StorageFormat = DEFAULT_STORAGE_FORMAT,
        options: Optional[Dict[str, str]] = None,
    ) -> "Store":
        """Create a Store configured for single-file download with a presigned URL.

        This is the most common configuration for programmatic result retrieval.

        :param format: The storage format. Defaults to parquet.
        :param options: Optional format-specific write options.
        """
        return Store(
            format=format,
            single=True,
            generate_presigned_url=True,
            options=options,
        )
