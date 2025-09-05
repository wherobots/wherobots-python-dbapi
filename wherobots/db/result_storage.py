from dataclasses import dataclass
from enum import auto
from strenum import LowercaseStrEnum
from typing import Union


class StorageFormat(LowercaseStrEnum):
    PARQUET = auto()
    CSV = auto()
    GEOJSON = auto()


@dataclass
class Store:
    format: Union[StorageFormat, None] = None
    single: bool = False
    generate_presigned_url: bool = False

    def __post_init__(self) -> None:
        assert (
            self.single or not self.generate_presigned_url
        ), "Presigned URL can only be generated when single part file is requested."
