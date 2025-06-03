from enum import auto
from strenum import LowercaseStrEnum
from typing import Union


class StorageFormat(LowercaseStrEnum):
    PARQUET = auto()
    CSV = auto()
    GEOJSON = auto()


class Store:
    def __init__(
        self,
        format: Union[StorageFormat, None] = None,
        single: bool = False,
        generate_presigned_url: bool = False,
    ):
        self.format = format
        self.single = single
        self.generate_presigned_url = generate_presigned_url
        assert (
            single or not generate_presigned_url
        ), "Presigned URL can only be generated when single part file is requested."

    def __repr__(self):
        return f"Store(format={self.format}, single={self.single}, generate_presigned_url={self.generate_presigned_url})"

    def __str__(self):
        return f"Store(format={self.format}, single={self.single}, generate_presigned_url={self.generate_presigned_url})"
