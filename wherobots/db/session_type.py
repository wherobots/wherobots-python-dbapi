from enum import auto
from strenum import LowercaseStrEnum


class SessionType(LowercaseStrEnum):
    SINGLE = auto()
    MULTI = auto()
