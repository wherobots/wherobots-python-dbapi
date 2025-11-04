from enum import Enum


class Runtime(Enum):
    MICRO = "micro"
    TINY = "tiny"
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    X_LARGE = "x-large"
    XX_LARGE = "2x-large"
    XXXX_LARGE = "4x-large"

    # HIMEM
    MEDIUM_HIMEM = "medium-himem"
    LARGE_HIMEM = "large-himem"
    X_LARGE_HIMEM = "x-large-himem"
    XX_LARGE_HIMEM = "2x-large-himem"
    XXXX_LARGE_HIMEM = "4x-large-himem"

    # GPU
    MICRO_A10_GPU = "micro-a10-gpu"
    TINY_A10_GPU = "tiny-a10-gpu"
    SMALL_A10_GPU = "small-a10-gpu"
    MEDIUM_A10_GPU = "medium-a10-gpu"


_NAME_TO_ENUM = {runtime.value: runtime for runtime in Runtime}


def from_name(runtime: str) -> Runtime:
    return _NAME_TO_ENUM[runtime]
