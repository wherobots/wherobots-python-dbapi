from enum import Enum


class Runtime(Enum):
    # CPU/General purpose runtimes
    MICRO = "micro"
    TINY = "tiny"
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    X_LARGE = "x-large"
    XX_LARGE = "2x-large"
    XXXX_LARGE = "4x-large"

    # HIMEM/Memory-optimized runtimes
    MEDIUM_HIMEM = "medium-himem"
    LARGE_HIMEM = "large-himem"
    X_LARGE_HIMEM = "x-large-himem"
    XX_LARGE_HIMEM = "2x-large-himem"
    XXXX_LARGE_HIMEM = "4x-large-himem"

    # HICPU/Compute-optimized runtimes
    X_LARGE_HICPU = "x-large-hicpu"
    XX_LARGE_HICPU = "2x-large-hicpu"

    # GPU-accelerated runtimes
    MICRO_A10_GPU = "micro-a10-gpu"
    TINY_A10_GPU = "tiny-a10-gpu"
    SMALL_A10_GPU = "small-a10-gpu"
    MEDIUM_A10_GPU = "medium-a10-gpu"
    LARGE_A10_GPU = "large-a10-gpu"
    X_LARGE_A10_GPU = "x-large-a10-gpu"


_NAME_TO_ENUM = {runtime.value: runtime for runtime in Runtime}


def from_name(runtime: str) -> Runtime:
    return _NAME_TO_ENUM[runtime]
