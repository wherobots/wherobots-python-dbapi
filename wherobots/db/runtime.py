from enum import Enum


class Runtime(Enum):
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
    TINY_A10_GPU = "tiny-a10-gpu"
    SMALL_A10_GPU = "small-a10-gpu"
    MEDIUM_A10_GPU = "medium-a10-gpu"

    # Deprecated names; will be removed in a later major version.
    SEDONA = "tiny"
    SAN_FRANCISCO = "small"
    NEW_YORK = "medium"
    CAIRO = "large"
    DELHI = "x-large"
    TOKYO = "2x-large"
    ATLANTIS = "4x-large"

    NEW_YORK_HIMEM = "medium-himem"
    CAIRO_HIMEM = "large-himem"
    DELHI_HIMEM = "x-large-himem"
    TOKYO_HIMEM = "2x-large-himem"
    ATLANTIS_HIMEM = "4x-large-himem"

    SEDONA_GPU = "tiny-a10-gpu"
    SAN_FRANCISCO_GPU = "small-a10-gpu"
    NEW_YORK_GPU = "medium-a10-gpu"
