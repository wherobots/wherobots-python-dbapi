from enum import Enum


class Runtime(Enum):
    SEDONA = "TINY"
    NEW_YORK = "MEDIUM"
    CAIRO = "LARGE"
    DELHI = "XLARGE"
    TOKYO = "XXLARGE"

    NEW_YORK_HIMEM = "medium-himem"
    CAIRO_HIMEM = "large-himem"
    DELHI_HIMEM = "x-large-himem"
    TOKYO_HIMEM = "2x-large-himem"
