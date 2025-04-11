from enum import Enum


class Region(Enum):
    AWS_US_WEST_2 = "aws-us-west-2"
    AWS_EU_WEST_1 = "aws-eu-west-1"


_NAME_TO_ENUM = {region.value: region for region in Region}


def from_name(region: str) -> Region:
    return _NAME_TO_ENUM[region]
