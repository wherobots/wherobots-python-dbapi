from enum import Enum


class Region(Enum):
    # Americas
    AWS_US_EAST_1 = "aws-us-east-1"
    AWS_US_WEST_2 = "aws-us-west-2"

    # EMEA
    AWS_EU_WEST_1 = "aws-eu-west-1"

    # APAC
    AWS_AP_SOUTH_1 = "aws-ap-south-1"


_NAME_TO_ENUM = {region.value: region for region in Region}


def from_name(region: str) -> Region:
    return _NAME_TO_ENUM[region]
