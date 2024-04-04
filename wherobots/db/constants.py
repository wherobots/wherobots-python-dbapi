from .region import Region
from .runtime import Runtime


DEFAULT_ENDPOINT = "api.wherobots.services"  # "api.cloud.wherobots.com"
STAGING_ENDPOINT = "api.staging.wherobots.services"  # "api.staging.wherobots.com"
DEFAULT_RUNTIME = Runtime.SEDONA
DEFAULT_REGION = Region.AWS_US_WEST_2
DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS = 300
