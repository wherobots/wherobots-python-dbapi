from packaging.version import Version

from .region import Region
from .runtime import Runtime
from .session_type import SessionType
from .types import StorageFormat


DEFAULT_ENDPOINT: str = "api.cloud.wherobots.com"  # "api.cloud.wherobots.com"
STAGING_ENDPOINT: str = "api.staging.wherobots.com"  # "api.staging.wherobots.com"

DEFAULT_RUNTIME: Runtime = Runtime.TINY
DEFAULT_REGION: Region = Region.AWS_US_WEST_2
DEFAULT_SESSION_TYPE: SessionType = SessionType.MULTI
DEFAULT_STORAGE_FORMAT: StorageFormat = StorageFormat.PARQUET
DEFAULT_READ_TIMEOUT_SECONDS: float = 0.25
DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS: float = 900

MAX_MESSAGE_SIZE: int = 100 * 2**20  # 100MiB
PROTOCOL_VERSION: Version = Version("1.0.0")

PARAM_STYLE = "pyformat"
