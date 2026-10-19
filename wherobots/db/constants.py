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
# Bound on the WebSocket close handshake performed by a graceful close().
DEFAULT_CLOSE_TIMEOUT_SECONDS: float = 1.0
DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS: float = 900
# Silence, per query, before the driver re-asks the session for its state.
DEFAULT_STALE_QUERY_PROBE_SECONDS: float = 30.0
# How long the driver keeps trying to reconnect to the SQL session after losing
# the connection. It must fit, along with the up to ~40s that keepalive takes
# to notice a dead connection, in the session's grace period for orphaned
# executions (120s by default), after which the session cancels them.
DEFAULT_RECONNECT_TIMEOUT_SECONDS: float = 60.0

MAX_MESSAGE_SIZE: int = 100 * 2**20  # 100MiB
PROTOCOL_VERSION: Version = Version("1.0.0")

PARAM_STYLE = "pyformat"
