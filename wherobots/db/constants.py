from enum import auto
from strenum import LowercaseStrEnum, StrEnum

from .region import Region
from .runtime import Runtime


DEFAULT_ENDPOINT: str = "api.cloud.wherobots.com"  # "api.cloud.wherobots.com"
STAGING_ENDPOINT: str = "api.staging.wherobots.com"  # "api.staging.wherobots.com"

DEFAULT_RUNTIME: Runtime = Runtime.TINY
DEFAULT_REGION: Region = Region.AWS_US_WEST_2
DEFAULT_READ_TIMEOUT_SECONDS: float = 0.25
DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS: float = 900
DEFAULT_REUSE_SESSION: bool = True

MAX_MESSAGE_SIZE: int = 100 * 2**20  # 100MiB
PROTOCOL_VERSION: str = "1.0.0"


class ExecutionState(LowercaseStrEnum):
    IDLE = auto()
    "Not executing any operation."

    EXECUTION_REQUESTED = auto()
    "Execution of a query has been requested by the driver."

    RUNNING = auto()
    "The SQL session has reported the query is running."

    SUCCEEDED = auto()
    "The SQL session has reported the query has completed successfully."

    FAILED = auto()
    "The SQL session has reported the query has failed."

    RESULTS_REQUESTED = auto()
    "The driver has requested the query results from the SQL session."

    COMPLETED = auto()
    "The driver has completed processing the query results."

    def is_terminal_state(self):
        return self in (ExecutionState.COMPLETED, ExecutionState.FAILED)


class RequestKind(LowercaseStrEnum):
    EXECUTE_SQL = auto()
    RETRIEVE_RESULTS = auto()


class EventKind(LowercaseStrEnum):
    STATE_UPDATED = auto()
    EXECUTION_RESULT = auto()
    ERROR = auto()


class ResultsFormat(LowercaseStrEnum):
    JSON = auto()
    ARROW = auto()


class DataCompression(LowercaseStrEnum):
    BROTLI = auto()


class GeometryRepresentation(LowercaseStrEnum):
    WKT = auto()
    WKB = auto()
    EWKT = auto()
    EWKB = auto()
    GEOJSON = auto()


class AppStatus(StrEnum):
    PENDING = auto()
    PREPARING = auto()
    PREPARE_FAILED = auto()
    REQUESTED = auto()
    DEPLOYING = auto()
    DEPLOY_FAILED = auto()
    DEPLOYED = auto()
    INITIALIZING = auto()
    INIT_FAILED = auto()
    READY = auto()
    DESTROY_REQUESTED = auto()
    DESTROYING = auto()
    DESTROY_FAILED = auto()
    DESTROYED = auto()

    def is_starting(self):
        return self in (
            AppStatus.PENDING,
            AppStatus.PREPARING,
            AppStatus.REQUESTED,
            AppStatus.DEPLOYING,
            AppStatus.DEPLOYED,
            AppStatus.INITIALIZING,
        )

    def is_terminal_state(self):
        return self in (
            AppStatus.PREPARE_FAILED,
            AppStatus.DEPLOY_FAILED,
            AppStatus.INIT_FAILED,
            AppStatus.DESTROY_FAILED,
            AppStatus.DESTROYED,
        )
