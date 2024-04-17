from enum import auto
from strenum import LowercaseStrEnum

from .region import Region
from .runtime import Runtime


DEFAULT_ENDPOINT: str = "api.wherobots.services"  # "api.cloud.wherobots.com"
STAGING_ENDPOINT: str = "api.staging.wherobots.services"  # "api.staging.wherobots.com"
DEFAULT_RUNTIME: Runtime = Runtime.SEDONA
DEFAULT_REGION: Region = Region.AWS_US_WEST_2
DEFAULT_READ_TIMEOUT_SECONDS: float = 0.25
DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS: float = 300
MAX_MESSAGE_SIZE: int = 100 * 2**20  # 100MiB


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
