from enum import StrEnum, auto

from .region import Region
from .runtime import Runtime


DEFAULT_ENDPOINT = "api.wherobots.services"  # "api.cloud.wherobots.com"
STAGING_ENDPOINT = "api.staging.wherobots.services"  # "api.staging.wherobots.com"
DEFAULT_RUNTIME = Runtime.SEDONA
DEFAULT_REGION = Region.AWS_US_WEST_2
DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS = 300


class ExecutionState(StrEnum):
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


class RequestKind(StrEnum):
    EXECUTE_SQL = auto()
    RETRIEVE_RESULTS = auto()


class EventKind(StrEnum):
    STATE_UPDATED = auto()
    EXECUTION_RESULT = auto()


class ResultsFormat(StrEnum):
    JSON = auto()
    ARROW = auto()


class DataCompression(StrEnum):
    BROTLI = auto()
