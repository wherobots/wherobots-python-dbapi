from enum import Enum

from .region import Region
from .runtime import Runtime


DEFAULT_ENDPOINT = "api.wherobots.services"  # "api.cloud.wherobots.com"
STAGING_ENDPOINT = "api.staging.wherobots.services"  # "api.staging.wherobots.com"
DEFAULT_RUNTIME = Runtime.SEDONA
DEFAULT_REGION = Region.AWS_US_WEST_2
DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS = 300


class ExecutionState(Enum):
    IDLE = "idle"
    "Not executing any operation."

    EXECUTION_REQUESTED = "requested"
    "Execution of a query has been requested by the driver."

    RUNNING = "running"
    "The SQL session has reported the query is running."

    SUCCEEDED = "succeeded"
    "The SQL session has reported the query has completed successfully."

    FAILED = "failed"
    "The SQL session has reported the query has failed."

    RESULTS_REQUESTED = "results_requested"
    "The driver has requested the query results from the SQL session."

    COMPLETED = "completed"
    "The driver has completed processing the query results."

    def is_terminal_state(self):
        return self in (ExecutionState.COMPLETED, ExecutionState.FAILED)


class RequestKind(Enum):
    EXECUTE_SQL = "execute_sql"
    RETRIEVE_RESULTS = "retrieve_results"


class EventKind(Enum):
    STATE_UPDATED = "state_updated"
    EXECUTION_RESULT = "execution_result"
