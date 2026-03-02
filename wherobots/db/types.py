from enum import auto
from strenum import LowercaseStrEnum, StrEnum


class ExecutionState(LowercaseStrEnum):
    IDLE = auto()
    "Not executing any operation."

    EXECUTION_REQUESTED = auto()
    "Execution of a query has been requested by the driver."

    RUNNING = auto()
    "The SQL session has reported the query is running."

    SUCCEEDED = auto()
    "The SQL session has reported the query has completed successfully."

    CANCELLED = auto()
    "The SQL session has reported the query has been cancelled."

    FAILED = auto()
    "The SQL session has reported the query has failed."

    RESULTS_REQUESTED = auto()
    "The driver has requested the query results from the SQL session."

    COMPLETED = auto()
    "The driver has completed processing the query results."

    def is_terminal_state(self) -> bool:
        return self in (
            ExecutionState.COMPLETED,
            ExecutionState.CANCELLED,
            ExecutionState.FAILED,
        )


class RequestKind(LowercaseStrEnum):
    EXECUTE_SQL = auto()
    RETRIEVE_RESULTS = auto()
    CANCEL = auto()


class EventKind(LowercaseStrEnum):
    STATE_UPDATED = auto()
    EXECUTION_RESULT = auto()
    ERROR = auto()
    EXECUTION_PROGRESS = auto()


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


class StorageFormat(LowercaseStrEnum):
    PARQUET = auto()
    CSV = auto()
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

    def is_starting(self) -> bool:
        return self in (
            AppStatus.PENDING,
            AppStatus.PREPARING,
            AppStatus.REQUESTED,
            AppStatus.DEPLOYING,
            AppStatus.DEPLOYED,
            AppStatus.INITIALIZING,
        )

    def is_terminal_state(self) -> bool:
        return self in (
            AppStatus.PREPARE_FAILED,
            AppStatus.DEPLOY_FAILED,
            AppStatus.INIT_FAILED,
            AppStatus.DESTROY_FAILED,
            AppStatus.DESTROYED,
        )
