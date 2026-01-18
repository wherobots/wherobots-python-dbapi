from dataclasses import dataclass

import pandas

from .store import StoreResult


@dataclass
class ExecutionResult:
    """Result of a query execution.

    This class encapsulates all possible outcomes of a query execution:
    a DataFrame result, an error, or a store result (when results are
    written to cloud storage).

    Attributes:
        results: The query results as a pandas DataFrame, or None if an error occurred.
        error: The error that occurred during execution, or None if successful.
        store_result: The store result if results were written to cloud storage.
    """

    results: pandas.DataFrame | None = None
    error: Exception | None = None
    store_result: StoreResult | None = None
