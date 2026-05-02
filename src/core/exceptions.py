class AppError(Exception):
    """Base exception for the application."""

class IngestionError(AppError):
    """Raised when there is an error fetching data from the API."""

class ProcessingError(AppError):
    """Raised when there is an error processing the data."""

class StorageError(AppError):
    """Raised when there is an error storing or retrieving the data."""
