"""
Custom exception classes for pqlens
"""


class PqlensError(Exception):
    """Base exception class for all pqlens errors."""
    pass


class InvalidFileError(PqlensError):
    """Raised when a file is not a valid Parquet file or cannot be read."""
    pass


class PermissionError(PqlensError):
    """Raised when access to a file is denied due to permission issues."""
    pass
