"""
Input validation utilities for pqlens
"""

import os
from pathlib import Path
from typing import Union, Optional


def validate_rows_parameter(rows: Union[int, str]) -> int:
    """
    Validate the rows parameter and return a valid integer value.
    
    Args:
        rows: The rows parameter to validate
        
    Returns:
        int: A valid positive integer for number of rows
        
    Raises:
        ValueError: If rows cannot be converted to a positive integer
    """
    if not isinstance(rows, int):
        try:
            rows = int(rows)
        except (ValueError, TypeError):
            raise ValueError(f"Invalid rows parameter '{rows}', must be a positive integer")

    if rows < 0:
        raise ValueError(f"Invalid rows parameter '{rows}', must be non-negative")

    return rows


def validate_path_parameter(path: Union[str, os.PathLike, None]) -> Optional[Path]:
    """
    Validate and normalize a file path parameter.
    
    Args:
        path: The path to validate and normalize
        
    Returns:
        Path: A Path object if valid, None if empty
        
    Raises:
        ValueError: If path is invalid type
    """
    if not path:
        return None

    if not isinstance(path, (str, os.PathLike)):
        raise ValueError(f"Invalid file path type. Expected string or path-like object, got {type(path).__name__}")

    return Path(path)
