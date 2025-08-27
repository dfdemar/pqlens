"""
Parquet file reading and validation
"""

import os
from typing import Optional

import pandas as pd

from ..utils.validation import validate_path_parameter


class ParquetReader:
    """Handles reading and validating Parquet files."""

    def read_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Reads a Parquet file and returns its content as a DataFrame.
        
        Performs comprehensive validation and provides specific error messages
        for different failure scenarios.

        Args:
            file_path: Path to the Parquet file.
            
        Returns:
            DataFrame containing the data from the Parquet file, or None if error.
        """
        # Input validation
        if not file_path:
            print("Error: No file path provided")
            return None

        try:
            path = validate_path_parameter(file_path)
            if path is None:
                print("Error: Empty file path provided")
                return None
        except ValueError as e:
            print(f"Error: {e}")
            return None

        # Convert to string for consistent handling
        file_path = str(path)

        # File existence validation
        if not path.exists():
            print(f"Error: File not found - '{file_path}'")
            print("Please check the file path and try again.")
            return None

        # Check file permissions
        if not os.access(file_path, os.R_OK):
            print(f"Error: Permission denied - cannot read file '{file_path}'")
            print("Please check file permissions and try again.")
            return None

        # Get file size for memory error reporting
        try:
            file_size = path.stat().st_size
        except OSError:
            file_size = 0

        # Check file extension (warning, not error)
        if not file_path.lower().endswith(('.parquet', '.pqt')):
            print(f"Warning: File '{file_path}' does not have a .parquet extension")
            print("Attempting to read as Parquet format anyway...")

        # Attempt to read the Parquet file with specific error handling
        try:
            df = pd.read_parquet(file_path)

            # Post-read validation
            if df is None:
                print(f"Error: Failed to read Parquet file '{file_path}' - result is None")
                return None

            # Check for zero columns (valid but unusual case)
            if len(df.columns) == 0:
                print(f"Warning: Parquet file '{file_path}' has no columns")
                print("This is a valid but unusual Parquet file structure.")

            return df

        except ImportError as e:
            print(f"Error: Missing required library - {e}")
            print("Please install required packages: pip install pandas pyarrow")
            return None

        except pd.errors.ParserError as e:
            print(f"Error: Invalid Parquet file format - '{file_path}'")
            print(f"Parser error: {e}")
            print("The file may be corrupted or not a valid Parquet file.")
            return None

        except PermissionError as e:
            print(f"Error: Permission denied accessing file '{file_path}'")
            print(f"System error: {e}")
            return None

        except FileNotFoundError as e:
            # This shouldn't happen due to our pre-checks, but handle it anyway
            print(f"Error: File not found during read operation - '{file_path}'")
            print(f"System error: {e}")
            return None

        except MemoryError as e:
            print(f"Error: Insufficient memory to load file '{file_path}'")
            print(f"The file may be too large for available memory.")
            print(f"File size: {file_size / (1024 * 1024):.1f} MB")
            print("Try using a machine with more RAM or processing the file in chunks.")
            return None

        except ValueError as e:
            error_msg = str(e).lower()
            if 'not a parquet file' in error_msg or 'parquet magic bytes' in error_msg:
                print(f"Error: '{file_path}' is not a valid Parquet file")
                print(f"The file may be corrupted, empty, or in a different format.")
            elif 'schema' in error_msg:
                print(f"Error: Invalid Parquet schema in file '{file_path}'")
                print(f"Schema error: {e}")
            else:
                print(f"Error: Invalid data in Parquet file '{file_path}'")
                print(f"Value error: {e}")
            return None

        except OSError as e:
            error_msg = str(e).lower()
            if 'no such file' in error_msg:
                print(f"Error: File disappeared during read operation - '{file_path}'")
            elif 'permission denied' in error_msg:
                print(f"Error: Permission denied accessing '{file_path}'")
            else:
                print(f"Error: System error accessing file '{file_path}'")
                print(f"OS error: {e}")
            return None

        except Exception as e:
            # Catch-all for unexpected errors
            print(f"Error: Unexpected error reading Parquet file '{file_path}'")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {e}")
            print("This may indicate a bug in the software or an unusual file format.")
            return None

    def validate_path(self, file_path: str) -> bool:
        """
        Validate that a file path exists and is readable.
        
        Args:
            file_path: Path to validate
            
        Returns:
            bool: True if valid and readable
        """
        try:
            path = validate_path_parameter(file_path)
            if path is None:
                return False
            return path.exists() and os.access(str(path), os.R_OK)
        except (ValueError, OSError):
            return False
