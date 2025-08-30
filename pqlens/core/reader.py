"""
Parquet file reading and validation
"""

import os
from typing import Optional

import pandas as pd
import psutil
import pyarrow.parquet as pq

from ..utils.validation import validate_path_parameter


class ParquetReader:
    """Handles reading and validating Parquet files with memory-efficient lazy loading."""

    def __init__(self, memory_threshold_mb: int = 100, enable_lazy_loading: bool = True):
        """
        Initialize ParquetReader with memory optimization settings.
        
        Args:
            memory_threshold_mb: File size threshold in MB above which to use lazy loading
            enable_lazy_loading: Whether to enable lazy loading for large files
        """
        self.memory_threshold_mb = memory_threshold_mb
        self.enable_lazy_loading = enable_lazy_loading
        self._parquet_file = None
        self._file_info = None

    def read_file(self, file_path: str, columns: Optional[list] = None,
                  row_range: Optional[tuple] = None) -> Optional[pd.DataFrame]:
        """
        Reads a Parquet file and returns its content as a DataFrame.
        
        Performs comprehensive validation and provides specific error messages
        for different failure scenarios. Uses lazy loading for large files.

        Args:
            file_path: Path to the Parquet file.
            columns: Optional list of columns to read (for memory efficiency)
            row_range: Optional tuple (start_row, end_row) for partial reading
            
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

        # Check if we should use lazy loading based on file size
        use_lazy_loading = self._should_use_lazy_loading(file_size)

        # Always try to get file metadata for potential lazy loading operations
        try:
            # Get file metadata first (for file_info)
            try:
                parquet_file = pq.ParquetFile(file_path)
                self._parquet_file = parquet_file
                self._file_info = {
                    'num_rows': parquet_file.metadata.num_rows,
                    'num_columns': len(parquet_file.schema_arrow),
                    'schema': parquet_file.schema_arrow
                }
            except Exception:
                # If we can't get metadata, file_info will remain None
                pass

            # Attempt to read the Parquet file with specific error handling
            if use_lazy_loading:
                df = self._read_with_lazy_loading(file_path, columns, row_range)
            else:
                df = self._read_traditional(file_path, columns, row_range)

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
            available_memory = self.get_available_memory_mb()
            print(f"Available memory: {available_memory:.1f} MB")
            if not use_lazy_loading:
                print("Tip: Try enabling lazy loading or reading only specific columns/rows.")
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

    def _should_use_lazy_loading(self, file_size_bytes: int) -> bool:
        """
        Determine if lazy loading should be used based on file size and available memory.
        
        Args:
            file_size_bytes: Size of the file in bytes
            
        Returns:
            bool: True if lazy loading should be used
        """
        if not self.enable_lazy_loading:
            return False

        file_size_mb = file_size_bytes / (1024 * 1024)

        # Use lazy loading if file is above threshold
        if file_size_mb > self.memory_threshold_mb:
            return True

        # Also check available memory - use lazy loading if file is >50% of available RAM
        available_memory_mb = self.get_available_memory_mb()
        if available_memory_mb > 0 and file_size_mb > (available_memory_mb * 0.5):
            return True

        return False

    def _read_traditional(self, file_path: str, columns: Optional[list] = None,
                          row_range: Optional[tuple] = None) -> Optional[pd.DataFrame]:
        """
        Traditional pandas read_parquet method.
        
        Args:
            file_path: Path to the Parquet file
            columns: Optional list of columns to read
            row_range: Optional tuple (start_row, end_row) for partial reading
            
        Returns:
            DataFrame or None if error
        """
        read_kwargs = {}
        if columns:
            read_kwargs['columns'] = columns

        df = pd.read_parquet(file_path, **read_kwargs)

        if row_range and df is not None:
            start_row, end_row = row_range
            df = df.iloc[start_row:end_row]

        return df

    def _read_with_lazy_loading(self, file_path: str, columns: Optional[list] = None,
                                row_range: Optional[tuple] = None) -> Optional[pd.DataFrame]:
        """
        Memory-efficient reading using pyarrow with lazy loading.
        
        Args:
            file_path: Path to the Parquet file
            columns: Optional list of columns to read
            row_range: Optional tuple (start_row, end_row) for partial reading
            
        Returns:
            DataFrame or None if error
        """
        # Use the already-loaded parquet file metadata
        parquet_file = self._parquet_file
        if parquet_file is None:
            # Fallback if metadata wasn't loaded earlier
            parquet_file = pq.ParquetFile(file_path)
            self._parquet_file = parquet_file

        # Print memory-efficient loading message
        print(f"Using memory-efficient loading for large file ({parquet_file.metadata.num_rows:,} rows, {len(parquet_file.schema_arrow)} columns)")

        # Build read arguments
        read_kwargs = {}
        if columns:
            read_kwargs['columns'] = columns

        # For row range, use pyarrow's ability to read specific row groups more efficiently
        if row_range:
            start_row, end_row = row_range
            # Calculate which row groups we need
            total_rows = 0
            start_row_group = 0
            end_row_group = parquet_file.num_row_groups

            for i in range(parquet_file.num_row_groups):
                row_group_size = parquet_file.metadata.row_group(i).num_rows
                if total_rows + row_group_size > start_row and start_row_group == 0:
                    start_row_group = i
                if total_rows + row_group_size >= end_row:
                    end_row_group = i + 1
                    break
                total_rows += row_group_size

            # Read only the necessary row groups
            table = parquet_file.read_row_groups(
                list(range(start_row_group, end_row_group)),
                **read_kwargs
            )
            df = table.to_pandas()

            # Fine-tune the row selection within the loaded row groups
            actual_start = start_row - sum(
                parquet_file.metadata.row_group(i).num_rows
                for i in range(start_row_group)
            )
            actual_end = actual_start + (end_row - start_row)
            df = df.iloc[actual_start:actual_end]
        else:
            # Read the entire file but potentially with column selection
            table = parquet_file.read(**read_kwargs)
            df = table.to_pandas()

        return df

    def get_file_info(self) -> Optional[dict]:
        """
        Get metadata about the currently loaded file (if using lazy loading).
        
        Returns:
            dict: File information including row count, columns, schema
        """
        return self._file_info

    def get_available_memory_mb(self) -> float:
        """
        Get available system memory in MB.
        
        Returns:
            float: Available memory in MB, or -1 if unable to determine
        """
        try:
            memory = psutil.virtual_memory()
            return memory.available / (1024 * 1024)
        except Exception:
            return -1

    def get_memory_usage_mb(self) -> float:
        """
        Get current process memory usage in MB.
        
        Returns:
            float: Memory usage in MB, or -1 if unable to determine
        """
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            return memory_info.rss / (1024 * 1024)
        except Exception:
            return -1
