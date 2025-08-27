#!/usr/bin/env python3
"""
Tests for error handling and validation in pqlens.parquet_viewer module
"""

import os
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path

import pandas as pd

from pqlens.parquet_viewer import view_parquet_file, display_table, paged_display


class TestErrorHandling(unittest.TestCase):
    """Test cases for error handling and validation."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_data_dir = Path(__file__).parent / "data"
        self.simple_file = self.test_data_dir / "simple.parquet"

    def capture_output(self, func, *args, **kwargs):
        """Helper to capture stdout from functions."""
        captured_output = StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured_output

        try:
            result = func(*args, **kwargs)
            output = captured_output.getvalue()
            return result, output
        finally:
            sys.stdout = old_stdout

    def test_view_parquet_file_empty_path(self):
        """Test view_parquet_file with empty path."""
        result, output = self.capture_output(view_parquet_file, "")

        self.assertIsNone(result)
        self.assertIn("Error: No file path provided", output)

    def test_view_parquet_file_none_path(self):
        """Test view_parquet_file with None path."""
        result, output = self.capture_output(view_parquet_file, None)

        self.assertIsNone(result)
        self.assertIn("Error: No file path provided", output)

    def test_view_parquet_file_invalid_type(self):
        """Test view_parquet_file with invalid path type."""
        result, output = self.capture_output(view_parquet_file, 123)

        self.assertIsNone(result)
        self.assertIn("Error: Invalid file path type", output)
        self.assertIn("Expected string or path-like object", output)

    def test_view_parquet_file_nonexistent(self):
        """Test view_parquet_file with non-existent file."""
        result, output = self.capture_output(view_parquet_file, "nonexistent_file.parquet")

        self.assertIsNone(result)
        self.assertIn("Error: File not found", output)
        self.assertIn("nonexistent_file.parquet", output)

    def test_view_parquet_file_directory(self):
        """Test view_parquet_file with directory path - pandas handles this."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result, output = self.capture_output(view_parquet_file, temp_dir)

            # pandas may treat empty directory as empty dataset or return error
            # Either behavior is acceptable - we just shouldn't crash
            self.assertTrue(result is None or isinstance(result, pd.DataFrame))

    def test_view_parquet_file_empty_file(self):
        """Test view_parquet_file with empty file - let pandas handle it."""
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_path = temp_file.name

        try:
            # File is created but empty (0 bytes) - pandas will handle this
            result, output = self.capture_output(view_parquet_file, temp_path)

            # pandas should return None or raise an error - either is fine
            # We just want to ensure we handle it gracefully without crashing
            self.assertTrue(result is None or isinstance(result, pd.DataFrame))
        finally:
            os.unlink(temp_path)

    def test_view_parquet_file_wrong_extension_warning(self):
        """Test view_parquet_file with wrong extension shows warning."""
        # Create a real parquet file with wrong extension
        with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as temp_file:
            temp_path = temp_file.name

        # Write actual parquet data to it
        df = pd.DataFrame({'a': [1, 2, 3]})
        df.to_parquet(temp_path, index=False)

        try:
            result, output = self.capture_output(view_parquet_file, temp_path)

            # Should still work but show warning
            self.assertIsNotNone(result)
            self.assertIn("Warning:", output)
            self.assertIn("does not have a .parquet extension", output)
        finally:
            os.unlink(temp_path)

    def test_view_parquet_file_corrupted_file(self):
        """Test view_parquet_file with corrupted file."""
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            # Write invalid data to simulate corruption
            temp_file.write(b"This is not parquet data, just some text")
            temp_path = temp_file.name

        try:
            result, output = self.capture_output(view_parquet_file, temp_path)

            self.assertIsNone(result)
            self.assertIn("Error:", output)
            # Should identify it as not a valid parquet file
            self.assertTrue(
                "not a valid Parquet file" in output or
                "Invalid Parquet file format" in output or
                "Invalid data in Parquet file" in output
            )
        finally:
            os.unlink(temp_path)

    def test_view_parquet_file_zero_columns(self):
        """Test view_parquet_file with zero columns."""
        # Create a DataFrame with no columns but has rows
        df_empty_cols = pd.DataFrame(index=range(3))  # 3 rows, 0 columns

        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_path = temp_file.name

        try:
            df_empty_cols.to_parquet(temp_path)
            result, output = self.capture_output(view_parquet_file, temp_path)

            # Should succeed but show warning
            self.assertIsNotNone(result)
            self.assertEqual(len(result.columns), 0)
            self.assertIn("Warning:", output)
            self.assertIn("has no columns", output)
        finally:
            os.unlink(temp_path)

    def test_display_table_invalid_rows_parameter(self):
        """Test display_table with invalid rows parameter."""
        df = pd.DataFrame({'a': [1, 2, 3]})

        # Test negative rows
        result, output = self.capture_output(display_table, df, -5)
        self.assertIn("Warning: Invalid rows parameter", output)
        self.assertIn("using default of 10", output)

        # Test non-integer rows
        result, output = self.capture_output(display_table, df, "not_a_number")
        self.assertIn("Warning: Invalid rows parameter", output)
        self.assertIn("using default of 10", output)

    def test_display_table_zero_columns(self):
        """Test display_table with DataFrame having zero columns."""
        df_empty_cols = pd.DataFrame(index=range(3))  # 3 rows, 0 columns

        result, output = self.capture_output(display_table, df_empty_cols, 10)

        self.assertIn("Parquet file shape: (3, 0)", output)
        self.assertIn("This Parquet file has no columns", output)
        self.assertIn("Number of rows: 3", output)

    def test_display_table_zero_rows(self):
        """Test display_table with DataFrame having zero rows."""
        df_empty_rows = pd.DataFrame({'col1': [], 'col2': []})

        result, output = self.capture_output(display_table, df_empty_rows, 10)

        self.assertIn("Parquet file shape: (0, 2)", output)
        self.assertIn("File structure (no data rows)", output)

    def test_display_table_tabulate_error_fallback(self):
        """Test display_table fallback when tabulate fails."""
        # Create a DataFrame that might cause tabulate issues
        df = pd.DataFrame({'col': [None, float('inf'), -float('inf')]})

        # This should not crash even if tabulate has issues
        result, output = self.capture_output(display_table, df, 10)

        # Should contain shape info regardless
        self.assertIn("Parquet file shape:", output)

    def test_paged_display_none_dataframe(self):
        """Test paged_display with None DataFrame."""
        result, output = self.capture_output(paged_display, None)

        self.assertIn("No data to display - DataFrame is None", output)

    def test_paged_display_invalid_page_size(self):
        """Test paged_display with invalid page_size - only tests validation, not interactive mode."""
        # Use an empty DataFrame to avoid entering interactive mode
        df = pd.DataFrame({'a': []})  # Empty but has columns

        # Test negative page size
        result, output = self.capture_output(paged_display, df, -5)
        self.assertIn("Warning: Invalid page_size", output)
        self.assertIn("using default of 10", output)

        # Test zero page size
        result, output = self.capture_output(paged_display, df, 0)
        self.assertIn("Warning: Invalid page_size", output)

    def test_paged_display_zero_columns(self):
        """Test paged_display with DataFrame having zero columns."""
        df_empty_cols = pd.DataFrame(index=range(3))  # 3 rows, 0 columns

        result, output = self.capture_output(paged_display, df_empty_cols)

        self.assertIn("This Parquet file has no columns", output)
        self.assertIn("Number of rows: 3", output)
        self.assertIn("Nothing to display in interactive mode", output)

    def test_paged_display_empty_dataframe(self):
        """Test paged_display with empty DataFrame (has columns but no rows)."""
        df_empty = pd.DataFrame({'col1': [], 'col2': []})

        result, output = self.capture_output(paged_display, df_empty)

        self.assertIn("DataFrame has columns but no data rows", output)
        self.assertIn("Columns (2): ['col1', 'col2']", output)
        self.assertIn("Nothing to navigate in interactive mode", output)

    def test_pathlib_path_support(self):
        """Test that pathlib.Path objects are supported."""
        path_obj = Path(self.simple_file)
        result = view_parquet_file(path_obj)

        # Should work with pathlib.Path objects
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
