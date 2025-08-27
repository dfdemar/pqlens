#!/usr/bin/env python3
"""
Unit tests for pqlens.parquet_viewer module
"""

import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path

import pandas as pd

from pqlens.main import view_parquet_file, display_table


class TestParquetViewer(unittest.TestCase):
    """Test cases for parquet viewer functions."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_data_dir = Path(__file__).parent / "data"
        self.simple_file = self.test_data_dir / "simple.parquet"
        self.empty_file = self.test_data_dir / "empty.parquet"
        self.large_file = self.test_data_dir / "large.parquet"
        self.mixed_types_file = self.test_data_dir / "mixed_types.parquet"

    def test_view_parquet_file_simple(self):
        """Test reading a simple parquet file."""
        df = view_parquet_file(str(self.simple_file))

        self.assertIsNotNone(df)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 3)
        self.assertEqual(list(df.columns), ['id', 'name', 'value'])

        # Check specific values
        self.assertEqual(df.iloc[0]['id'], 1)
        self.assertEqual(df.iloc[0]['name'], 'Alice')
        self.assertEqual(df.iloc[0]['value'], 10.5)

    def test_view_parquet_file_empty(self):
        """Test reading an empty parquet file."""
        df = view_parquet_file(str(self.empty_file))

        self.assertIsNotNone(df)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)
        self.assertEqual(list(df.columns), ['col1', 'col2'])

    def test_view_parquet_file_large(self):
        """Test reading a large parquet file."""
        df = view_parquet_file(str(self.large_file))

        self.assertIsNotNone(df)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 100)
        self.assertEqual(list(df.columns), ['id', 'category', 'value', 'timestamp'])

    def test_view_parquet_file_mixed_types(self):
        """Test reading a parquet file with mixed data types."""
        df = view_parquet_file(str(self.mixed_types_file))

        self.assertIsNotNone(df)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 5)
        self.assertEqual(list(df.columns), ['int_col', 'float_col', 'str_col', 'bool_col', 'date_col'])

        # Check data types are preserved with nullable dtypes
        self.assertTrue(pd.api.types.is_integer_dtype(df['int_col']))  # Int64 nullable
        self.assertTrue(pd.api.types.is_float_dtype(df['float_col']))
        self.assertTrue(pd.api.types.is_string_dtype(df['str_col']))  # string nullable
        # boolean column might be read as boolean or bool depending on pandas version
        self.assertTrue('bool' in str(df['bool_col'].dtype).lower())
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['date_col']))

    def test_view_parquet_file_nonexistent(self):
        """Test reading a non-existent parquet file."""
        df = view_parquet_file("nonexistent.parquet")

        self.assertIsNone(df)

    def test_view_parquet_file_invalid_path(self):
        """Test reading with an invalid file path."""
        # Create a temporary directory and try to read it as a file
        with tempfile.TemporaryDirectory() as temp_dir:
            df = view_parquet_file(temp_dir)
            # pandas treats empty directory as empty dataset, not an error
            self.assertIsInstance(df, pd.DataFrame)
            self.assertEqual(len(df), 0)
            self.assertEqual(len(df.columns), 0)

    def test_display_table_simple(self):
        """Test displaying a simple table."""
        df = view_parquet_file(str(self.simple_file))

        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            display_table(df, rows=2)
            output = captured_output.getvalue()

            # Check that output contains expected content
            self.assertIn("Parquet file shape: (3, 3)", output)
            self.assertIn("Column types:", output)
            self.assertIn("First 2 rows:", output)
            self.assertIn("Alice", output)
            self.assertIn("Bob", output)
            self.assertNotIn("Charlie", output)  # Should not show 3rd row

        finally:
            sys.stdout = sys.__stdout__

    def test_display_table_empty(self):
        """Test displaying an empty table."""
        df = view_parquet_file(str(self.empty_file))

        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            display_table(df, rows=10)
            output = captured_output.getvalue()

            # Check that output handles empty dataframe gracefully
            self.assertIn("Parquet file shape: (0, 2)", output)

        finally:
            sys.stdout = sys.__stdout__

    def test_display_table_none_dataframe(self):
        """Test displaying None dataframe."""
        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            display_table(None, rows=10)
            output = captured_output.getvalue()

            # Should handle None gracefully
            self.assertIn("No data to display", output)

        finally:
            sys.stdout = sys.__stdout__

    def test_display_table_custom_rows(self):
        """Test displaying table with custom row limit."""
        df = view_parquet_file(str(self.large_file))

        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            display_table(df, rows=5)
            output = captured_output.getvalue()

            # Should show first 5 rows
            self.assertIn("First 5 rows:", output)
            self.assertIn("Parquet file shape: (100, 4)", output)

        finally:
            sys.stdout = sys.__stdout__


if __name__ == '__main__':
    unittest.main()
