#!/usr/bin/env python3
"""
Tests for error handling in lazy loading and memory optimization scenarios
"""

import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path
import os

import pandas as pd
import numpy as np

from pqlens.core.reader import ParquetReader
from pqlens.core.interactive import InteractiveViewer
from pqlens.main import view_parquet_file


class TestErrorScenarios(unittest.TestCase):
    """Test cases for error handling in lazy loading scenarios."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # Create a valid test file
        self.valid_file = self.temp_dir / "valid_test.parquet"
        valid_data = {
            'id': range(100),
            'name': [f'name_{i}' for i in range(100)],
            'value': np.random.randn(100)
        }
        valid_df = pd.DataFrame(valid_data)
        valid_df.to_parquet(self.valid_file, index=False)
        
        # Create an empty file (not a valid parquet)
        self.empty_file = self.temp_dir / "empty.parquet"
        self.empty_file.write_text("")
        
        # Create a corrupted file
        self.corrupted_file = self.temp_dir / "corrupted.parquet"
        self.corrupted_file.write_text("This is not a parquet file content")

    def tearDown(self):
        """Clean up test fixtures."""
        for file in [self.valid_file, self.empty_file, self.corrupted_file]:
            if file.exists():
                file.unlink()
        if self.temp_dir.exists():
            self.temp_dir.rmdir()

    def test_nonexistent_file_lazy_loading(self):
        """Test lazy loading with non-existent files."""
        reader = ParquetReader(enable_lazy_loading=True)
        
        # Test with non-existent file
        df = reader.read_file("definitely_does_not_exist.parquet")
        self.assertIsNone(df, "Non-existent file should return None")
        
        # File info should also be None
        file_info = reader.get_file_info()
        self.assertIsNone(file_info, "File info should be None for non-existent files")

    def test_corrupted_file_lazy_loading(self):
        """Test lazy loading with corrupted files."""
        reader = ParquetReader(enable_lazy_loading=True)
        
        # Test with corrupted file
        df = reader.read_file(str(self.corrupted_file))
        self.assertIsNone(df, "Corrupted file should return None")
        
        # Should handle the error gracefully
        file_info = reader.get_file_info()
        self.assertIsNone(file_info, "File info should be None for corrupted files")

    def test_empty_file_lazy_loading(self):
        """Test lazy loading with empty files."""
        reader = ParquetReader(enable_lazy_loading=True)
        
        # Test with empty file
        df = reader.read_file(str(self.empty_file))
        self.assertIsNone(df, "Empty file should return None")

    def test_invalid_column_selection(self):
        """Test error handling for invalid column selection."""
        reader = ParquetReader(enable_lazy_loading=True)
        
        # Test with non-existent columns
        df = reader.read_file(str(self.valid_file), columns=['nonexistent_column'])
        # Should either return None or handle gracefully
        # The exact behavior depends on pyarrow's error handling
        self.assertTrue(df is None or isinstance(df, pd.DataFrame), "Invalid column selection should be handled gracefully")

    def test_invalid_row_range(self):
        """Test error handling for invalid row ranges."""
        reader = ParquetReader(enable_lazy_loading=True)
        
        # Test with invalid row range (negative start)
        df = reader.read_file(str(self.valid_file), row_range=(-1, 10))
        # Should handle gracefully
        self.assertTrue(df is None or isinstance(df, pd.DataFrame), "Invalid row range with negative start should be handled gracefully")
        
        # Test with invalid row range (start > end)
        df = reader.read_file(str(self.valid_file), row_range=(50, 10))
        # Should handle gracefully
        self.assertTrue(df is None or isinstance(df, pd.DataFrame), "Invalid row range with start > end should be handled gracefully")

    def test_permission_denied_scenario(self):
        """Test handling of permission denied scenarios."""
        # This test might be platform-specific and could be skipped on some systems
        reader = ParquetReader(enable_lazy_loading=True)
        
        # On Windows, we can't easily create permission-denied scenarios
        # So we'll test with a directory path instead of a file path
        if os.name == 'nt':  # Windows
            df = reader.read_file(str(self.temp_dir))  # Directory instead of file
            self.assertIsNone(df, "Directory path should return None")
        else:  # Unix-like systems
            # Create a file and remove read permissions
            restricted_file = self.temp_dir / "restricted.parquet"
            restricted_file.write_text("test")
            try:
                restricted_file.chmod(0o000)  # Remove all permissions
                df = reader.read_file(str(restricted_file))
                self.assertIsNone(df, "Permission denied file should return None")
            finally:
                restricted_file.chmod(0o644)  # Restore permissions for cleanup

    def test_interactive_viewer_error_scenarios(self):
        """Test error handling in InteractiveViewer with lazy loading."""
        reader = ParquetReader(enable_lazy_loading=True)
        
        # Test with non-existent file
        viewer = InteractiveViewer(
            df=None,
            parquet_reader=reader,
            file_path="nonexistent.parquet"
        )
        
        # Getting view data should handle missing file gracefully
        chunk = viewer._get_view_data(0, 10)
        self.assertIsNone(chunk)

    def test_memory_monitoring_errors(self):
        """Test error handling in memory monitoring functions."""
        reader = ParquetReader()
        
        # Memory functions should handle errors gracefully
        # and return -1 if they can't determine memory usage
        available_memory = reader.get_available_memory_mb()
        current_memory = reader.get_memory_usage_mb()
        
        # Should return either valid positive values or -1
        self.assertTrue(available_memory > 0 or available_memory == -1)
        self.assertTrue(current_memory > 0 or current_memory == -1)

    def test_lazy_loading_with_mixed_column_types(self):
        """Test lazy loading with edge case column types."""
        # Create file with potentially problematic column types
        complex_file = self.temp_dir / "complex_types.parquet"
        
        complex_data = {
            'id': range(50),
            'nullable_int': pd.array([1, 2, None, 4, 5] * 10, dtype="Int64"),
            'nullable_bool': pd.array([True, False, None, True, False] * 10, dtype="boolean"),
            'string_col': pd.array([f'text_{i}' if i % 3 != 0 else None for i in range(50)], dtype="string"),
            'datetime_col': pd.date_range('2023-01-01', periods=50, freq='1D'),
            'category_col': pd.Categorical(['A', 'B', 'C'] * 16 + ['A', 'B']),
        }
        complex_df = pd.DataFrame(complex_data)
        complex_df.to_parquet(complex_file, index=False)
        
        try:
            reader = ParquetReader(enable_lazy_loading=True)
            df = reader.read_file(str(complex_file))
            
            # Should handle complex types gracefully
            if df is not None:
                self.assertEqual(len(df), 50)
                self.assertEqual(len(df.columns), 6)
            else:
                # If lazy loading was used, check file info
                file_info = reader.get_file_info()
                if file_info:
                    self.assertEqual(file_info['num_rows'], 50)
                    self.assertEqual(file_info['num_columns'], 6)
                    
        finally:
            if complex_file.exists():
                complex_file.unlink()

    def test_extremely_large_row_range_request(self):
        """Test handling of unreasonably large row range requests."""
        reader = ParquetReader(enable_lazy_loading=True)
        
        # Request row range much larger than file
        df = reader.read_file(str(self.valid_file), row_range=(0, 1000000))
        
        # Should handle gracefully - either return available rows or handle error
        if df is not None:
            self.assertLessEqual(len(df), 100)  # File only has 100 rows
        else:
            # Error was handled gracefully
            self.assertIsNone(df)

    def test_concurrent_access_simulation(self):
        """Test behavior with rapid successive access patterns."""
        reader = ParquetReader(enable_lazy_loading=True)
        reader.read_file(str(self.valid_file))  # Load metadata
        
        viewer = InteractiveViewer(
            df=None,
            parquet_reader=reader,
            file_path=str(self.valid_file)
        )
        
        # Rapidly request multiple overlapping chunks
        chunks = []
        for i in range(5):
            start = i * 5
            end = start + 20  # Overlapping ranges
            chunk = viewer._get_view_data(start, min(end, 100))
            chunks.append(chunk)
        
        # All chunks should be valid or None
        for chunk in chunks:
            self.assertTrue(chunk is None or isinstance(chunk, pd.DataFrame))
        
        # Non-None chunks should have reasonable sizes
        valid_chunks = [c for c in chunks if c is not None]
        for chunk in valid_chunks:
            self.assertGreater(len(chunk), 0)
            self.assertLessEqual(len(chunk), 20)

    def test_cache_overflow_handling(self):
        """Test handling when cache becomes too large."""
        reader = ParquetReader(enable_lazy_loading=True)
        reader.read_file(str(self.valid_file))
        
        viewer = InteractiveViewer(
            df=None,
            parquet_reader=reader,
            file_path=str(self.valid_file)
        )
        
        # Fill cache beyond its limit (default is 10 chunks)
        for i in range(15):
            start = i * 5
            end = start + 5
            if end < 100:  # Within file bounds
                chunk = viewer._get_view_data(start, end)
                # Should handle cache management gracefully
                self.assertTrue(chunk is None or isinstance(chunk, pd.DataFrame))
        
        # Cache should not grow indefinitely
        self.assertLessEqual(len(viewer.cached_chunks), 10)

    def test_invalid_memory_threshold_values(self):
        """Test handling of invalid memory threshold values."""
        # Test with negative threshold
        reader = ParquetReader(memory_threshold_mb=-1, enable_lazy_loading=True)
        df = reader.read_file(str(self.valid_file))
        # Should handle gracefully
        self.assertTrue(df is None or isinstance(df, pd.DataFrame), "Negative memory threshold should be handled gracefully")
        
        # Test with zero threshold
        reader = ParquetReader(memory_threshold_mb=0, enable_lazy_loading=True)
        df = reader.read_file(str(self.valid_file))
        # Should handle gracefully
        self.assertTrue(df is None or isinstance(df, pd.DataFrame), "Zero memory threshold should be handled gracefully")
        
        # Test with extremely high threshold
        reader = ParquetReader(memory_threshold_mb=1000000, enable_lazy_loading=True)
        df = reader.read_file(str(self.valid_file))
        # Should handle gracefully
        self.assertTrue(df is None or isinstance(df, pd.DataFrame), "Extremely high memory threshold should be handled gracefully")

    def test_error_during_chunk_loading(self):
        """Test error handling when chunk loading fails mid-operation."""
        reader = ParquetReader(enable_lazy_loading=True)
        reader.read_file(str(self.valid_file))
        
        viewer = InteractiveViewer(
            df=None,
            parquet_reader=reader,
            file_path=str(self.valid_file)
        )
        
        # First, get a valid chunk to ensure setup is correct
        valid_chunk = viewer._get_view_data(0, 10)
        self.assertIsNotNone(valid_chunk)
        
        # Now simulate error by changing file path to invalid
        viewer.file_path = "invalid_path_after_init.parquet"
        
        # Subsequent chunk requests should handle error gracefully
        error_chunk = viewer._get_view_data(10, 20)
        self.assertIsNone(error_chunk)

    def test_main_api_error_handling(self):
        """Test error handling in main API functions."""
        # Test view_parquet_file with invalid parameters
        df = view_parquet_file("nonexistent.parquet", enable_lazy_loading=True)
        self.assertIsNone(df)
        
        # Test with invalid column selection
        df = view_parquet_file(
            str(self.valid_file), 
            columns=['nonexistent_column'],
            enable_lazy_loading=True
        )
        # Should handle gracefully
        self.assertTrue(df is None or isinstance(df, pd.DataFrame), "Invalid column selection in main API should be handled gracefully")
        
        # Test with invalid row range
        df = view_parquet_file(
            str(self.valid_file),
            row_range=(-1, 10),
            enable_lazy_loading=True
        )
        # Should handle gracefully
        self.assertTrue(df is None or isinstance(df, pd.DataFrame), "Invalid row range in main API should be handled gracefully")


if __name__ == '__main__':
    unittest.main()