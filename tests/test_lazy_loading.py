#!/usr/bin/env python3
"""
Tests for lazy loading functionality in pqlens
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
from pqlens.main import view_parquet_file, paged_display


class TestLazyLoading(unittest.TestCase):
    """Test cases for lazy loading functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_data_dir = Path(__file__).parent / "data"
        self.large_file = self.test_data_dir / "large.parquet"
        
        # Create a larger test file for lazy loading tests
        self.temp_dir = Path(tempfile.mkdtemp())
        self.large_lazy_file = self.temp_dir / "large_lazy_test.parquet"
        
        # Create a file that's large enough to trigger lazy loading
        np.random.seed(42)
        large_data = {
            'id': range(10000),
            'name': [f'user_{i}' for i in range(10000)],
            'value': np.random.randn(10000),
            'category': np.random.choice(['A', 'B', 'C', 'D'], 10000),
            'timestamp': pd.date_range('2023-01-01', periods=10000, freq='1min'),
            'flag': np.random.choice([True, False], 10000)
        }
        large_df = pd.DataFrame(large_data)
        large_df.to_parquet(self.large_lazy_file, index=False)

    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up temporary files
        if self.large_lazy_file.exists():
            self.large_lazy_file.unlink()
        if self.temp_dir.exists():
            self.temp_dir.rmdir()

    def test_parquet_reader_lazy_loading_threshold(self):
        """Test that lazy loading is triggered based on file size threshold."""
        # Test with low threshold - should trigger lazy loading
        reader_lazy = ParquetReader(memory_threshold_mb=0.1, enable_lazy_loading=True)  # 0.1MB threshold
        file_size = self.large_lazy_file.stat().st_size
        
        # Should use lazy loading for large file with low threshold
        should_use_lazy = reader_lazy._should_use_lazy_loading(file_size)
        file_size_mb = file_size / (1024 * 1024)
        self.assertTrue(should_use_lazy, f"Low memory threshold (0.1MB) should trigger lazy loading for large files (actual file size: {file_size_mb:.2f}MB)")
        
        # Test with high threshold - should not trigger lazy loading
        reader_no_lazy = ParquetReader(memory_threshold_mb=1000, enable_lazy_loading=True)
        should_not_use_lazy = reader_no_lazy._should_use_lazy_loading(file_size)
        self.assertFalse(should_not_use_lazy, "High memory threshold should not trigger lazy loading for small files")

    def test_parquet_reader_disable_lazy_loading(self):
        """Test that lazy loading can be disabled."""
        reader = ParquetReader(memory_threshold_mb=1, enable_lazy_loading=False)
        file_size = self.large_lazy_file.stat().st_size
        
        # Should not use lazy loading when disabled
        should_use_lazy = reader._should_use_lazy_loading(file_size)
        self.assertFalse(should_use_lazy, "Lazy loading should be disabled when explicitly turned off")

    def test_parquet_reader_memory_info(self):
        """Test memory monitoring functions."""
        reader = ParquetReader()
        
        # Test memory info functions
        available_memory = reader.get_available_memory_mb()
        current_memory = reader.get_memory_usage_mb()
        
        self.assertIsInstance(available_memory, float, "Available memory should be returned as float")
        self.assertIsInstance(current_memory, float, "Current memory usage should be returned as float")
        self.assertGreater(available_memory, 0, "Available memory should be positive")
        self.assertGreater(current_memory, 0, "Current memory usage should be positive")

    def test_parquet_reader_column_selection(self):
        """Test reading specific columns only."""
        reader = ParquetReader(enable_lazy_loading=False)
        
        # Read only specific columns
        df_partial = reader.read_file(str(self.large_lazy_file), columns=['id', 'name'])
        
        self.assertIsNotNone(df_partial, "DataFrame should be loaded with column selection")
        self.assertEqual(list(df_partial.columns), ['id', 'name'], "Only selected columns should be present")
        self.assertEqual(len(df_partial), 10000, "All rows should be loaded with column selection")

    def test_parquet_reader_row_range_selection(self):
        """Test reading specific row ranges."""
        reader = ParquetReader(enable_lazy_loading=False)
        
        # Read only specific row range
        df_range = reader.read_file(str(self.large_lazy_file), row_range=(100, 200))
        
        self.assertIsNotNone(df_range, "DataFrame should be loaded with row range selection")
        self.assertEqual(len(df_range), 100, "Row range should contain exactly 100 rows")  # 200 - 100 = 100 rows
        self.assertEqual(df_range.iloc[0]['id'], 100, "First row should have id 100 when starting at row 100")  # First row should have id 100

    def test_parquet_reader_lazy_loading_with_metadata(self):
        """Test that lazy loading provides file metadata without loading full data."""
        reader = ParquetReader(memory_threshold_mb=1, enable_lazy_loading=True)
        
        # This should load metadata only
        df = reader.read_file(str(self.large_lazy_file))
        
        # Get file info
        file_info = reader.get_file_info()
        
        if file_info:  # If lazy loading was used
            self.assertIn('num_rows', file_info, "File info should contain row count")
            self.assertIn('num_columns', file_info, "File info should contain column count")
            self.assertEqual(file_info['num_rows'], 10000, "File should contain 10000 rows")
            self.assertEqual(file_info['num_columns'], 6, "File should contain 6 columns")

    def test_view_parquet_file_with_columns(self):
        """Test the main API function with column selection."""
        # Test column selection
        df = view_parquet_file(str(self.large_lazy_file), columns=['id', 'value'])
        
        self.assertIsNotNone(df, "DataFrame should be loaded with column selection via main API")
        self.assertEqual(list(df.columns), ['id', 'value'], "Only requested columns should be returned")
        self.assertEqual(len(df), 10000, "All rows should be returned with column selection")

    def test_view_parquet_file_with_row_range(self):
        """Test the main API function with row range selection."""
        # Test row range selection
        df = view_parquet_file(str(self.large_lazy_file), row_range=(500, 600))
        
        self.assertIsNotNone(df, "DataFrame should be loaded with row range selection via main API")
        self.assertEqual(len(df), 100, "Row range should contain exactly 100 rows")
        self.assertEqual(df.iloc[0]['id'], 500, "First row should have id 500 when starting at row 500")

    def test_interactive_viewer_lazy_loading_init(self):
        """Test InteractiveViewer initialization with lazy loading."""
        reader = ParquetReader(memory_threshold_mb=1, enable_lazy_loading=True)
        reader.read_file(str(self.large_lazy_file))  # Load metadata
        
        # Create viewer with lazy loading
        viewer = InteractiveViewer(
            df=None, 
            parquet_reader=reader, 
            file_path=str(self.large_lazy_file)
        )
        
        self.assertTrue(viewer.lazy_loading_enabled, "Lazy loading should be enabled for interactive viewer")
        self.assertIsNotNone(viewer.file_info, "File info should be available for lazy loading")
        self.assertEqual(viewer.file_info['num_rows'], 10000, "File info should show correct row count")

    def test_interactive_viewer_get_view_data(self):
        """Test that InteractiveViewer can get data chunks on demand."""
        reader = ParquetReader(memory_threshold_mb=1, enable_lazy_loading=True)
        reader.read_file(str(self.large_lazy_file))  # Load metadata
        
        viewer = InteractiveViewer(
            df=None, 
            parquet_reader=reader, 
            file_path=str(self.large_lazy_file)
        )
        
        # Get a small chunk of data
        chunk = viewer._get_view_data(0, 10)
        
        self.assertIsNotNone(chunk, "Data chunk should be loaded on demand")
        self.assertEqual(len(chunk), 10, "Chunk should contain requested number of rows")
        self.assertEqual(chunk.iloc[0]['id'], 0, "First row of chunk should have id 0")
        self.assertEqual(chunk.iloc[9]['id'], 9, "Last row of chunk should have id 9")

    def test_interactive_viewer_caching(self):
        """Test that InteractiveViewer caches data chunks."""
        reader = ParquetReader(memory_threshold_mb=1, enable_lazy_loading=True)
        reader.read_file(str(self.large_lazy_file))
        
        viewer = InteractiveViewer(
            df=None, 
            parquet_reader=reader, 
            file_path=str(self.large_lazy_file)
        )
        
        # Get the same chunk twice
        chunk1 = viewer._get_view_data(0, 10)
        chunk2 = viewer._get_view_data(0, 10)
        
        # Should be cached
        self.assertEqual(len(viewer.cached_chunks), 1, "Exactly one chunk should be cached")
        self.assertTrue((0, 10) in viewer.cached_chunks, "Requested chunk should be in cache")
        
        # Chunks should be identical
        pd.testing.assert_frame_equal(chunk1, chunk2, "Cached chunks should be identical")

    def test_error_handling_lazy_loading(self):
        """Test error handling in lazy loading scenarios."""
        reader = ParquetReader(enable_lazy_loading=True)
        
        # Test with non-existent file
        df = reader.read_file("nonexistent_file.parquet")
        self.assertIsNone(df, "Non-existent file should return None")
        
        # Test with invalid file path for lazy loading
        viewer = InteractiveViewer(
            df=None,
            parquet_reader=reader,
            file_path="invalid_path.parquet"
        )
        
        # Should handle missing file gracefully
        chunk = viewer._get_view_data(0, 10)
        self.assertIsNone(chunk, "Missing file should return None for data chunks")


if __name__ == '__main__':
    unittest.main()