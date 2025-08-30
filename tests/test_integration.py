#!/usr/bin/env python3
"""
Integration tests for large file handling and end-to-end scenarios in pqlens
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


class TestLargeFileIntegration(unittest.TestCase):
    """Integration test cases for large file handling."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # Create various sized test files
        np.random.seed(42)
        
        # Medium file - 5000 rows, triggers some optimizations
        self.medium_file = self.temp_dir / "medium_test.parquet"
        medium_data = {
            'id': range(5000),
            'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], 5000),
            'value': np.random.randn(5000),
            'description': [f'description_text_for_item_{i}_with_some_length' for i in range(5000)],
            'timestamp': pd.date_range('2023-01-01', periods=5000, freq='5min'),
            'flag': np.random.choice([True, False], 5000)
        }
        medium_df = pd.DataFrame(medium_data)
        medium_df.to_parquet(self.medium_file, index=False)
        
        # Wide file - many columns
        self.wide_file = self.temp_dir / "wide_test.parquet"
        wide_data = {'id': range(1000)}
        # Add 50 additional columns
        for i in range(50):
            wide_data[f'col_{i:02d}'] = np.random.randn(1000)
        wide_df = pd.DataFrame(wide_data)
        wide_df.to_parquet(self.wide_file, index=False)
        
        # Large file - should definitely trigger lazy loading
        self.large_file = self.temp_dir / "large_integration_test.parquet"
        large_data = {
            'id': range(20000),
            'category': np.random.choice(['Category_A', 'Category_B', 'Category_C'], 20000),
            'measurement': np.random.randn(20000) * 100,
            'notes': [f'detailed_notes_for_record_{i}_with_substantial_text_content' for i in range(20000)],
            'created_at': pd.date_range('2020-01-01', periods=20000, freq='1H'),
            'active': np.random.choice([True, False], 20000),
            'score': np.random.uniform(0, 100, 20000)
        }
        large_df = pd.DataFrame(large_data)
        large_df.to_parquet(self.large_file, index=False)

    def tearDown(self):
        """Clean up test fixtures."""
        for file in [self.medium_file, self.wide_file, self.large_file]:
            if file.exists():
                file.unlink()
        if self.temp_dir.exists():
            self.temp_dir.rmdir()

    def test_medium_file_lazy_loading(self):
        """Test lazy loading behavior with medium-sized files."""
        # Force lazy loading with low threshold
        reader = ParquetReader(memory_threshold_mb=1, enable_lazy_loading=True)
        
        # Read the file
        df = reader.read_file(str(self.medium_file))
        
        # Should successfully read the data
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 5000)
        self.assertEqual(len(df.columns), 6)
        
        # Check file info was populated for lazy loading
        file_info = reader.get_file_info()
        if file_info:  # If lazy loading was triggered
            self.assertEqual(file_info['num_rows'], 5000)
            self.assertEqual(file_info['num_columns'], 6)

    def test_large_file_column_selection(self):
        """Test column selection with large files."""
        # Read only specific columns
        df = view_parquet_file(
            str(self.large_file), 
            columns=['id', 'category', 'measurement'],
            enable_lazy_loading=True
        )
        
        self.assertIsNotNone(df)
        self.assertEqual(list(df.columns), ['id', 'category', 'measurement'])
        self.assertEqual(len(df), 20000)
        
        # Verify data integrity
        self.assertTrue(all(isinstance(x, (int, np.integer)) for x in df['id']))
        self.assertTrue(all(isinstance(x, str) for x in df['category']))
        self.assertTrue(all(isinstance(x, (float, np.floating)) for x in df['measurement']))

    def test_large_file_row_range_selection(self):
        """Test row range selection with large files."""
        # Read specific row range
        df = view_parquet_file(
            str(self.large_file),
            row_range=(1000, 2000),
            enable_lazy_loading=True
        )
        
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 1000)  # 2000 - 1000 = 1000 rows
        self.assertEqual(df.iloc[0]['id'], 1000)  # First row should have id 1000
        self.assertEqual(df.iloc[-1]['id'], 1999)  # Last row should have id 1999

    def test_wide_file_handling(self):
        """Test handling of files with many columns."""
        reader = ParquetReader(enable_lazy_loading=True, memory_threshold_mb=1)
        df = reader.read_file(str(self.wide_file))
        
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 1000)
        self.assertEqual(len(df.columns), 51)  # id + 50 additional columns
        
        # Test column selection with many columns
        selected_columns = ['id', 'col_00', 'col_25', 'col_49']
        df_selected = reader.read_file(str(self.wide_file), columns=selected_columns)
        
        self.assertEqual(list(df_selected.columns), selected_columns)
        self.assertEqual(len(df_selected), 1000)

    def test_interactive_viewer_large_file(self):
        """Test interactive viewer with large files."""
        reader = ParquetReader(memory_threshold_mb=1, enable_lazy_loading=True)
        reader.read_file(str(self.large_file))  # Load metadata
        
        # Create interactive viewer
        viewer = InteractiveViewer(
            df=None,
            parquet_reader=reader,
            file_path=str(self.large_file)
        )
        
        self.assertTrue(viewer.lazy_loading_enabled)
        self.assertIsNotNone(viewer.file_info)
        self.assertEqual(viewer.file_info['num_rows'], 20000)
        self.assertEqual(viewer.file_info['num_columns'], 7)
        
        # Test getting different chunks
        chunk1 = viewer._get_view_data(0, 50)
        chunk2 = viewer._get_view_data(1000, 1050)
        
        self.assertIsNotNone(chunk1)
        self.assertIsNotNone(chunk2)
        self.assertEqual(len(chunk1), 50)
        self.assertEqual(len(chunk2), 50)
        
        # Verify chunks contain expected data
        self.assertEqual(chunk1.iloc[0]['id'], 0)
        self.assertEqual(chunk1.iloc[-1]['id'], 49)
        self.assertEqual(chunk2.iloc[0]['id'], 1000)
        self.assertEqual(chunk2.iloc[-1]['id'], 1049)

    def test_memory_usage_progression(self):
        """Test memory usage as files are processed."""
        reader = ParquetReader()
        
        # Get initial memory
        initial_memory = reader.get_memory_usage_mb()
        memory_readings = [initial_memory]
        
        # Process files of increasing size
        files_and_sizes = [
            (self.medium_file, 5000),
            (self.wide_file, 1000),
            (self.large_file, 20000)
        ]
        
        for file_path, expected_rows in files_and_sizes:
            df = reader.read_file(str(file_path))
            current_memory = reader.get_memory_usage_mb()
            memory_readings.append(current_memory)
            
            # Verify the data was loaded correctly
            self.assertIsNotNone(df)
            self.assertEqual(len(df), expected_rows)
            
            # Memory should be positive
            self.assertGreater(current_memory, 0)
            
            # Clean up to free memory
            del df
        
        # All memory readings should be reasonable
        for memory in memory_readings:
            self.assertGreater(memory, 0)
            self.assertLess(memory, 10000)  # Less than 10GB

    def test_end_to_end_lazy_loading_workflow(self):
        """Test complete workflow with lazy loading enabled."""
        # Test the complete workflow: load metadata, navigate, get chunks
        reader = ParquetReader(memory_threshold_mb=1, enable_lazy_loading=True)
        
        # Step 1: Load file metadata
        initial_result = reader.read_file(str(self.large_file))
        file_info = reader.get_file_info()
        
        if file_info:  # If lazy loading was used
            # Step 2: Create interactive viewer
            viewer = InteractiveViewer(
                df=None,
                parquet_reader=reader,
                file_path=str(self.large_file)
            )
            
            # Step 3: Simulate navigation through the file
            page_size = 100
            total_rows = file_info['num_rows']
            pages_to_test = min(5, total_rows // page_size)  # Test first 5 pages
            
            for page in range(pages_to_test):
                start_row = page * page_size
                end_row = min(start_row + page_size, total_rows)
                
                chunk = viewer._get_view_data(start_row, end_row)
                
                self.assertIsNotNone(chunk)
                expected_length = end_row - start_row
                self.assertEqual(len(chunk), expected_length)
                
                # Verify data integrity for the chunk
                self.assertEqual(chunk.iloc[0]['id'], start_row)
                
                # Check that categories are valid
                valid_categories = ['Category_A', 'Category_B', 'Category_C']
                self.assertTrue(all(cat in valid_categories for cat in chunk['category']))

    def test_performance_comparison(self):
        """Test performance characteristics of lazy vs non-lazy loading."""
        import time
        
        # Test regular loading
        start_time = time.time()
        reader_regular = ParquetReader(enable_lazy_loading=False)
        df_regular = reader_regular.read_file(str(self.medium_file))
        regular_time = time.time() - start_time
        
        # Test lazy loading
        start_time = time.time()
        reader_lazy = ParquetReader(memory_threshold_mb=1, enable_lazy_loading=True)
        df_lazy = reader_lazy.read_file(str(self.medium_file))
        lazy_time = time.time() - start_time
        
        # Both should produce valid results
        if df_lazy is not None:  # Regular loading
            self.assertEqual(len(df_regular), len(df_lazy))
            self.assertEqual(list(df_regular.columns), list(df_lazy.columns))
        else:  # Lazy loading was used
            file_info = reader_lazy.get_file_info()
            self.assertIsNotNone(file_info)
            self.assertEqual(file_info['num_rows'], len(df_regular))
        
        # Both times should be reasonable (less than 30 seconds)
        self.assertLess(regular_time, 30)
        self.assertLess(lazy_time, 30)

    def test_error_recovery_in_large_files(self):
        """Test error handling and recovery with large files."""
        reader = ParquetReader(enable_lazy_loading=True)
        
        # Test with corrupted/non-existent file path
        df = reader.read_file("definitely_does_not_exist.parquet")
        self.assertIsNone(df)
        
        # Test with valid file after error
        df = reader.read_file(str(self.medium_file))
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 5000)


if __name__ == '__main__':
    unittest.main()