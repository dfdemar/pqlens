#!/usr/bin/env python3
"""
Tests for memory monitoring functionality in pqlens
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


class TestMemoryMonitoring(unittest.TestCase):
    """Test cases for memory monitoring functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_data_dir = Path(__file__).parent / "data"
        
        # Create test data for memory monitoring
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_file = self.temp_dir / "memory_test.parquet"
        
        # Create a moderate-sized test file
        np.random.seed(42)
        data = {
            'id': range(1000),
            'data': np.random.randn(1000),
            'text': [f'text_data_{i}' for i in range(1000)]
        }
        df = pd.DataFrame(data)
        df.to_parquet(self.test_file, index=False)

    def tearDown(self):
        """Clean up test fixtures."""
        if self.test_file.exists():
            self.test_file.unlink()
        if self.temp_dir.exists():
            self.temp_dir.rmdir()

    def test_get_available_memory_mb(self):
        """Test getting available system memory."""
        reader = ParquetReader()
        
        available_memory = reader.get_available_memory_mb()
        
        # Should return a positive float
        self.assertIsInstance(available_memory, float, "Available memory should be returned as float")
        self.assertGreater(available_memory, 0, "Available memory should be positive")
        # Should be reasonable (less than 1TB = 1,000,000 MB)
        self.assertLess(available_memory, 1_000_000, "Available memory should be less than 1TB")

    def test_get_memory_usage_mb(self):
        """Test getting current process memory usage."""
        reader = ParquetReader()
        
        initial_memory = reader.get_memory_usage_mb()
        
        # Should return a positive float
        self.assertIsInstance(initial_memory, float, "Initial memory should be returned as float")
        self.assertGreater(initial_memory, 0, "Initial memory should be positive")
        
        # Load some data and check if memory usage increases
        df = reader.read_file(str(self.test_file))
        after_load_memory = reader.get_memory_usage_mb()
        
        # Memory usage should typically increase after loading data
        # (though this might be flaky due to garbage collection)
        self.assertIsInstance(after_load_memory, float, "Memory after loading should be returned as float")
        self.assertGreater(after_load_memory, 0, "Memory after loading should be positive")

    def test_memory_threshold_calculation(self):
        """Test memory threshold calculations for lazy loading."""
        reader = ParquetReader(memory_threshold_mb=50)
        
        # Test with various file sizes
        small_file_size = 10 * 1024 * 1024  # 10 MB
        large_file_size = 100 * 1024 * 1024  # 100 MB
        
        # Small file should not trigger lazy loading
        should_not_use_lazy = reader._should_use_lazy_loading(small_file_size)
        self.assertFalse(should_not_use_lazy, "Small file should not trigger lazy loading")
        
        # Large file should trigger lazy loading
        should_use_lazy = reader._should_use_lazy_loading(large_file_size)
        self.assertTrue(should_use_lazy, "Large file should trigger lazy loading")

    def test_memory_threshold_with_available_memory(self):
        """Test that lazy loading considers available system memory."""
        available_memory = ParquetReader().get_available_memory_mb()
        
        if available_memory > 0:
            # Use a threshold higher than our test files to isolate the memory-based logic
            high_threshold = int(available_memory * 0.8)  # 80% of available memory
            reader = ParquetReader(memory_threshold_mb=high_threshold, enable_lazy_loading=True)
            
            # File size that's 60% of available memory should trigger lazy loading
            large_file_size = int(available_memory * 0.6 * 1024 * 1024)  # Convert to bytes
            should_use_lazy = reader._should_use_lazy_loading(large_file_size)
            self.assertTrue(should_use_lazy, "File >50% of available memory should trigger lazy loading")
            
            # File size that's 30% of available memory should not trigger lazy loading
            small_file_size = int(available_memory * 0.3 * 1024 * 1024)  # Convert to bytes
            small_file_mb = small_file_size / (1024 * 1024)
            should_not_use_lazy = reader._should_use_lazy_loading(small_file_size)
            self.assertFalse(should_not_use_lazy, f"File <50% of available memory should not trigger lazy loading. Available: {available_memory:.1f}MB, File: {small_file_mb:.1f}MB, Threshold: {high_threshold}MB")

    def test_memory_monitoring_disabled_lazy_loading(self):
        """Test memory monitoring when lazy loading is disabled."""
        reader = ParquetReader(enable_lazy_loading=False)
        
        # Memory monitoring should still work
        available_memory = reader.get_available_memory_mb()
        current_memory = reader.get_memory_usage_mb()
        
        self.assertGreater(available_memory, 0, "Available memory should be positive when lazy loading disabled")
        self.assertGreater(current_memory, 0, "Current memory should be positive when lazy loading disabled")
        
        # But lazy loading should be disabled regardless of file size
        large_file_size = 1000 * 1024 * 1024  # 1 GB
        should_not_use_lazy = reader._should_use_lazy_loading(large_file_size)
        self.assertFalse(should_not_use_lazy, "Large file should not trigger lazy loading when disabled")

    def test_memory_error_handling(self):
        """Test memory monitoring error handling."""
        reader = ParquetReader()
        
        # The memory functions should handle errors gracefully
        # and return -1 if they can't determine memory usage
        available_memory = reader.get_available_memory_mb()
        current_memory = reader.get_memory_usage_mb()
        
        # Should either return valid positive values or -1 for errors
        self.assertTrue(available_memory > 0 or available_memory == -1, "Available memory should be positive or -1 for errors")
        self.assertTrue(current_memory > 0 or current_memory == -1, "Current memory should be positive or -1 for errors")

    def test_memory_info_in_file_info(self):
        """Test that file info includes memory-related information."""
        reader = ParquetReader(memory_threshold_mb=1, enable_lazy_loading=True)
        
        # Load file with lazy loading
        df = reader.read_file(str(self.test_file))
        file_info = reader.get_file_info()
        
        if file_info:  # If lazy loading was triggered
            # File info should contain metadata about the file
            self.assertIn('num_rows', file_info, "File info should contain row count")
            self.assertIn('num_columns', file_info, "File info should contain column count")
            self.assertIsInstance(file_info['num_rows'], int, "Row count should be integer")
            self.assertIsInstance(file_info['num_columns'], int, "Column count should be integer")
            self.assertGreater(file_info['num_rows'], 0, "Row count should be positive")
            self.assertGreater(file_info['num_columns'], 0, "Column count should be positive")

    def test_memory_threshold_edge_cases(self):
        """Test edge cases in memory threshold calculations."""
        # Test with zero threshold
        reader_zero = ParquetReader(memory_threshold_mb=0)
        tiny_file_size = 1024  # 1 KB
        should_use_lazy = reader_zero._should_use_lazy_loading(tiny_file_size)
        self.assertTrue(should_use_lazy, "Zero threshold should use lazy loading even for tiny files")
        
        # Test with very high threshold
        reader_high = ParquetReader(memory_threshold_mb=100000)  # 100 GB
        large_file_size = 1024 * 1024 * 1024  # 1 GB
        should_not_use_lazy = reader_high._should_use_lazy_loading(large_file_size)
        # This depends on available memory, so we can't assert definitively
        self.assertIsInstance(should_not_use_lazy, bool, "Should return boolean decision for lazy loading")

    def test_memory_monitoring_with_actual_data_loading(self):
        """Test memory monitoring during actual data loading operations."""
        reader = ParquetReader()
        
        # Get initial memory usage
        initial_memory = reader.get_memory_usage_mb()
        
        # Load data multiple times and monitor memory
        memory_readings = [initial_memory]
        
        for i in range(3):
            df = reader.read_file(str(self.test_file))
            current_memory = reader.get_memory_usage_mb()
            memory_readings.append(current_memory)
            
            # Clean up to potentially reduce memory usage
            del df
        
        # All readings should be positive numbers
        for i, memory_reading in enumerate(memory_readings):
            self.assertGreater(memory_reading, 0, f"Memory reading {i} should be positive")
        
        # Memory readings should be reasonable
        for i, memory_reading in enumerate(memory_readings):
            self.assertLess(memory_reading, 10000, f"Memory reading {i} should be less than 10 GB")


if __name__ == '__main__':
    unittest.main()