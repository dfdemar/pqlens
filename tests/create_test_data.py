#!/usr/bin/env python3
"""
Script to create sample Parquet files for testing
"""

import pandas as pd
import numpy as np
from pathlib import Path


def create_test_data():
    """Create sample Parquet files for testing."""
    test_data_dir = Path(__file__).parent / "data"
    test_data_dir.mkdir(exist_ok=True)
    
    # Simple dataset
    simple_df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [10.5, 20.0, 30.5]
    })
    simple_df.to_parquet(test_data_dir / "simple.parquet", index=False)
    
    # Empty dataset
    empty_df = pd.DataFrame({'col1': [], 'col2': []})
    empty_df.to_parquet(test_data_dir / "empty.parquet", index=False)
    
    # Large dataset (for pagination testing)
    np.random.seed(42)
    large_df = pd.DataFrame({
        'id': range(100),
        'category': np.random.choice(['A', 'B', 'C', 'D'], 100),
        'value': np.random.normal(50, 15, 100),
        'timestamp': pd.date_range('2024-01-01', periods=100, freq='D')
    })
    large_df.to_parquet(test_data_dir / "large.parquet", index=False)
    
    # Wide dataset (many columns)
    wide_data = {f'col_{i}': np.random.randn(10) for i in range(20)}
    wide_df = pd.DataFrame(wide_data)
    wide_df.to_parquet(test_data_dir / "wide.parquet", index=False)
    
    # Dataset with various data types using nullable dtypes
    mixed_df = pd.DataFrame({
        'int_col': pd.Series([1, 2, 3, None, 5], dtype='Int64'),
        'float_col': [1.1, 2.2, None, 4.4, 5.5],
        'str_col': pd.Series(['a', 'b', 'c', None, 'e'], dtype='string'),
        'bool_col': pd.Series([True, False, True, False, None], dtype='boolean'),
        'date_col': pd.date_range('2024-01-01', periods=5)
    })
    mixed_df.to_parquet(test_data_dir / "mixed_types.parquet", index=False)
    
    print(f"Test data created in {test_data_dir}")
    return test_data_dir


if __name__ == "__main__":
    create_test_data()