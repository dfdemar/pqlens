#!/usr/bin/env python3
"""
Parquet File Viewer

This script allows you to view the contents of a Parquet file in a nicely formatted table.

Setup:
   # Create a virtual environment
   $ python -m venv .venv

   # Activate the virtual environment
   $ source .venv/Scripts/activate

   # Install required packages
   $ pip install pandas pyarrow tabulate readchar

Usage:
   $ python ./pqlens/parquet_viewer.py /path/to/file.parquet

   # Show first 10 rows
   $ python ./pqlens/parquet_viewer.py -n 10 /path/to/file.parquet

   # Interactive mode with arrow key navigation
   # - UP/DOWN arrows: Navigate between pages of rows
   # - LEFT/RIGHT arrows: Scroll through columns horizontally
   #   (row numbers column always stays visible)
   # - Press 'q' to quit
   $ python ./pqlens/parquet_viewer.py --interactive /path/to/file.parquet

   # Use fancy grid table format
   $ python ./pqlens/parquet_viewer.py --table-format fancy_grid /path/to/file.parquet
"""

import sys

from .core.display import DataFrameDisplay
from .core.interactive import InteractiveViewer
from .core.reader import ParquetReader


# Check for required dependencies
def check_package(package_name):
    try:
        __import__(package_name)
        return True
    except ImportError:
        print(f"ERROR: Missing required package '{package_name}'")
        print(f"Please install it using: pip install {package_name}")
        print(f"If using a system Python, you may need: python -m pip install --user {package_name}")
        print(f"Or use a virtual environment: python -m venv .venv && source .venv/Scripts/activate && pip install {package_name}")
        return False


# Check for required packages
required_packages = ["pandas", "pyarrow"]
optional_packages = ["tabulate", "readchar"]

# Check required packages first
missing_required = [pkg for pkg in required_packages if not check_package(pkg)]
if missing_required:
    print(f"ERROR: Missing required packages: {', '.join(missing_required)}")
    sys.exit(1)

# Check psutil for memory monitoring
try:
    import psutil
except ImportError:
    print("WARNING: psutil not available - memory monitoring disabled")
    print("Install with: pip install psutil")

# Check optional packages
[check_package(pkg) for pkg in optional_packages]


def view_parquet_file(file_path, columns=None, row_range=None, enable_lazy_loading=True):
    """
    Reads a Parquet file and returns its content as a DataFrame with memory optimization.
    
    Performs comprehensive validation and provides specific error messages
    for different failure scenarios. Uses lazy loading for large files.

    :param file_path: Path to the Parquet file.
    :param columns: Optional list of columns to read (for memory efficiency)
    :param row_range: Optional tuple (start_row, end_row) for partial reading
    :param enable_lazy_loading: Whether to enable lazy loading for large files
    :return: DataFrame containing the data from the Parquet file, or None if error.
    """
    reader = ParquetReader(enable_lazy_loading=enable_lazy_loading)
    return reader.read_file(file_path, columns=columns, row_range=row_range)


def display_table(df, rows=10):
    """
    Display a DataFrame as a nicely formatted table.
    
    Handles edge cases like zero columns, zero rows, and invalid parameters.

    :param df: DataFrame to display
    :param rows: Number of rows to display
    """
    display = DataFrameDisplay()
    display.show_table(df, rows)


def paged_display(df, page_size=10, table_format='grid'):
    """
    Display DataFrame in pages with arrow key navigation.
    
    Handles edge cases like zero columns, invalid parameters, and provides
    helpful feedback for unusual data structures.

    :param df: DataFrame to display
    :param page_size: Number of rows to show per page
    :param table_format: Table format style ('grid', 'fancy_grid', etc.)
    """
    viewer = InteractiveViewer(df)
    viewer.start_interactive_mode(page_size, table_format)


def main():
    """Main function for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(description='View Parquet file content')
    parser.add_argument('file_path', nargs='?', default='.samples/weather.parquet', help='Path to the parquet file')
    parser.add_argument('-n', '--rows', type=int, default=10, help='Number of rows to display')
    parser.add_argument('-i', '--interactive', action='store_true', help='Enable interactive mode with arrow key navigation')
    parser.add_argument('-t', '--table-format', choices=['plain', 'simple', 'github', 'grid', 'fancy_grid', 'pipe', 'orgtbl', 'jira'],
                        default='grid', help='Table format style')
    parser.add_argument('-c', '--columns', nargs='+', help='Specific columns to display (space-separated)')
    parser.add_argument('--no-lazy-loading', action='store_true', help='Disable lazy loading for large files')
    parser.add_argument('--memory-threshold', type=int, default=100, help='File size threshold in MB for lazy loading (default: 100)')
    args = parser.parse_args()

    enable_lazy = not args.no_lazy_loading
    
    # Initialize reader with memory optimization settings
    reader = ParquetReader(
        memory_threshold_mb=args.memory_threshold,
        enable_lazy_loading=enable_lazy
    )
    
    if args.interactive and enable_lazy:
        # For interactive mode with lazy loading, don't load the full dataset upfront
        reader.read_file(args.file_path)  # Load metadata only
        if args.interactive:
            paged_display(
                df=None, 
                page_size=args.rows, 
                table_format=args.table_format,
                file_path=args.file_path,
                columns=args.columns,
                enable_lazy_loading=enable_lazy
            )
            return
    
    # Load the data (potentially with lazy loading)
    result_df = reader.read_file(args.file_path, columns=args.columns)

    if result_df is not None:
        # Set pandas display options
        import pandas as pd
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        if args.interactive:
            # Use the paged display mode
            paged_display(result_df, args.rows, args.table_format, columns=args.columns)
        else:
            # Print summary info and display table
            display = DataFrameDisplay()
            display.show_table(result_df, args.rows)
            
            # Show memory usage if reader is available
            if hasattr(reader, 'get_memory_usage_mb'):
                current_memory = reader.get_memory_usage_mb()
                if current_memory > 0:
                    print(f"\nMemory usage: {current_memory:.1f} MB")


if __name__ == "__main__":
    main()
