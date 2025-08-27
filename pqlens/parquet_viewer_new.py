#!/usr/bin/env python3
"""
Parquet File Viewer - Modular Implementation

This module provides the new modular implementation of the parquet viewer,
maintaining backward compatibility with the original API.
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

# Check optional packages
[check_package(pkg) for pkg in optional_packages]


def view_parquet_file(file_path):
    """
    Reads a Parquet file and returns its content as a DataFrame.
    
    Maintains backward compatibility with the original function.
    
    :param file_path: Path to the Parquet file.
    :return: DataFrame containing the data from the Parquet file, or None if error.
    """
    reader = ParquetReader()
    return reader.read_file(file_path)


def display_table(df, rows=10):
    """
    Display a DataFrame as a nicely formatted table.
    
    Maintains backward compatibility with the original function.
    
    :param df: DataFrame to display
    :param rows: Number of rows to display
    """
    display = DataFrameDisplay()
    display.show_table(df, rows)


def paged_display(df, page_size=10, table_format='grid'):
    """
    Display DataFrame in pages with arrow key navigation.
    
    Maintains backward compatibility with the original function.
    
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
    args = parser.parse_args()

    result_df = view_parquet_file(args.file_path)

    if result_df is not None:
        # Set pandas display options
        import pandas as pd
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        if args.interactive:
            # Use the paged display mode
            paged_display(result_df, args.rows, args.table_format)
        else:
            # Print summary info and display table
            display = DataFrameDisplay()
            display.show_table(result_df, args.rows)


if __name__ == "__main__":
    main()
