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
   $ pip install pandas pyarrow tabulate

Usage:
   $ python ./pqlens/parquet_viewer.py /path/to/file.parquet

   # Show first 10 rows
   $ python ./pqlens/parquet_viewer.py -n 10 /path/to/file.parquet

   # Interactive mode with scrolling
   $ python ./pqlens/parquet_viewer.py --interactive /path/to/file.parquet

   # Use fancy grid table format
   $ python ./pqlens/parquet_viewer.py --table-format fancy_grid /path/to/file.parquet
"""

import sys


# Check for required dependencies
def check_package(package_name):
    try:
        __import__(package_name)
        return True
    except ImportError:
        print(f"ERROR: Missing required package '{package_name}'")
        print(f"Please install it using: pip install {package_name}")
        print(f"If using a system Python, you may need: python -m pip install --user {package_name}")
        print(
            f"Or use a virtual environment: python -m venv venv && source venv/bin/activate && pip install {package_name}")
        return False


# Check for required packages
required_packages = ["pandas", "pyarrow"]
optional_packages = ["tabulate"]

# Check required packages first
missing_required = [pkg for pkg in required_packages if not check_package(pkg)]
if missing_required:
    print(f"ERROR: Missing required packages: {', '.join(missing_required)}")
    sys.exit(1)

# Check optional packages
[check_package(pkg) for pkg in optional_packages]

# Import required packages after checking they're installed
import pandas as pd

# Import tabulate conditionally to avoid issues if not installed
try:
    from tabulate import tabulate
except ImportError:
    def tabulate(data_table, **format_kwargs):
        # Fallback for when tabulate is not available
        return "tabulate not installed. Install with: pip install tabulate"

# Import other packages
try:
    import curses
except ImportError:
    print("Curses module not available. Interactive mode will be disabled.")
    curses = None


def view_parquet_file(file_path):
    """
    Reads a Parquet file and returns its content as a DataFrame.


    :param file_path: Path to the Parquet file.
    :return: DataFrame containing the data from the Parquet file.
    """
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return None


def display_table(df, rows=10):
    """
    Display a DataFrame as a nicely formatted table.

    :param df: DataFrame to display
    :param rows: Number of rows to display
    """
    if df is None or df.empty:
        print("No data to display")
        return

    print(f"\nParquet file shape: {df.shape}")
    print(f"\nColumn types:\n{df.dtypes}")
    print(f"\nFirst {rows} rows:")

    # Format as a nice table with borders
    table = tabulate(df.head(rows), headers=df.columns, tablefmt='grid')
    print(table)


def paged_display(df, page_size=10, table_format='grid'):
    """
    Display DataFrame in pages without requiring user interaction.

    :param df: DataFrame to display
    :param page_size: Number of rows to show per page
    :param table_format: Table format style ('grid', 'fancy_grid', etc.)
    """
    if df is None or df.empty:
        print("No data to display")
        return

    total_rows = len(df)
    total_pages = (total_rows + page_size - 1) // page_size  # Ceiling division

    # Print summary information
    print(f"\nParquet file shape: {df.shape}")
    print(f"Column types:\n{df.dtypes}\n")

    # Display all pages
    for page in range(total_pages):
        start_idx = page * page_size
        end_idx = min(start_idx + page_size, total_rows)

        # Display page header
        print(f"\n--- Showing rows {start_idx + 1}-{end_idx} of {total_rows} (Page {page + 1}/{total_pages}) ---\n")

        # Display current page data as a formatted table
        page_df = df.iloc[start_idx:end_idx]
        try:
            formatted_table = tabulate(page_df, headers=df.columns, tablefmt=table_format, showindex=True)
            print(formatted_table)
        except Exception as e:
            # Fallback to basic DataFrame display if tabulate fails
            print(page_df)

        # If not the last page, prompt to continue
        if page < total_pages - 1:
            try:
                input("\nPress Enter for next page (Ctrl+C to stop)...")
            except (EOFError, KeyboardInterrupt):
                print("\nDisplay stopped.")
                break


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='View Parquet file content')
    parser.add_argument('file_path', nargs='?',
                        default='.samples/weather.parquet',
                        help='Path to the parquet file')
    parser.add_argument('-n', '--rows', type=int, default=5, help='Number of rows to display')
    parser.add_argument('-i', '--interactive', action='store_true', help='Enable interactive mode with scrolling')
    parser.add_argument('-t', '--table-format',
                        choices=['plain', 'simple', 'github', 'grid', 'fancy_grid', 'pipe', 'orgtbl', 'jira'],
                        default='grid', help='Table format style')
    args = parser.parse_args()

    result_df = view_parquet_file(args.file_path)

    if result_df is not None:
        # Set pandas display options
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        if args.interactive:
            # Use the paged display mode
            paged_display(result_df, args.rows, args.table_format)
        else:
            # Print summary info
            print(f"\nParquet file shape: {result_df.shape}")
            print(f"\nColumn types:\n{result_df.dtypes}")
            print(f"\nFirst {args.rows} rows:")

            # Display as formatted table
            formatted_table = tabulate(result_df.head(args.rows), headers=result_df.columns,
                                       tablefmt=args.table_format, showindex=True)
            print(formatted_table)
