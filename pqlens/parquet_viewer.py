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
   # - Use DOWN arrow to go to next page
   # - Use UP arrow to go to previous page
   # - Press 'q' to quit
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
optional_packages = ["tabulate", "readchar"]

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
    def tabulate(data_rows, headers=None, tablefmt='grid', showindex=False):
        # Fallback for when tabulate is not available
        return "tabulate not installed. Install with: pip install tabulate"

# Import other packages for terminal input
has_readchar = False
readchar = None
try:
    import readchar

    # Test if the key module exists and works
    if hasattr(readchar, 'readkey'):
        has_readchar = True
except ImportError:
    print("readchar module not available. Install with: pip install readchar")
    print("Arrow key navigation disabled in interactive mode.")


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
    Display DataFrame in pages with arrow key navigation.

    :param df: DataFrame to display
    :param page_size: Number of rows to show per page
    :param table_format: Table format style ('grid', 'fancy_grid', etc.)
    """
    if df is None or df.empty:
        print("No data to display")
        return

    total_rows = len(df)
    total_pages = (total_rows + page_size - 1) // page_size  # Ceiling division
    current_page = 0

    # Print summary information once at the beginning
    print(f"\nParquet file shape: {df.shape}")
    print(f"Column types:\n{df.dtypes}\n")

    def display_current_page():
        """Helper function to display the current page"""
        # Clear screen using ANSI escape codes
        print("\033[H\033[J", end="")

        start_idx = current_page * page_size
        end_idx = min(start_idx + page_size, total_rows)

        # Display page header
        print(
            f"\n--- Showing rows {start_idx + 1}-{end_idx} of {total_rows} (Page {current_page + 1}/{total_pages}) ---")
        print("Navigation: ↑ Previous page | ↓ Next page | q Quit\n")

        # Display current page data as a formatted table
        page_df = df.iloc[start_idx:end_idx]
        try:
            table_output = tabulate(page_df, headers=df.columns, tablefmt=table_format, showindex=True)
            print(table_output)
        except Exception:
            # Fallback to basic DataFrame display if tabulate fails
            print(page_df)

    # Initial page display
    display_current_page()

    if has_readchar:
        # Main interaction loop using arrow keys
        while True:
            try:
                key = readchar.readkey()

                # Arrow keys and alternative keys for navigation
                down_keys = ('j', 'n')  # Down arrow alternatives
                up_keys = ('k', 'p')  # Up arrow alternatives

                # Check for down arrow or alternatives
                if hasattr(readchar, 'key') and hasattr(readchar.key, 'DOWN') and key == readchar.key.DOWN:
                    if current_page < total_pages - 1:
                        current_page += 1
                        display_current_page()
                # Check for alternative down keys
                elif key in down_keys:
                    if current_page < total_pages - 1:
                        current_page += 1
                        display_current_page()
                # Check for up arrow
                elif hasattr(readchar, 'key') and hasattr(readchar.key, 'UP') and key == readchar.key.UP:
                    if current_page > 0:
                        current_page -= 1
                        display_current_page()
                # Check for alternative up keys
                elif key in up_keys:
                    if current_page > 0:
                        current_page -= 1
                        display_current_page()
                # Check for quit keys
                elif key in ('q', 'Q', '\x03'):  # q, Q or Ctrl+C
                    print("\nExiting interactive mode.")
                    break
            except (AttributeError, TypeError):
                # Fallback if readchar is imported but key reading fails
                print("\nError reading keys. Exiting interactive mode.")
                break
    else:
        # Fallback to Enter key navigation if readchar is not available
        while current_page < total_pages - 1:
            try:
                user_input = input("\nPress Enter for next page, 'p' for previous page, 'q' to quit: ").lower()
                if user_input == 'q':
                    print("\nExiting interactive mode.")
                    break
                elif user_input == 'p' and current_page > 0:
                    current_page -= 1
                else:
                    current_page += 1
                display_current_page()
            except (EOFError, KeyboardInterrupt):
                print("\nDisplay stopped.")
                break


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='View Parquet file content')
    parser.add_argument('file_path', nargs='?', default='.samples/weather.parquet', help='Path to the parquet file')
    parser.add_argument('-n', '--rows', type=int, default=5, help='Number of rows to display')
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='Enable interactive mode with arrow key navigation')
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
