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

import os
import shutil  # For getting terminal size
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
    
    Performs comprehensive validation and provides specific error messages
    for different failure scenarios.

    :param file_path: Path to the Parquet file.
    :return: DataFrame containing the data from the Parquet file, or None if error.
    """
    # Input validation
    if not file_path:
        print("Error: No file path provided")
        return None
    
    if not isinstance(file_path, (str, os.PathLike)):
        print(f"Error: Invalid file path type. Expected string or path-like object, got {type(file_path).__name__}")
        return None
    
    # Convert to string for consistent handling
    file_path = str(file_path)
    
    # File existence validation
    if not os.path.exists(file_path):
        print(f"Error: File not found - '{file_path}'")
        print("Please check the file path and try again.")
        return None
    
    # Let pandas handle directories - it may treat them as datasets
    # Only validate that the path exists, don't restrict file types
    
    # Check file permissions
    if not os.access(file_path, os.R_OK):
        print(f"Error: Permission denied - cannot read file '{file_path}'")
        print("Please check file permissions and try again.")
        return None
    
    # Get file size for memory error reporting
    file_size = os.path.getsize(file_path)
    
    # Check file extension (warning, not error)
    if not file_path.lower().endswith(('.parquet', '.pqt')):
        print(f"Warning: File '{file_path}' does not have a .parquet extension")
        print("Attempting to read as Parquet format anyway...")
    
    # Attempt to read the Parquet file with specific error handling
    try:
        df = pd.read_parquet(file_path)
        
        # Post-read validation
        if df is None:
            print(f"Error: Failed to read Parquet file '{file_path}' - result is None")
            return None
            
        # Check for zero columns (valid but unusual case)
        if len(df.columns) == 0:
            print(f"Warning: Parquet file '{file_path}' has no columns")
            print("This is a valid but unusual Parquet file structure.")
        
        return df
        
    except ImportError as e:
        print(f"Error: Missing required library - {e}")
        print("Please install required packages: pip install pandas pyarrow")
        return None
        
    except pd.errors.ParserError as e:
        print(f"Error: Invalid Parquet file format - '{file_path}'")
        print(f"Parser error: {e}")
        print("The file may be corrupted or not a valid Parquet file.")
        return None
        
    except PermissionError as e:
        print(f"Error: Permission denied accessing file '{file_path}'")
        print(f"System error: {e}")
        return None
        
    except FileNotFoundError as e:
        # This shouldn't happen due to our pre-checks, but handle it anyway
        print(f"Error: File not found during read operation - '{file_path}'")
        print(f"System error: {e}")
        return None
        
    except MemoryError as e:
        print(f"Error: Insufficient memory to load file '{file_path}'")
        print(f"The file may be too large for available memory.")
        print(f"File size: {file_size / (1024*1024):.1f} MB")
        print("Try using a machine with more RAM or processing the file in chunks.")
        return None
        
    except ValueError as e:
        error_msg = str(e).lower()
        if 'not a parquet file' in error_msg or 'parquet magic bytes' in error_msg:
            print(f"Error: '{file_path}' is not a valid Parquet file")
            print(f"The file may be corrupted, empty, or in a different format.")
        elif 'schema' in error_msg:
            print(f"Error: Invalid Parquet schema in file '{file_path}'")
            print(f"Schema error: {e}")
        else:
            print(f"Error: Invalid data in Parquet file '{file_path}'")
            print(f"Value error: {e}")
        return None
        
    except OSError as e:
        error_msg = str(e).lower()
        if 'no such file' in error_msg:
            print(f"Error: File disappeared during read operation - '{file_path}'")
        elif 'permission denied' in error_msg:
            print(f"Error: Permission denied accessing '{file_path}'")
        else:
            print(f"Error: System error accessing file '{file_path}'")
            print(f"OS error: {e}")
        return None
        
    except Exception as e:
        # Catch-all for unexpected errors
        print(f"Error: Unexpected error reading Parquet file '{file_path}'")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {e}")
        print("This may indicate a bug in the software or an unusual file format.")
        return None


def display_table(df, rows=10):
    """
    Display a DataFrame as a nicely formatted table.
    
    Handles edge cases like zero columns, zero rows, and invalid parameters.

    :param df: DataFrame to display
    :param rows: Number of rows to display
    """
    if df is None:
        print("No data to display")
        return
    
    # Validate rows parameter
    if not isinstance(rows, int) or rows < 0:
        print(f"Warning: Invalid rows parameter '{rows}', using default of 10")
        rows = 10

    print(f"\nParquet file shape: {df.shape}")
    
    # Handle zero columns case
    if len(df.columns) == 0:
        print("\nThis Parquet file has no columns.")
        print("It contains only row metadata without any data columns.")
        if len(df) > 0:
            print(f"Number of rows: {len(df)}")
        return
    
    print(f"\nColumn types:\n{df.dtypes}")
    
    # Handle zero rows case
    if len(df) == 0:
        print(f"\nFile structure (no data rows):")
    else:
        # Determine actual number of rows to show
        actual_rows = min(rows, len(df))
        print(f"\nFirst {actual_rows} rows:")

    try:
        # Format as a nice table with borders
        table = tabulate(df.head(rows), headers=df.columns, tablefmt='grid')
        print(table)
    except Exception as e:
        # Fallback for tabulate errors
        print(f"Warning: Could not format table properly: {e}")
        print("Falling back to basic display:")
        print(df.head(rows))


def paged_display(df, page_size=10, table_format='grid'):
    """
    Display DataFrame in pages with arrow key navigation.
    
    Handles edge cases like zero columns, invalid parameters, and provides
    helpful feedback for unusual data structures.

    :param df: DataFrame to display
    :param page_size: Number of rows to show per page
    :param table_format: Table format style ('grid', 'fancy_grid', etc.)
    """
    if df is None:
        print("No data to display - DataFrame is None")
        return
    
    # Validate parameters
    if not isinstance(page_size, int) or page_size <= 0:
        print(f"Warning: Invalid page_size '{page_size}', using default of 10")
        page_size = 10
    
    # Handle zero columns case
    if len(df.columns) == 0:
        print("\nThis Parquet file has no columns.")
        print("It contains only row metadata without any data columns.")
        if len(df) > 0:
            print(f"Number of rows: {len(df)}")
        print("\nNothing to display in interactive mode.")
        return
    
    # Handle empty DataFrame (has columns but no rows)
    if df.empty:
        print("\nDataFrame has columns but no data rows.")
        print(f"Columns ({len(df.columns)}): {list(df.columns)}")
        print(f"Column types:\n{df.dtypes}")
        print("\nNothing to navigate in interactive mode.")
        return

    # Get terminal dimensions
    terminal_width, terminal_height = shutil.get_terminal_size()

    # Constants for display formatting
    min_col_width = 10  # Minimum column width in characters
    max_col_width = 100  # Maximum column width in characters
    row_num_width = 6  # Fixed width for row numbers column
    separator_width = 3  # Width of column separators in table
    table_border_width = 4  # Width used by table borders
    extra_space = 10  # A bit of extra space

    total_rows = len(df)
    total_cols = len(df.columns)
    total_pages = (total_rows + page_size - 1) // page_size  # Ceiling division

    # Navigation state
    start_row = 0  # Starting row index (for single row scrolling)
    left_col_idx = 0  # Leftmost visible data column (0-indexed, not counting row numbers)

    # Print summary information once at the beginning
    print(f"\nParquet file shape: {df.shape}")
    print(f"Column types:\n{df.dtypes}\n")

    def get_visible_columns():
        """Determine which columns can fit in the current terminal width"""
        # Start with the row number column which is always visible
        available_width = terminal_width - row_num_width - separator_width - table_border_width - extra_space

        visible_cols = []
        col_idx = left_col_idx  # Start from current horizontal scroll position

        # Add columns until we run out of space
        while col_idx < total_cols and available_width > min_col_width:
            col_name = df.columns[col_idx]
            # Get sample values to estimate column width
            sample_values = df.iloc[:min(10, len(df)), col_idx].astype(str)
            max_data_width = sample_values.str.len().max() if len(sample_values) > 0 else 0

            # Estimate column width (max of column name and data width, capped at max_col_width)
            col_width = min(max(len(str(col_name)), max_data_width, min_col_width), max_col_width) + separator_width

            if available_width >= col_width:
                visible_cols.append(col_idx)
                available_width -= col_width
                col_idx += 1
            else:
                break

        return visible_cols

    def display_current_view():
        """Helper function to display the current view with both horizontal and vertical scrolling"""
        # Clear screen using ANSI escape codes
        print("\033[H\033[J", end="")

        # Get row indices for current view
        end_idx = min(start_row + page_size, total_rows)

        # Calculate current page number for display purposes
        current_page = start_row // page_size

        # Get columns that fit in the current terminal width
        visible_cols = get_visible_columns()

        # Display page header and navigation info
        print(f"\n--- Showing rows {start_row + 1}-{end_idx} of {total_rows} (Page {current_page + 1}/{total_pages}) ---")
        col_range_text = f"Columns {left_col_idx + 1}-{left_col_idx + len(visible_cols)} of {total_cols}"
        print(f"Navigation: ↑↓ Move one row | Page Up/Down: Move full page | ←→ Scroll Columns | q Quit | {col_range_text}\n")

        # Get current view data
        view_df = df.iloc[start_row:end_idx]

        # Select only visible columns for display
        if visible_cols:  # If we have any visible data columns
            display_df = view_df.iloc[:, visible_cols]
            try:
                # Format the DataFrame to control column widths
                # First, truncate any long values in the display DataFrame
                formatted_df = display_df.copy()

                # Truncate column headers and values to maximum width
                for col in formatted_df.columns:
                    col_name = str(col)
                    if len(col_name) > max_col_width:
                        new_name = col_name[:max_col_width - 3] + '...'
                        formatted_df.rename(columns={col: new_name}, inplace=True)

                # Truncate cell values
                for col in formatted_df.columns:
                    formatted_df[col] = formatted_df[col].astype(str).apply(
                        lambda x: x[:max_col_width - 3] + '...' if len(x) > max_col_width else x)

                # Create table with row index (always visible) and visible columns
                table_output = tabulate(formatted_df, headers=formatted_df.columns, tablefmt=table_format, showindex=True)

                print(table_output)
            except Exception:
                # Fallback to basic DataFrame display if tabulate fails
                print(display_df)
        else:
            # If no data columns can be displayed, just show row numbers
            print(tabulate([], headers=[], tablefmt=table_format, showindex=view_df.index))
            print("\nTerminal too narrow to display any data columns. Resize terminal or use horizontal scrolling.")

    # Initial view display
    display_current_view()

    if has_readchar:
        # Main interaction loop using arrow keys
        while True:
            try:
                key = readchar.readkey()

                # Map keys for navigation
                down_keys = ('j', 'n')  # Down arrow alternatives
                up_keys = ('k', 'p')  # Up arrow alternatives
                right_keys = ('l', ' ')  # Right arrow alternatives
                left_keys = ('h', 'b')  # Left arrow alternatives

                # Single row scrolling - DOWN arrow
                if hasattr(readchar, 'key') and hasattr(readchar.key, 'DOWN') and key == readchar.key.DOWN:
                    if start_row < total_rows - 1:  # Not at the last row
                        start_row += 1
                        display_current_view()
                # Alternative down keys
                elif key in down_keys:
                    if start_row < total_rows - 1:  # Not at the last row
                        start_row += 1
                        display_current_view()
                # Single row scrolling - UP arrow
                elif hasattr(readchar, 'key') and hasattr(readchar.key, 'UP') and key == readchar.key.UP:
                    if start_row > 0:  # Not at the first row
                        start_row -= 1
                        display_current_view()
                # Alternative up keys
                elif key in up_keys:
                    if start_row > 0:  # Not at the first row
                        start_row -= 1
                        display_current_view()
                # Full page scrolling - Page Down
                elif hasattr(readchar, 'key') and hasattr(readchar.key, 'PAGE_DOWN') and key == readchar.key.PAGE_DOWN:
                    if start_row + page_size < total_rows:  # Not at the last page
                        start_row = min(start_row + page_size, total_rows - page_size)
                        display_current_view()
                # Full page scrolling - Page Up
                elif hasattr(readchar, 'key') and hasattr(readchar.key, 'PAGE_UP') and key == readchar.key.PAGE_UP:
                    if start_row > 0:  # Not at the first page
                        start_row = max(0, start_row - page_size)
                        display_current_view()
                # Horizontal scrolling - RIGHT arrow
                elif hasattr(readchar, 'key') and hasattr(readchar.key, 'RIGHT') and key == readchar.key.RIGHT:
                    if left_col_idx < total_cols - 1:
                        left_col_idx += 1
                        display_current_view()
                # Alternative right keys
                elif key in right_keys:
                    if left_col_idx < total_cols - 1:
                        left_col_idx += 1
                        display_current_view()
                # Horizontal scrolling - LEFT arrow
                elif hasattr(readchar, 'key') and hasattr(readchar.key, 'LEFT') and key == readchar.key.LEFT:
                    if left_col_idx > 0:
                        left_col_idx -= 1
                        display_current_view()
                # Alternative left keys
                elif key in left_keys:
                    if left_col_idx > 0:
                        left_col_idx -= 1
                        display_current_view()
                # Quit keys
                elif key in ('q', 'Q', '\x03'):  # q, Q or Ctrl+C
                    print("\nExiting interactive mode.")
                    break
            except (AttributeError, TypeError):
                # Fallback if readchar is imported but key reading fails
                print("\nError reading keys. Exiting interactive mode.")
                break
    else:
        # Fallback to simplified keyboard navigation if readchar is not available
        while True:
            try:
                user_input = input("\nNavigation: [n]ext row, [p]revious row, [f]orward page, [b]ack page, [r]ight column, [l]eft column, [q]uit: ").lower()
                if user_input == 'q':
                    print("\nExiting interactive mode.")
                    break
                elif user_input == 'p' and start_row > 0:  # Previous row
                    start_row -= 1
                elif user_input == 'n' and start_row < total_rows - 1:  # Next row
                    start_row += 1
                elif user_input == 'b' and start_row > 0:  # Page back
                    start_row = max(0, start_row - page_size)
                elif user_input == 'f' and start_row < total_rows - page_size:  # Page forward
                    start_row = min(start_row + page_size, total_rows - 1)
                elif user_input == 'r' and left_col_idx < total_cols - 1:  # Right column
                    left_col_idx += 1
                elif user_input == 'l' and left_col_idx > 0:  # Left column
                    left_col_idx -= 1
                display_current_view()
            except (EOFError, KeyboardInterrupt):
                print("\nDisplay stopped.")
                break


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


if __name__ == "__main__":
    main()
