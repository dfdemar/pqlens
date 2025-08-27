#!/usr/bin/env python3
"""
Parquet File Viewer - Legacy Compatibility Wrapper

This file maintains backward compatibility with the original parquet_viewer.py
while using the new modular architecture internally.

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

# Import all functionality from the new modular implementation
from .parquet_viewer_new import (
    check_package,
    view_parquet_file,
    display_table,
    paged_display,
    main
)

# Re-export for backward compatibility
__all__ = [
    "check_package",
    "view_parquet_file",
    "display_table",
    "paged_display",
    "main"
]

# Support direct execution
if __name__ == "__main__":
    main()
