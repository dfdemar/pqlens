"""
pqlens - A Python command-line tool for viewing and exploring Parquet files
"""

__version__ = "0.1.0"
__author__ = "David DeMar"
__email__ = "pqlens@example.com"

from .parquet_viewer import view_parquet_file, display_table, paged_display

__all__ = ["view_parquet_file", "display_table", "paged_display", "__version__"]
