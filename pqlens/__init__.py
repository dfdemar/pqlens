"""
pqlens - A Python command-line tool for viewing and exploring Parquet files
"""

__version__ = "0.1.0"
__author__ = "David DeMar"
__email__ = "pqlens@example.com"

from .core.display import DataFrameDisplay
from .core.interactive import InteractiveViewer
# Import new modular components for advanced usage
from .core.reader import ParquetReader
from .formatters.simple import SimpleFormatter
from .formatters.table import TabulateFormatter
# Import from main module
from .main import view_parquet_file, display_table, paged_display
from .utils.terminal import TerminalHelper

__all__ = [
    "view_parquet_file",
    "display_table",
    "paged_display",
    "__version__",
    # New modular components
    "ParquetReader",
    "DataFrameDisplay",
    "InteractiveViewer",
    "TabulateFormatter",
    "SimpleFormatter",
    "TerminalHelper"
]
