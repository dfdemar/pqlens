"""
Tabulate-based table formatter
"""

import pandas as pd

from .formatter import Formatter

# Import tabulate conditionally to avoid issues if not installed
try:
    from tabulate import tabulate

    TABULATE_AVAILABLE = True
except ImportError:
    TABULATE_AVAILABLE = False


    def tabulate(data_rows, headers=None, tablefmt='grid', showindex=False):
        # Fallback for when tabulate is not available
        return "tabulate not installed. Install with: pip install tabulate"


class TabulateFormatter(Formatter):
    """Formatter using the tabulate library."""

    def __init__(self):
        """Initialize the formatter."""
        self.available = TABULATE_AVAILABLE

    def format_table(self, df: pd.DataFrame, style: str = 'grid', showindex: bool = False, **kwargs) -> str:
        """
        Format a DataFrame using tabulate.
        
        Args:
            df: DataFrame to format
            style: Table format style ('grid', 'fancy_grid', etc.)
            showindex: Whether to show row indices
            **kwargs: Additional tabulate options
            
        Returns:
            str: Formatted table as string
        """
        if not self.available:
            return "tabulate not installed. Install with: pip install tabulate"

        try:
            return tabulate(df, headers=df.columns, tablefmt=style, showindex=showindex, **kwargs)
        except Exception as e:
            return f"Error formatting table: {e}\nFalling back to basic display:\n{df}"
