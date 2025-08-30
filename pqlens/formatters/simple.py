"""
Simple fallback formatter
"""

import pandas as pd

from .formatter import Formatter


class SimpleFormatter(Formatter):
    """Simple fallback formatter that doesn't require external dependencies."""

    def format_table(self, df: pd.DataFrame, **kwargs) -> str:
        """
        Format a DataFrame using basic string representation.
        
        Args:
            df: DataFrame to format
            **kwargs: Ignored for simple formatter
            
        Returns:
            str: Formatted table as string
        """
        return str(df)
