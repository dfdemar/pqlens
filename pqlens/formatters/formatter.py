"""
Abstract base formatter interface
"""

from abc import ABC, abstractmethod

import pandas as pd


class Formatter(ABC):
    """Abstract base class for table formatters."""

    @abstractmethod
    def format_table(self, df: pd.DataFrame, **kwargs) -> str:
        """
        Format a DataFrame as a string table.
        
        Args:
            df: DataFrame to format
            **kwargs: Formatter-specific options
            
        Returns:
            str: Formatted table as string
        """
        pass
