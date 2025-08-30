"""
DataFrame display logic
"""

import pandas as pd

from ..formatters.simple import SimpleFormatter
from ..formatters.table import TabulateFormatter
from ..utils.validation import validate_rows_parameter


class DataFrameDisplay:
    """Handles static display of DataFrames."""

    def __init__(self, formatter=None):
        """
        Initialize display with a formatter.
        
        Args:
            formatter: Formatter instance to use. If None, defaults to TabulateFormatter
                      with SimpleFormatter fallback.
        """
        if formatter is None:
            # Try tabulate formatter first, fallback to simple
            tabulate_formatter = TabulateFormatter()
            if tabulate_formatter.available:
                self.formatter = tabulate_formatter
            else:
                self.formatter = SimpleFormatter()
        else:
            self.formatter = formatter

    def show_summary(self, df: pd.DataFrame) -> None:
        """
        Display DataFrame summary information.
        
        Args:
            df: DataFrame to summarize
        """
        if df is None:
            print("No data to display")
            return

        print(f"\nParquet file shape: {df.shape}")

        # Handle zero columns case
        if len(df.columns) == 0:
            print("\nThis Parquet file has no columns.")
            print("It contains only row metadata without any data columns.")
            if len(df) > 0:
                print(f"Number of rows: {len(df)}")
            return

        print(f"\nColumn types:\n{df.dtypes}")

    def show_table(self, df: pd.DataFrame, rows: int = 10) -> None:
        """
        Display a DataFrame as a nicely formatted table.
        
        Handles edge cases like zero columns, zero rows, and invalid parameters.

        Args:
            df: DataFrame to display
            rows: Number of rows to display
        """
        if df is None:
            print("No data to display")
            return

        # Validate rows parameter
        try:
            rows = validate_rows_parameter(rows)
        except ValueError as e:
            print(f"Warning: {e}, using default of 10")
            rows = 10

        self.show_summary(df)

        # Handle zero columns case
        if len(df.columns) == 0:
            return

        # Handle zero rows case
        if len(df) == 0:
            print(f"\nFile structure (no data rows):")
        else:
            # Determine actual number of rows to show
            actual_rows = min(rows, len(df))
            print(f"\nFirst {actual_rows} rows:")

        # Format the table
        try:
            table = self.formatter.format_table(df.head(rows), showindex=True)
            print(table)
        except Exception as e:
            # Fallback for formatter errors
            print(f"Warning: Could not format table properly: {e}")
            print("Falling back to basic display:")
            print(df.head(rows))

    def _handle_edge_cases(self, df: pd.DataFrame) -> bool:
        """
        Handle common edge cases for DataFrames.
        
        Args:
            df: DataFrame to check
            
        Returns:
            bool: True if edge case was handled, False if normal processing should continue
        """
        if df is None:
            print("No data to display")
            return True

        if len(df.columns) == 0:
            print("\nThis Parquet file has no columns.")
            print("It contains only row metadata without any data columns.")
            if len(df) > 0:
                print(f"Number of rows: {len(df)}")
            return True

        return False
