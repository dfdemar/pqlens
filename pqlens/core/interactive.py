"""
Interactive viewer with navigation
"""

import pandas as pd

from ..formatters.simple import SimpleFormatter
from ..formatters.table import TabulateFormatter
from ..utils.terminal import TerminalHelper
from ..utils.validation import validate_rows_parameter

# Import readchar conditionally
try:
    import readchar

    HAS_READCHAR = True
    # Test if the key module exists and works
    if not hasattr(readchar, 'readkey'):
        HAS_READCHAR = False
except ImportError:
    HAS_READCHAR = False
    readchar = None


class InteractiveViewer:
    """Interactive DataFrame viewer with arrow key navigation."""

    def __init__(self, df: pd.DataFrame, display=None, terminal_helper=None):
        """
        Initialize interactive viewer.
        
        Args:
            df: DataFrame to display interactively
            display: Display instance to use (optional)
            terminal_helper: TerminalHelper instance (optional)
        """
        self.df = df
        self.terminal = terminal_helper or TerminalHelper()

        # Set up formatter
        if display is not None:
            self.formatter = display.formatter
        else:
            # Try tabulate formatter first, fallback to simple
            tabulate_formatter = TabulateFormatter()
            if tabulate_formatter.available:
                self.formatter = tabulate_formatter
            else:
                self.formatter = SimpleFormatter()

        # Navigation state
        self.start_row = 0
        self.left_col_idx = 0

        # Display constants
        self.min_col_width = 10
        self.max_col_width = 100
        self.row_num_width = 6
        self.separator_width = 3
        self.table_border_width = 4
        self.extra_space = 10

    def start_interactive_mode(self, page_size: int = 10, table_format: str = 'grid') -> None:
        """
        Start the interactive navigation mode.
        
        Args:
            page_size: Number of rows to show per page
            table_format: Table format style ('grid', 'fancy_grid', etc.)
        """
        # Validate parameters
        try:
            page_size = validate_rows_parameter(page_size)
            if page_size <= 0:
                print(f"Warning: Invalid page_size '{page_size}', using default of 10")
                page_size = 10
        except ValueError as e:
            print(f"Warning: Invalid page_size '{page_size}', using default of 10")
            page_size = 10

        if self.df is None:
            print("No data to display - DataFrame is None")
            return

        # Handle zero columns case
        if len(self.df.columns) == 0:
            print("\nThis Parquet file has no columns.")
            print("It contains only row metadata without any data columns.")
            if len(self.df) > 0:
                print(f"Number of rows: {len(self.df)}")
            print("\nNothing to display in interactive mode.")
            return

        # Handle empty DataFrame (has columns but no rows)
        if self.df.empty:
            print("\nDataFrame has columns but no data rows.")
            print(f"Columns ({len(self.df.columns)}): {list(self.df.columns)}")
            print(f"Column types:\n{self.df.dtypes}")
            print("\nNothing to navigate in interactive mode.")
            return

        # Print summary information once at the beginning
        print(f"\nParquet file shape: {self.df.shape}")
        print(f"Column types:\n{self.df.dtypes}\n")

        # Start navigation
        self._handle_navigation(page_size, table_format)

    def _handle_navigation(self, page_size: int, table_format: str) -> None:
        """
        Handle the navigation loop.
        
        Args:
            page_size: Number of rows per page
            table_format: Table format style
        """
        total_rows = len(self.df)
        total_cols = len(self.df.columns)
        total_pages = (total_rows + page_size - 1) // page_size  # Ceiling division

        # Initial display
        self._refresh_display(page_size, table_format, total_rows, total_cols, total_pages)

        if HAS_READCHAR:
            self._handle_arrow_key_navigation(page_size, table_format, total_rows, total_cols, total_pages)
        else:
            print("readchar module not available. Install with: pip install readchar")
            print("Arrow key navigation disabled in interactive mode.")
            self._handle_text_navigation(page_size, table_format, total_rows, total_cols, total_pages)

    def _handle_arrow_key_navigation(self, page_size: int, table_format: str,
                                     total_rows: int, total_cols: int, total_pages: int) -> None:
        """Handle arrow key navigation."""
        while True:
            try:
                key = readchar.readkey()

                # Map keys for navigation
                down_keys = ('j', 'n')  # Down arrow alternatives
                up_keys = ('k', 'p')  # Up arrow alternatives
                right_keys = ('l', ' ')  # Right arrow alternatives
                left_keys = ('h', 'b')  # Left arrow alternatives

                # Navigation logic
                if (hasattr(readchar, 'key') and hasattr(readchar.key, 'DOWN') and
                    key == readchar.key.DOWN) or key in down_keys:
                    if self.start_row < total_rows - 1:
                        self.start_row += 1
                        self._refresh_display(page_size, table_format, total_rows, total_cols, total_pages)

                elif (hasattr(readchar, 'key') and hasattr(readchar.key, 'UP') and
                      key == readchar.key.UP) or key in up_keys:
                    if self.start_row > 0:
                        self.start_row -= 1
                        self._refresh_display(page_size, table_format, total_rows, total_cols, total_pages)

                elif (hasattr(readchar, 'key') and hasattr(readchar.key, 'PAGE_DOWN') and
                      key == readchar.key.PAGE_DOWN):
                    if self.start_row + page_size < total_rows:
                        self.start_row = min(self.start_row + page_size, total_rows - page_size)
                        self._refresh_display(page_size, table_format, total_rows, total_cols, total_pages)

                elif (hasattr(readchar, 'key') and hasattr(readchar.key, 'PAGE_UP') and
                      key == readchar.key.PAGE_UP):
                    if self.start_row > 0:
                        self.start_row = max(0, self.start_row - page_size)
                        self._refresh_display(page_size, table_format, total_rows, total_cols, total_pages)

                elif (hasattr(readchar, 'key') and hasattr(readchar.key, 'RIGHT') and
                      key == readchar.key.RIGHT) or key in right_keys:
                    if self.left_col_idx < total_cols - 1:
                        self.left_col_idx += 1
                        self._refresh_display(page_size, table_format, total_rows, total_cols, total_pages)

                elif (hasattr(readchar, 'key') and hasattr(readchar.key, 'LEFT') and
                      key == readchar.key.LEFT) or key in left_keys:
                    if self.left_col_idx > 0:
                        self.left_col_idx -= 1
                        self._refresh_display(page_size, table_format, total_rows, total_cols, total_pages)

                elif key in ('q', 'Q', '\x03'):  # q, Q or Ctrl+C
                    print("\nExiting interactive mode.")
                    break

            except (AttributeError, TypeError):
                print("\nError reading keys. Exiting interactive mode.")
                break

    def _handle_text_navigation(self, page_size: int, table_format: str,
                                total_rows: int, total_cols: int, total_pages: int) -> None:
        """Handle text-based navigation fallback."""
        while True:
            try:
                user_input = input("\nNavigation: [n]ext row, [p]revious row, [f]orward page, [b]ack page, [r]ight column, [l]eft column, [q]uit: ").lower()

                if user_input == 'q':
                    print("\nExiting interactive mode.")
                    break
                elif user_input == 'p' and self.start_row > 0:  # Previous row
                    self.start_row -= 1
                elif user_input == 'n' and self.start_row < total_rows - 1:  # Next row
                    self.start_row += 1
                elif user_input == 'b' and self.start_row > 0:  # Page back
                    self.start_row = max(0, self.start_row - page_size)
                elif user_input == 'f' and self.start_row < total_rows - page_size:  # Page forward
                    self.start_row = min(self.start_row + page_size, total_rows - 1)
                elif user_input == 'r' and self.left_col_idx < total_cols - 1:  # Right column
                    self.left_col_idx += 1
                elif user_input == 'l' and self.left_col_idx > 0:  # Left column
                    self.left_col_idx -= 1

                self._refresh_display(page_size, table_format, total_rows, total_cols, total_pages)

            except (EOFError, KeyboardInterrupt):
                print("\nDisplay stopped.")
                break

    def _refresh_display(self, page_size: int, table_format: str,
                         total_rows: int, total_cols: int, total_pages: int) -> None:
        """Refresh the display with current navigation state."""
        # Clear screen
        self.terminal.clear_screen()

        # Get row indices for current view
        end_idx = min(self.start_row + page_size, total_rows)

        # Calculate current page number for display purposes
        current_page = self.start_row // page_size

        # Get columns that fit in the current terminal width
        visible_cols = self._get_visible_columns()

        # Display page header and navigation info
        print(f"\n--- Showing rows {self.start_row + 1}-{end_idx} of {total_rows} (Page {current_page + 1}/{total_pages}) ---")
        col_range_text = f"Columns {self.left_col_idx + 1}-{self.left_col_idx + len(visible_cols)} of {total_cols}"
        print(f"Navigation: ↑↓ Move one row | Page Up/Down: Move full page | ←→ Scroll Columns | q Quit | {col_range_text}\n")

        # Get current view data
        view_df = self.df.iloc[self.start_row:end_idx]

        # Select only visible columns for display
        if visible_cols:  # If we have any visible data columns
            display_df = view_df.iloc[:, visible_cols]
            try:
                # Format the DataFrame to control column widths
                formatted_df = self._format_for_display(display_df)
                table_output = self.formatter.format_table(formatted_df, style=table_format, showindex=True)
                print(table_output)
            except Exception:
                # Fallback to basic DataFrame display if formatter fails
                print(display_df)
        else:
            # If no data columns can be displayed, just show row numbers
            try:
                empty_table = self.formatter.format_table(pd.DataFrame(), style=table_format, showindex=True)
                print(empty_table)
            except Exception:
                print("Row indices:", list(view_df.index))
            print("\nTerminal too narrow to display any data columns. Resize terminal or use horizontal scrolling.")

    def _get_visible_columns(self):
        """Determine which columns can fit in the current terminal width."""
        terminal_width, _ = self.terminal.get_size()

        # Start with the row number column which is always visible
        available_width = (terminal_width - self.row_num_width - self.separator_width -
                           self.table_border_width - self.extra_space)

        visible_cols = []
        col_idx = self.left_col_idx  # Start from current horizontal scroll position
        total_cols = len(self.df.columns)

        # Add columns until we run out of space
        while col_idx < total_cols and available_width > self.min_col_width:
            col_name = self.df.columns[col_idx]
            # Get sample values to estimate column width
            sample_values = self.df.iloc[:min(10, len(self.df)), col_idx].astype(str)
            max_data_width = sample_values.str.len().max() if len(sample_values) > 0 else 0

            # Estimate column width (max of column name and data width, capped at max_col_width)
            col_width = (min(max(len(str(col_name)), max_data_width, self.min_col_width),
                             self.max_col_width) + self.separator_width)

            if available_width >= col_width:
                visible_cols.append(col_idx)
                available_width -= col_width
                col_idx += 1
            else:
                break

        return visible_cols

    def _format_for_display(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format DataFrame for display with proper column width handling."""
        formatted_df = df.copy()

        # Truncate column headers and values to maximum width
        for col in formatted_df.columns:
            col_name = str(col)
            if len(col_name) > self.max_col_width:
                new_name = col_name[:self.max_col_width - 3] + '...'
                formatted_df.rename(columns={col: new_name}, inplace=True)

        # Truncate cell values
        for col in formatted_df.columns:
            formatted_df[col] = formatted_df[col].astype(str).apply(
                lambda x: x[:self.max_col_width - 3] + '...' if len(x) > self.max_col_width else x)

        return formatted_df
