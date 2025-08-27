"""
Terminal handling utilities for pqlens
"""

import shutil
import sys
from typing import Tuple


class TerminalHelper:
    """Helper class for terminal operations and detection."""

    @staticmethod
    def get_size() -> Tuple[int, int]:
        """
        Get terminal size in characters.
        
        Returns:
            Tuple[int, int]: (width, height) in characters
        """
        return shutil.get_terminal_size()

    @staticmethod
    def clear_screen() -> None:
        """Clear the terminal screen using ANSI escape codes."""
        print("\033[H\033[J", end="")

    @staticmethod
    def supports_unicode() -> bool:
        """
        Check if the terminal supports Unicode characters.
        
        Returns:
            bool: True if Unicode is likely supported
        """
        # Basic heuristic: check encoding and platform
        encoding = getattr(sys.stdout, 'encoding', '').lower()
        return 'utf' in encoding or 'unicode' in encoding

    @staticmethod
    def supports_ansi_colors() -> bool:
        """
        Check if the terminal supports ANSI color codes.
        
        Returns:
            bool: True if ANSI colors are likely supported
        """
        # Check if stdout is a tty (interactive terminal)
        return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
