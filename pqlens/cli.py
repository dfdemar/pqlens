#!/usr/bin/env python3
"""
Command-line interface for pqlens
"""

import sys

from . import __version__
from .main import main as viewer_main


def main():
    """Main entry point for the pqlens CLI."""
    # Add version option handling
    if len(sys.argv) > 1 and sys.argv[1] in ['--version', '-V']:
        print(f"pqlens {__version__}")
        sys.exit(0)

    # Delegate to the main viewer function
    viewer_main()


if __name__ == "__main__":
    main()
