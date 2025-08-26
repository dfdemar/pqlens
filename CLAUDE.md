# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pqlens` is a Python command-line tool for viewing and exploring Parquet files with interactive navigation capabilities. The project consists of a single main module that provides both static and interactive viewing modes for Parquet data.

## Architecture

- **pqlens/parquet_viewer.py**: Main module containing all functionality
  - `view_parquet_file()`: Core function to read Parquet files using pandas/pyarrow
  - `display_table()`: Static table display for simple viewing
  - `paged_display()`: Interactive viewer with arrow key navigation and horizontal/vertical scrolling
  - Terminal-aware column width calculation and display optimization
  - Graceful fallback when optional dependencies are missing

## Dependencies

**Required packages:**
- `pandas`: DataFrame operations and Parquet file reading
- `pyarrow`: Parquet file format support

**Optional packages (with fallbacks):**
- `tabulate`: Table formatting (fallback to basic display)
- `readchar`: Arrow key input for interactive mode (fallback to text input)

## Usage Patterns

The tool supports multiple invocation modes:
```bash
# Basic usage - shows first 10 rows
python ./pqlens/parquet_viewer.py /path/to/file.parquet

# Specify number of rows
python ./pqlens/parquet_viewer.py -n 20 /path/to/file.parquet

# Interactive mode with navigation
python ./pqlens/parquet_viewer.py --interactive /path/to/file.parquet

# Different table formats
python ./pqlens/parquet_viewer.py --table-format fancy_grid /path/to/file.parquet
```

## Development Environment

- Python 3.11+ (configured in pyproject.toml and .python-version)
- Use virtual environment: `python -m venv .venv` then activate with `.venv/Scripts/activate`
- Install dependencies: `pip install pandas pyarrow tabulate readchar`

## Key Implementation Details

- Interactive mode uses ANSI escape codes for screen clearing and terminal dimension detection
- Column width calculation dynamically adapts to terminal size
- Horizontal scrolling preserves row number column visibility
- Multiple keyboard input methods supported (arrow keys + alternative keys)
- Comprehensive error handling for missing dependencies and file access issues