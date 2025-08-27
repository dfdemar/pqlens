# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pqlens` is a Python command-line tool for viewing and exploring Parquet files with interactive navigation capabilities. The project now uses a modular
architecture that separates concerns and provides both a backward-compatible legacy API and a new extensible modular API.

## Architecture

pqlens now uses a modular architecture for better maintainability, testability, and extensibility:

### Core Modules
- **pqlens/core/reader.py**: `ParquetReader` class for file I/O and validation
    - `read_file()`: Read Parquet files with comprehensive error handling
    - `validate_path()`: Path validation and normalization
    - Graceful error handling with specific error messages
    
- **pqlens/core/display.py**: `DataFrameDisplay` class for static table display
    - `show_table()`: Display DataFrames with customizable formatting
    - `show_summary()`: Display file metadata and column information
    - Pluggable formatter system (tabulate/simple)
    
- **pqlens/core/interactive.py**: `InteractiveViewer` class for navigation
    - `start_interactive_mode()`: Arrow key navigation and horizontal/vertical scrolling
    - Terminal-aware column width calculation and display optimization
    - Support for both readchar and text-based navigation fallbacks

### Formatters
- **pqlens/formatters/base.py**: `BaseFormatter` abstract interface for extensibility
- **pqlens/formatters/table.py**: `TabulateFormatter` with tabulate library support
- **pqlens/formatters/simple.py**: `SimpleFormatter` fallback implementation

### Utilities
- **pqlens/utils/terminal.py**: `TerminalHelper` for terminal operations (size, clearing, etc.)
- **pqlens/utils/validation.py**: Input validation functions (`validate_rows_parameter`, `validate_path_parameter`)
- **pqlens/utils/errors.py**: Custom exception classes (`PqlensError`, `InvalidFileError`, etc.)

### Legacy Compatibility
- **pqlens/parquet_viewer.py**: Backward-compatible wrapper that imports from new modular structure
- **pqlens/parquet_viewer_new.py**: New modular implementation (internal)
- **pqlens/parquet_viewer_original.py**: Backup of original monolithic implementation
- **pqlens/cli.py**: Command-line entry point with version handling
- **pqlens/__main__.py**: Support for `python -m pqlens` execution
- **pqlens/__init__.py**: Package initialization with both legacy and modular API exports

## Dependencies

**Required packages:**

- `pandas`: DataFrame operations and Parquet file reading
- `pyarrow`: Parquet file format support

**Optional packages (with fallbacks):**

- `tabulate`: Table formatting (fallback to basic display)
- `readchar`: Arrow key input for interactive mode (fallback to text input)

**Development packages:**

- `pytest`: Testing framework for comprehensive test suite

## Usage Patterns

The tool supports multiple invocation modes:

```bash
# Package command (preferred)
pqlens /path/to/file.parquet
pqlens --interactive /path/to/file.parquet
pqlens -n 20 --table-format github /path/to/file.parquet

# Module execution
python -m pqlens /path/to/file.parquet

# Direct script (fallback)
python ./pqlens/parquet_viewer.py /path/to/file.parquet
```

## Development Environment

- Python 3.11+ (configured in pyproject.toml and .python-version)
- Package installation: `pip install -e .[interactive,dev]` (editable mode with all dependencies)
- Use virtual environment: `python -m venv .venv` then activate with `.venv/Scripts/activate`

## Testing

**Comprehensive test suite with 100% pass rate (51 tests):**

```bash
# Run all tests
pytest tests/ -v

# Run specific test modules
pytest tests/test_parquet_viewer.py -v    # Unit tests for core functions
pytest tests/test_cli.py -v               # Integration tests for CLI
pytest tests/test_package.py -v           # Package structure tests
```

**Test categories:**

- **Unit tests**: Core functions with various data types and edge cases
- **Integration tests**: Full CLI functionality via subprocess (real behavior)
- **Package tests**: Import structure, version management, API exports

**Test data:** Automatically generated Parquet files covering simple, empty, large, wide, and mixed-type datasets using pandas nullable dtypes (`Int64`,
`boolean`, `string`)

**Testing principles:**

- No mocks - all tests use real behavior and actual Parquet files
- Comprehensive edge case coverage including error conditions
- Maintains 100% pass rate across all scenarios

## Key Implementation Details

- **Modular Architecture**: Clean separation of concerns with pluggable components
- **Backward Compatibility**: Legacy API preserved through compatibility wrapper
- **Single Responsibility Principle**: Each module/class has one clear purpose
- **Dependency Injection**: Components accept formatters/helpers as parameters
- **Plugin System**: Easy to extend with new formatters without touching core logic
- Interactive mode uses ANSI escape codes for screen clearing and terminal dimension detection
- Column width calculation dynamically adapts to terminal size
- Horizontal scrolling preserves row number column visibility
- Multiple keyboard input methods supported (arrow keys + alternative keys)
- Comprehensive error handling for missing dependencies and file access issues

# important-instruction-reminders

Do what has been asked; nothing more, nothing less.
**ALWAYS** ask for clarification if your instructions or any tasks are unclear
**NEVER** create files unless they're absolutely necessary for achieving your goal.
**ALWAYS** prefer editing an existing file to creating a new one.
**NEVER** proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
**ALWAYS** run the tests after writing them
**NEVER** use mocks or simulated behavior in tests
**ALWAYS** test actual behavior
**NEVER** rewrite tests using mocks to get them to pass
**NEVER** assume that a test failure is irrelevant or unrelated to your changes
**ALWAYS** run the tests after completing your tasks to verify that there are no test failures
**ALWAYS** ensure that the test pass rate is 100% with zero failing, skipped, or disabled tests
