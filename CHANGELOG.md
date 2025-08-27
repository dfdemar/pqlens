# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025-08-27

### Major Architectural Refactoring

This release introduces a complete modular architecture while maintaining 100% backward compatibility.

### Added

- **Modular Architecture**: Clean separation of concerns with pluggable components
  - `pqlens.core.reader`: `ParquetReader` class for file I/O and validation
  - `pqlens.core.display`: `DataFrameDisplay` class for static table display  
  - `pqlens.core.interactive`: `InteractiveViewer` class for interactive navigation
  - `pqlens.formatters.formatter`: `Formatter` abstract interface for extensibility
  - `pqlens.formatters.table`: `TabulateFormatter` with tabulate library support
  - `pqlens.formatters.simple`: `SimpleFormatter` fallback implementation
  - `pqlens.utils.terminal`: `TerminalHelper` for terminal operations
  - `pqlens.utils.validation`: Input validation functions
  - `pqlens.utils.errors`: Custom exception classes

- **Programmatic API**: New modular components can be imported and used directly
  ```python
  from pqlens import ParquetReader, DataFrameDisplay, InteractiveViewer
  from pqlens import TabulateFormatter, SimpleFormatter, TerminalHelper
  ```

- **Plugin Architecture**: Easy to extend with custom formatters without touching core logic
- **Dependency Injection**: Components accept formatters/helpers as parameters for flexibility

### Changed

- **Complete codebase refactoring** from monolithic (~600 lines) to modular architecture
- **Enhanced test suite**: Expanded from 33 to 51 tests (100% pass rate maintained)
- **Improved maintainability**: Single responsibility principle applied throughout
- **Better extensibility**: Plugin system for formatters, easy to add new readers/formatters

### Technical Improvements

- **Legacy Compatibility**: Original API preserved through compatibility wrapper - no breaking changes
- **Error Handling**: Centralized custom exceptions with specific error types
- **Code Organization**: Clear module boundaries with well-defined interfaces
- **Documentation**: Updated README.md and CLAUDE.md with architecture details and API examples

### Backward Compatibility

- All existing functionality preserved through `pqlens.parquet_viewer` compatibility wrapper
- CLI interface unchanged - all existing commands and options work exactly as before
- Legacy API functions (`view_parquet_file`, `display_table`, `paged_display`) maintain identical behavior
- No changes required for existing users or scripts

### Benefits

- **Maintainability**: Each module has single responsibility and clear purpose
- **Testability**: Components can be tested in isolation with proper dependency injection
- **Extensibility**: Plugin architecture allows easy addition of new formatters/readers
- **Performance**: Modular loading reduces memory footprint for simple operations

## [0.1.0] - 2025-08-26

### Added

- Complete package structure with proper distribution support
- Command-line entry point: users can now run `pqlens file.parquet` instead of full Python path
- Version management with `pqlens --version` support
- Python module execution support with `python -m pqlens`
- Comprehensive testing framework with 100% pass rate (33 tests)
- Separate dependency groups for better installation control:
    - Core dependencies: `pandas`, `pyarrow` (always installed)
    - Interactive features: `tabulate`, `readchar` (install with `pip install .[interactive]`)
    - Development tools: `pytest`, `black`, `ruff` (install with `pip install .[dev]`)

### Changed

- Refactored main script to support both package and direct execution
- Updated README.md with package-based usage instructions and testing information
- Updated CLAUDE.md with testing framework details and package structure
- Improved installation documentation with multiple installation options
- Enhanced `display_table()` function to show structure info for empty DataFrames (consistency fix)

### Testing Framework

- **33 tests** across 3 test modules with 100% pass rate
- **Unit tests** (`test_parquet_viewer.py`): Core functions with various data types and edge cases
- **Integration tests** (`test_cli.py`): Full CLI functionality via subprocess (real behavior testing)
- **Package tests** (`test_package.py`): Import structure, version management, API exports
- **Test data**: Automatically generated Parquet files with diverse scenarios:
    - Simple datasets with basic data types
    - Empty datasets
    - Large datasets (100+ rows) for pagination testing
    - Wide datasets (20+ columns) for horizontal scrolling
    - Mixed data types using pandas nullable dtypes (`Int64`, `boolean`, `string`)

### Installation

Users can now install pqlens as a Python package
