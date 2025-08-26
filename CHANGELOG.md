# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-26

### Added
- Complete package structure with proper distribution support
- Command-line entry point: users can now run `pqlens file.parquet` instead of full Python path
- Version management with `pqlens --version` support
- Python module execution support with `python -m pqlens`
- Enhanced `pyproject.toml` with complete package metadata, dependencies, and optional features
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

### Technical Details
- Created `pqlens/cli.py` as main command-line interface entry point
- Added `pqlens/__main__.py` for `python -m pqlens` support
- Enhanced `pqlens/__init__.py` with version info and public API exports
- Configured entry point script in `pyproject.toml` for seamless command-line usage
- Maintained backward compatibility with direct script execution

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
- **No mocks**: All tests use real behavior and actual Parquet files
- **Comprehensive coverage**: Error conditions, edge cases, and various data scenarios

### Installation
Users can now install pqlens as a proper Python package:
```bash
# Basic installation
pip install .

# With interactive features (recommended)
pip install .[interactive]

# Development mode with testing
pip install -e .[interactive,dev]
```

### Usage
After installation, pqlens can be used as a simple command:
```bash
pqlens data.parquet
pqlens --interactive data.parquet
pqlens --version
python -m pqlens data.parquet
```

### Testing
Run the comprehensive test suite:
```bash
pytest tests/ -v  # All 33 tests with detailed output
pytest tests/test_parquet_viewer.py -v  # Unit tests only
pytest tests/test_cli.py -v  # Integration tests only
pytest tests/test_package.py -v  # Package structure tests only
```
