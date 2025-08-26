# pqlens

A Python command-line tool for viewing and exploring Parquet files with interactive navigation capabilities.

## Features

- **Static viewing**: Display Parquet file contents in formatted tables
- **Interactive mode**: Navigate through large datasets with arrow keys
- **Horizontal scrolling**: View wide datasets that don't fit in terminal width
- **Multiple table formats**: Choose from various table styling options
- **Terminal-aware**: Automatically adapts to terminal size and capabilities
- **Graceful fallbacks**: Works even when optional dependencies are missing

## Installation

### Prerequisites

- Python 3.11 or higher

### Install from Source

1. Clone or download this repository
2. Install the package:
   ```bash
   # Install with basic functionality
   pip install .
   
   # Install with interactive features (recommended)
   pip install .[interactive]
   
   # Install in development mode (editable)
   pip install -e .[interactive]
   ```

### Alternative: Manual Setup

If you prefer not to install as a package:

1. Create a virtual environment:
   ```bash
   python -m venv .venv
   ```

2. Activate the virtual environment:
   ```bash
   # On Windows
   .venv\Scripts\activate
   
   # On macOS/Linux
   source .venv/bin/activate
   ```

3. Install required dependencies:
   ```bash
   pip install pandas pyarrow tabulate readchar
   ```

## Usage

### Basic Usage

After installation, you can use `pqlens` as a command:

```bash
# View first 10 rows of a Parquet file
pqlens /path/to/file.parquet

# Or using Python module syntax
python -m pqlens /path/to/file.parquet

# View first 20 rows
pqlens -n 20 /path/to/file.parquet

# Use a different table format
pqlens --table-format github /path/to/file.parquet

# Check version
pqlens --version
```

### Interactive Mode

```bash
# Enable interactive navigation
pqlens --interactive /path/to/file.parquet

# Interactive mode with more rows per page
pqlens -i -n 25 /path/to/file.parquet
```

### Alternative: Direct Script Usage

If not installed as a package, you can still run the script directly:

```bash
# Direct script usage (manual setup only)
python ./pqlens/parquet_viewer.py /path/to/file.parquet
```

In interactive mode, use these controls:
- **↑/↓** or **k/j**: Navigate up/down one row at a time
- **Page Up/Page Down**: Navigate full pages
- **←/→** or **h/l**: Scroll horizontally through columns
- **q**: Quit interactive mode

## Viewer

#### Viewer

```
--- Showing rows 1-10 of 150 (Page 1/15) ---
Navigation: ↑↓ Move one row | Page Up/Down: Move full page | ←→ Scroll Columns | q Quit | Columns 1-5 of 5

+----+----------------+---------------+----------------+---------------+-----------+
|    |   sepal.length |   sepal.width |   petal.length |   petal.width | variety   |
+====+================+===============+================+===============+===========+
|  0 |            5.1 |           3.5 |            1.4 |           0.2 | Setosa    |
+----+----------------+---------------+----------------+---------------+-----------+
|  1 |            4.9 |           3   |            1.4 |           0.2 | Setosa    |
+----+----------------+---------------+----------------+---------------+-----------+
|  2 |            4.7 |           3.2 |            1.3 |           0.2 | Setosa    |
+----+----------------+---------------+----------------+---------------+-----------+
|  3 |            4.6 |           3.1 |            1.5 |           0.2 | Setosa    |
+----+----------------+---------------+----------------+---------------+-----------+
|  4 |            5   |           3.6 |            1.4 |           0.2 | Setosa    |
+----+----------------+---------------+----------------+---------------+-----------+
|  5 |            5.4 |           3.9 |            1.7 |           0.4 | Setosa    |
+----+----------------+---------------+----------------+---------------+-----------+
|  6 |            4.6 |           3.4 |            1.4 |           0.3 | Setosa    |
+----+----------------+---------------+----------------+---------------+-----------+
|  7 |            5   |           3.4 |            1.5 |           0.2 | Setosa    |
+----+----------------+---------------+----------------+---------------+-----------+
|  8 |            4.4 |           2.9 |            1.4 |           0.2 | Setosa    |
+----+----------------+---------------+----------------+---------------+-----------+
|  9 |            4.9 |           3.1 |            1.5 |           0.1 | Setosa    |
+----+----------------+---------------+----------------+---------------+-----------+
```

## Command Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `file_path` | | Path to the Parquet file to view | `.samples/weather.parquet` |
| `--rows` | `-n` | Number of rows to display | `10` |
| `--interactive` | `-i` | Enable interactive mode with navigation | `False` |
| `--table-format` | `-t` | Table format style (see below) | `grid` |

## Table Format Options

The `--table-format` option supports the following styles:

| Format | Description | Example |
|--------|-------------|---------|
| `plain` | Simple space-separated columns | Basic text layout |
| `simple` | Clean table with minimal borders | Lines above and below headers |
| `github` | GitHub-flavored Markdown table format | Pipe-separated with alignment |
| `grid` | Full grid with borders around all cells | Complete box drawing |
| `fancy_grid` | Enhanced grid with decorative borders | Unicode box-drawing characters |
| `pipe` | Pipe-separated format | Similar to GitHub but simpler |
| `orgtbl` | Org-mode table format | Emacs org-mode compatible |
| `jira` | JIRA table format | Atlassian JIRA markup |

### Format Examples

**grid** (default):
```
+-------+--------+----------+
| Col 1 | Col 2  | Col 3    |
+=======+========+==========+
| val1  | val2   | val3     |
+-------+--------+----------+
```

**fancy_grid**:
```
╒═══════╤════════╤══════════╕
│ Col 1 │ Col 2  │ Col 3    │
╞═══════╪════════╪══════════╡
│ val1  │ val2   │ val3     │
╘═══════╧════════╧══════════╛
```

**github**:
```
| Col 1 | Col 2 | Col 3 |
|-------|-------|-------|
| val1  | val2  | val3  |
```

## Interactive Mode Features

### Navigation
- **Single-row scrolling**: Move up/down one row at a time for precise navigation
- **Page-based navigation**: Jump through large datasets quickly
- **Horizontal scrolling**: View datasets with many columns
- **Row numbers always visible**: Index column stays fixed during horizontal scrolling

### Terminal Adaptation
- **Dynamic column width**: Automatically calculates optimal column widths
- **Terminal size awareness**: Adapts display to current terminal dimensions
- **Long value truncation**: Handles cells with long content gracefully
- **Column overflow handling**: Shows subset of columns that fit in terminal width

### Fallback Support
- **Alternative key bindings**: Works with `hjkl` and other keys if arrow keys aren't available
- **Text-based navigation**: Falls back to text prompts if `readchar` module is missing
- **Basic table display**: Functions without `tabulate` module (with reduced formatting)

## Examples

### Viewing a Dataset
```bash
# Basic view - see file structure and first 10 rows
pqlens data.parquet
```

### Exploring Large Files
```bash
# Interactive mode for large datasets
pqlens --interactive large_dataset.parquet

# Show more rows at once
pqlens -i -n 25 large_dataset.parquet
```

### Different Formats
```bash
# Clean GitHub-style table
pqlens -t github data.parquet

# Fancy Unicode borders (may have encoding issues on some Windows terminals)
pqlens -t fancy_grid data.parquet

# Simple format for copy-pasting
pqlens -t simple data.parquet
```

## Dependencies

### Required
- **pandas**: DataFrame operations and Parquet reading
- **pyarrow**: Parquet file format support

### Optional
- **tabulate**: Enhanced table formatting (fallback to basic display if missing)
- **readchar**: Arrow key input for interactive mode (fallback to text input if missing)

## Requirements

- Python 3.11+
- Terminal with Unicode support (recommended for best table formatting)

## Troubleshooting

### Missing Dependencies
If you see dependency errors, install missing packages:
```bash
pip install pandas pyarrow tabulate readchar
```

### Arrow Keys Not Working
If arrow key navigation doesn't work in interactive mode:
- Ensure `readchar` is installed: `pip install readchar`
- Use alternative keys: `hjkl` for navigation, `np` for up/down, `fb` for page navigation

### Display Issues
- Increase terminal width for better column display
- Use `--table-format simple` for terminals with limited Unicode support
- Try different table formats if borders appear corrupted
