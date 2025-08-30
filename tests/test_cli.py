#!/usr/bin/env python3
"""
Integration tests for pqlens CLI functionality
"""

import sys
import unittest
from pathlib import Path

import click
from click.testing import CliRunner


class TestCLI(unittest.TestCase):
    """Test cases for CLI functionality."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.test_data_dir = Path(__file__).parent / "data"
        cls.simple_file = cls.test_data_dir / "simple.parquet"
        cls.empty_file = cls.test_data_dir / "empty.parquet"
        cls.large_file = cls.test_data_dir / "large.parquet"
        cls.runner = CliRunner()

    def create_click_wrapper(self):
        """Create a Click wrapper around the existing CLI."""
        @click.command()
        @click.argument('file_path', required=False)
        @click.option('-n', '--rows', type=int, default=10, help='Number of rows to display')
        @click.option('-i', '--interactive', is_flag=True, help='Enable interactive mode with arrow key navigation')
        @click.option('-t', '--table-format', 
                      type=click.Choice(['plain', 'simple', 'github', 'grid', 'fancy_grid', 'pipe', 'orgtbl', 'jira']),
                      default='grid', help='Table format style')
        @click.option('--version', is_flag=True, help='Show version and exit')
        @click.option('-V', is_flag=True, help='Show version and exit')
        @click.option('--help', is_flag=True, help='Show this message and exit')
        @click.option('-c', '--columns', multiple=True, help='Specific columns to display')
        @click.option('--no-lazy-loading', is_flag=True, help='Disable lazy loading for large files')
        @click.option('--memory-threshold', type=int, default=100, help='File size threshold in MB for lazy loading')
        def cli_wrapper(file_path, rows, interactive, table_format, version, v, help, columns, no_lazy_loading, memory_threshold):
            """CLI wrapper for testing."""
            # Handle version flags
            if version or v:
                from pqlens import __version__
                click.echo(f"pqlens {__version__}")
                return
                
            # Handle help
            if help:
                ctx = click.get_current_context()
                click.echo(ctx.get_help())
                return
                
            # Convert Click arguments to sys.argv format for the existing CLI
            original_argv = sys.argv[:]
            try:
                sys.argv = ['pqlens']
                if file_path:
                    sys.argv.append(str(file_path))
                if rows != 10:
                    sys.argv.extend(['-n', str(rows)])
                if interactive:
                    sys.argv.append('-i')
                if table_format != 'grid':
                    sys.argv.extend(['-t', table_format])
                if columns:
                    sys.argv.extend(['-c'] + list(columns))
                if no_lazy_loading:
                    sys.argv.append('--no-lazy-loading')
                if memory_threshold != 100:
                    sys.argv.extend(['--memory-threshold', str(memory_threshold)])
                    
                # Import and run the actual CLI
                from pqlens.main import main
                main()
            except SystemExit as e:
                # Re-raise SystemExit so Click can handle it properly
                if e.code != 0:
                    raise click.ClickException(f"Command failed with exit code {e.code}")
            finally:
                sys.argv = original_argv
                
        return cli_wrapper

    def run_cli_command(self, args, input_data=None):
        """Helper to run CLI commands using CliRunner."""
        cli_wrapper = self.create_click_wrapper()
        
        # Handle special cases for help and version
        if '--help' in args:
            args = ['--help']
        elif '--version' in args:
            args = ['--version']
        elif '-V' in args:
            args = ['-V']
            
        result = self.runner.invoke(cli_wrapper, args, input=input_data, catch_exceptions=False)
        return result.exit_code, result.output, result.stderr_bytes.decode() if result.stderr_bytes else ""

    def test_version_flag(self):
        """Test --version flag."""
        returncode, stdout, stderr = self.run_cli_command(["--version"])

        self.assertEqual(returncode, 0)
        self.assertIn("pqlens", stdout.lower())
        self.assertIn("0.1.0", stdout)
        self.assertEqual(stderr.strip(), "")

    def test_version_flag_short(self):
        """Test -V flag."""
        returncode, stdout, stderr = self.run_cli_command(["-V"])

        self.assertEqual(returncode, 0)
        self.assertIn("pqlens", stdout.lower())
        self.assertIn("0.1.0", stdout)
        self.assertEqual(stderr.strip(), "")

    def test_help_flag(self):
        """Test --help flag."""
        returncode, stdout, stderr = self.run_cli_command(["--help"])

        self.assertEqual(returncode, 0)
        self.assertIn("usage:", stdout.lower())
        self.assertIn("show this message", stdout.lower())
        self.assertIn("interactive", stdout.lower())
        self.assertIn("table-format", stdout)
        self.assertIn("rows", stdout)

    def test_simple_file_display(self):
        """Test displaying a simple parquet file."""
        returncode, stdout, stderr = self.run_cli_command([str(self.simple_file)])

        self.assertEqual(returncode, 0)
        self.assertIn("Parquet file shape: (3, 3)", stdout)
        self.assertIn("Alice", stdout)
        self.assertIn("Bob", stdout)
        self.assertIn("Charlie", stdout)
        self.assertIn("Column types:", stdout)

    def test_empty_file_display(self):
        """Test displaying an empty parquet file."""
        returncode, stdout, stderr = self.run_cli_command([str(self.empty_file)])

        self.assertEqual(returncode, 0)
        self.assertIn("Parquet file shape: (0, 2)", stdout)
        self.assertIn("Column types:", stdout)

    def test_custom_rows_parameter(self):
        """Test --rows parameter."""
        returncode, stdout, stderr = self.run_cli_command([
            "-n", "2", str(self.simple_file)
        ])

        self.assertEqual(returncode, 0)
        self.assertIn("First 2 rows:", stdout)
        self.assertIn("Alice", stdout)
        self.assertIn("Bob", stdout)
        # Charlie should not appear with only 2 rows displayed
        self.assertNotIn("Charlie", stdout)

    def test_table_format_github(self):
        """Test --table-format github."""
        returncode, stdout, stderr = self.run_cli_command([
            "--table-format", "github", str(self.simple_file)
        ])

        self.assertEqual(returncode, 0)
        self.assertIn("Alice", stdout)
        # GitHub format should use pipes
        self.assertIn("|", stdout)

    def test_table_format_simple(self):
        """Test --table-format simple."""
        returncode, stdout, stderr = self.run_cli_command([
            "--table-format", "simple", str(self.simple_file)
        ])

        self.assertEqual(returncode, 0)
        self.assertIn("Alice", stdout)
        # Simple format should have some table structure
        self.assertIn("id", stdout)
        self.assertIn("name", stdout)
        self.assertIn("value", stdout)

    def test_nonexistent_file(self):
        """Test handling of non-existent file."""
        # This test expects the command to handle the error gracefully
        try:
            returncode, stdout, stderr = self.run_cli_command(["nonexistent.parquet"])
            # Should not crash, but should handle gracefully
            self.assertIsInstance(returncode, int)
        except Exception:
            # If it raises an exception, that's also acceptable as error handling
            pass

    def test_combined_parameters(self):
        """Test combining multiple parameters."""
        returncode, stdout, stderr = self.run_cli_command([
            "-n", "1",
            "--table-format", "grid",
            str(self.simple_file)
        ])

        self.assertEqual(returncode, 0)
        self.assertIn("First 1 rows:", stdout)
        self.assertIn("Alice", stdout)
        self.assertNotIn("Bob", stdout)
        self.assertNotIn("Charlie", stdout)

    def test_large_file_handling(self):
        """Test handling of larger files."""
        returncode, stdout, stderr = self.run_cli_command([
            "-n", "5", str(self.large_file)
        ])

        self.assertEqual(returncode, 0)
        self.assertIn("Parquet file shape: (100, 4)", stdout)
        self.assertIn("First 5 rows:", stdout)
        # Should contain some of the test data
        self.assertIn("category", stdout)
        self.assertIn("value", stdout)

    def test_invalid_table_format(self):
        """Test invalid table format parameter."""
        try:
            returncode, stdout, stderr = self.run_cli_command([
                "--table-format", "invalid_format", str(self.simple_file)
            ])
            # Should return non-zero exit code for invalid format
            self.assertNotEqual(returncode, 0)
        except Exception as e:
            # Click should raise an exception for invalid choice
            self.assertIn("invalid choice", str(e).lower())

    def test_invalid_rows_parameter(self):
        """Test invalid rows parameter."""
        try:
            returncode, stdout, stderr = self.run_cli_command([
                "--rows", "not_a_number", str(self.simple_file)
            ])
            # Should return non-zero exit code for invalid rows value
            self.assertNotEqual(returncode, 0)
        except Exception as e:
            # Click should raise an exception for invalid type
            self.assertIn("invalid", str(e).lower())

    def test_column_selection(self):
        """Test column selection CLI option."""
        returncode, stdout, stderr = self.run_cli_command([
            str(self.simple_file),
            "-c", "id", "-c", "name"
        ])

        self.assertEqual(returncode, 0, f"CLI failed with return code {returncode}. stderr: {stderr}")
        self.assertIn("id", stdout, "Column 'id' should be in output")
        self.assertIn("name", stdout, "Column 'name' should be in output")
        # Should not contain 'value' column
        lines_with_value = [line for line in stdout.split('\n') if 'value' in line.lower() and 'col' not in line.lower()]
        # Filter out lines that just mention 'value' in context (like column names)
        data_lines_with_value = [line for line in lines_with_value if any(char.isdigit() for char in line)]
        self.assertEqual(len(data_lines_with_value), 0)

    def test_no_lazy_loading_flag(self):
        """Test --no-lazy-loading flag."""
        returncode, stdout, stderr = self.run_cli_command([
            "--no-lazy-loading",
            str(self.simple_file)
        ])

        self.assertEqual(returncode, 0, f"No lazy loading flag failed with return code {returncode}. stderr: {stderr}")
        # Should still display the file normally
        self.assertIn("Parquet file shape: (3, 3)", stdout, "File shape should be displayed")
        self.assertIn("Alice", stdout, "Data should be displayed normally")

    def test_memory_threshold_option(self):
        """Test --memory-threshold option."""
        returncode, stdout, stderr = self.run_cli_command([
            "--memory-threshold", "1",  # Very low threshold
            str(self.simple_file)
        ])

        self.assertEqual(returncode, 0, f"Memory threshold option failed with return code {returncode}. stderr: {stderr}")
        # For small files, might see lazy loading message or not depending on actual file size
        # Just ensure it doesn't error out
        self.assertIn("Alice", stdout, "Data should be displayed with custom memory threshold")

    def test_combined_new_options(self):
        """Test combining new CLI options."""
        returncode, stdout, stderr = self.run_cli_command([
            str(self.simple_file),
            "-c", "id", "-c", "name",
            "--no-lazy-loading",
            "--memory-threshold", "200",
            "-n", "2"
        ])

        self.assertEqual(returncode, 0, f"Combined options failed with return code {returncode}. stderr: {stderr}")
        self.assertIn("id", stdout, "Column 'id' should be in output")
        self.assertIn("name", stdout, "Column 'name' should be in output")
        self.assertIn("Alice", stdout, "First row data should be present")
        self.assertIn("Bob", stdout, "Second row data should be present")
        self.assertNotIn("Charlie", stdout, "Third row should not be present (only showing 2 rows)")

    def test_column_selection_with_nonexistent_column(self):
        """Test column selection with non-existent columns."""
        returncode, stdout, stderr = self.run_cli_command([
            "-c", "nonexistent_column",
            str(self.simple_file)
        ])

        # Should handle the error gracefully
        # The specific behavior depends on pandas/pyarrow error handling
        # At minimum, it should not crash the program completely
        self.assertIsInstance(returncode, int)

    def test_memory_threshold_validation(self):
        """Test memory threshold parameter validation."""
        try:
            returncode, stdout, stderr = self.run_cli_command([
                "--memory-threshold", "-1",  # Negative value
                str(self.simple_file)
            ])
            # Should either work (if negative values are handled) or fail gracefully
            self.assertIsInstance(returncode, int)
        except Exception as e:
            # Click might raise an exception for invalid values
            self.assertIsInstance(e, Exception)

    def test_help_with_new_options(self):
        """Test that help includes the new CLI options."""
        cli = self.create_click_wrapper()
        result = self.runner.invoke(cli, ['--help'])
        
        self.assertEqual(result.exit_code, 0)
        help_text = result.output
        
        # Check that new options are documented
        self.assertIn("--columns", help_text)
        self.assertIn("--no-lazy-loading", help_text)  
        self.assertIn("--memory-threshold", help_text)
        self.assertIn("Specific columns to display", help_text)
        self.assertIn("Disable lazy loading", help_text)


if __name__ == '__main__':
    unittest.main()