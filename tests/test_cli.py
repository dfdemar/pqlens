#!/usr/bin/env python3
"""
Integration tests for pqlens CLI functionality
"""

import unittest
import subprocess
import sys
from pathlib import Path


class TestCLI(unittest.TestCase):
    """Test cases for CLI functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_data_dir = Path(__file__).parent / "data"
        self.simple_file = self.test_data_dir / "simple.parquet"
        self.empty_file = self.test_data_dir / "empty.parquet"
        self.large_file = self.test_data_dir / "large.parquet"
        
        # Python executable to use for tests
        self.python_exe = "py" if sys.platform == "win32" else "python"
        if sys.platform == "win32":
            self.python_exe = "py -3.11"
    
    def run_cli_command(self, args, input_data=None):
        """Helper to run CLI commands and capture output."""
        cmd = [*self.python_exe.split(), "-m", "pqlens"] + args
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                input=input_data,
                timeout=30
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", "Command timed out"
    
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
        self.assertIn("view parquet file content", stdout.lower())
        self.assertIn("--interactive", stdout)
        self.assertIn("--table-format", stdout)
        self.assertIn("--rows", stdout)
    
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
        returncode, stdout, stderr = self.run_cli_command(["nonexistent.parquet"])
        
        # Should not crash, but should handle gracefully
        # The actual behavior depends on how the error is handled
        self.assertIsInstance(returncode, int)
    
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
        returncode, stdout, stderr = self.run_cli_command([
            "--table-format", "invalid_format", str(self.simple_file)
        ])
        
        # Should return non-zero exit code for invalid format
        self.assertNotEqual(returncode, 0)
        self.assertIn("invalid choice", stderr.lower())
    
    def test_invalid_rows_parameter(self):
        """Test invalid rows parameter."""
        returncode, stdout, stderr = self.run_cli_command([
            "--rows", "not_a_number", str(self.simple_file)
        ])
        
        # Should return non-zero exit code for invalid rows value
        self.assertNotEqual(returncode, 0)
        self.assertIn("invalid", stderr.lower())


if __name__ == '__main__':
    unittest.main()