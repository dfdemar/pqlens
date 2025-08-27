#!/usr/bin/env python3
"""
Tests for package structure and imports
"""

import unittest


class TestPackage(unittest.TestCase):
    """Test cases for package structure."""

    def test_package_import(self):
        """Test that pqlens package can be imported."""
        import pqlens
        self.assertTrue(hasattr(pqlens, '__version__'))

    def test_version_attribute(self):
        """Test that version attribute is correct."""
        import pqlens
        self.assertEqual(pqlens.__version__, "0.1.0")
        self.assertIsInstance(pqlens.__version__, str)

    def test_public_api_exports(self):
        """Test that public API functions are exported."""
        import pqlens

        # Check that main functions are available
        self.assertTrue(hasattr(pqlens, 'view_parquet_file'))
        self.assertTrue(hasattr(pqlens, 'display_table'))
        self.assertTrue(hasattr(pqlens, 'paged_display'))

    def test_submodule_imports(self):
        """Test that submodules can be imported."""
        # Test main module
        from pqlens import main
        self.assertTrue(hasattr(main, 'view_parquet_file'))
        self.assertTrue(hasattr(main, 'display_table'))
        self.assertTrue(hasattr(main, 'paged_display'))
        self.assertTrue(hasattr(main, 'main'))

        # Test CLI module
        from pqlens import cli
        self.assertTrue(hasattr(cli, 'main'))

    def test_main_module_importable(self):
        """Test that __main__ module can be imported."""
        try:
            from pqlens import __main__
            # Should not raise an exception
        except ImportError:
            self.fail("__main__ module should be importable")

    def test_cli_main_callable(self):
        """Test that CLI main function is callable."""
        from pqlens.cli import main
        self.assertTrue(callable(main))

    def test_main_module_main_callable(self):
        """Test that main module's main function is callable."""
        from pqlens.main import main
        self.assertTrue(callable(main))

    def test_view_parquet_file_callable(self):
        """Test that view_parquet_file function is callable."""
        from pqlens import view_parquet_file
        self.assertTrue(callable(view_parquet_file))

    def test_display_table_callable(self):
        """Test that display_table function is callable."""
        from pqlens import display_table
        self.assertTrue(callable(display_table))

    def test_paged_display_callable(self):
        """Test that paged_display function is callable."""
        from pqlens import paged_display
        self.assertTrue(callable(paged_display))


if __name__ == '__main__':
    unittest.main()
