"""
Test PMDA Scaffolding and Structure.
"""

import importlib
import sys
from types import ModuleType

import pytest


def test_pmda_import() -> None:
    """Test that the PMDA package can be imported."""
    import coreason_etl_pmda

    assert coreason_etl_pmda is not None
    assert coreason_etl_pmda.__doc__ is not None


def test_pmda_modules_import() -> None:
    """Test that all core PMDA modules can be imported and have docstrings."""
    modules = ["crawler", "downloader", "parser", "models", "pipeline"]
    for module_name in modules:
        full_name = f"coreason_etl_pmda.{module_name}"
        module = importlib.import_module(full_name)
        assert isinstance(module, ModuleType)
        assert module.__doc__ is not None
        assert len(module.__doc__.strip()) > 0


def test_dependencies_available() -> None:
    """Test that core dependencies for PMDA are installed."""
    import dlt
    import polars

    assert dlt is not None
    assert polars is not None


def test_namespace_isolation() -> None:
    """
    Test that PMDA and EUCTR namespaces are isolated.
    Importing PMDA should not expose EUCTR modules in the PMDA namespace.
    """
    import coreason_etl_pmda

    # Ensure EUCTR specific attributes are not in PMDA
    assert not hasattr(coreason_etl_pmda, "run_bronze")
    assert not hasattr(coreason_etl_pmda, "run_silver")


def test_missing_submodule_raises_error() -> None:
    """Test that importing a non-existent submodule raises ModuleNotFoundError."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("coreason_etl_pmda.non_existent_module")


def test_package_structure_integrity() -> None:
    """
    Verify that the package structure on the filesystem matches expectations.
    """
    import os
    import coreason_etl_pmda

    package_path = os.path.dirname(coreason_etl_pmda.__file__)  # type: ignore[type-var]

    expected_files = {
        "__init__.py",
        "crawler.py",
        "downloader.py",
        "parser.py",
        "models.py",
        "pipeline.py"
    }

    actual_files = set(os.listdir(package_path))

    # Check that all expected files are present (ignoring __pycache__)
    missing_files = expected_files - actual_files
    assert not missing_files, f"Missing files in package: {missing_files}"
