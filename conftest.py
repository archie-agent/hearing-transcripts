"""Pytest configuration: register custom markers, default filtering, and path setup."""

import sys
from pathlib import Path

import pytest

# Add project root to sys.path so tests can import modules without per-file boilerplate.
_PROJECT_ROOT = str(Path(__file__).parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: hits real network endpoints (deselected by default, run with -m integration)",
    )


def pytest_collection_modifyitems(config, items):
    # If the user explicitly requested integration tests via -m, don't touch anything.
    if config.getoption("-m"):
        return
    skip_integration = pytest.mark.skip(reason="integration tests require -m integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
