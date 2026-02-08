"""Pytest configuration: register custom markers and default filtering."""

import pytest


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
