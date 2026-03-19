"""
Pytest configuration and shared fixtures.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def base_dir() -> Path:
    """Return path to project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def ecg_dir(base_dir: Path) -> Path:
    """Return path to ECG directory."""
    return base_dir / "electrocardiograms"


@pytest.fixture
def fhir_dir(base_dir: Path) -> Path:
    """Return path to clinical-records directory."""
    return base_dir / "clinical-records"


@pytest.fixture
def output_dir(base_dir: Path) -> Path:
    """Return path to output directory."""
    return base_dir / "output"
