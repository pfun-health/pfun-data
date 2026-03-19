"""
Tests for ECG Parser Module

Run with: pytest tests/test_ecg_parser.py -v
Run single test: pytest tests/test_ecg_parser.py::test_parse_metadata -v
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

# Import from parent package
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.ecg_parser import (
    ECGParseError,
    extract_sample_rate_hz,
    extract_date_from_filename,
    parse_metadata_line,
    parse_ecg_metadata,
    parse_ecg_waveform,
    parse_ecg_full,
    parse_recorded_date,
)


# Test fixtures
@pytest.fixture
def ecg_file_path() -> Path:
    """Return path to a sample ECG file."""
    return Path(__file__).parent.parent / "electrocardiograms" / "ecg_2020-02-05.csv"


@pytest.fixture
def ecg_dir_path() -> Path:
    """Return path to ECG directory."""
    return Path(__file__).parent.parent / "electrocardiograms"


class TestExtractSampleRateHz:
    """Tests for extract_sample_rate_hz function."""

    def test_valid_sample_rate(self) -> None:
        """Test parsing valid sample rate string."""
        result = extract_sample_rate_hz("512.844 hertz")
        assert result == 512.844

    def test_integer_sample_rate(self) -> None:
        """Test parsing integer sample rate."""
        result = extract_sample_rate_hz("512 hertz")
        assert result == 512.0

    def test_missing_hertz(self) -> None:
        """Test parsing without 'hertz' suffix."""
        result = extract_sample_rate_hz("512.844")
        assert result is None

    def test_empty_string(self) -> None:
        """Test empty string returns None."""
        result = extract_sample_rate_hz("")
        assert result is None


class TestParseMetadataLine:
    """Tests for parse_metadata_line function."""

    def test_simple_line(self) -> None:
        """Test parsing simple key-value line."""
        field, value = parse_metadata_line("Name,John Doe")
        assert field == "Name"
        assert value == "John Doe"

    def test_quoted_value(self) -> None:
        """Test parsing line with quoted value."""
        field, value = parse_metadata_line('Date of Birth,"Feb 5, 1952"')
        assert field == "Date of Birth"
        assert "Feb 5, 1952" in value


class TestExtractDateFromFilename:
    """Tests for extract_date_from_filename function."""

    def test_standard_filename(self) -> None:
        """Test extracting date from standard filename."""
        result = extract_date_from_filename("ecg_2020-02-05.csv")
        assert result == datetime(2020, 2, 5)

    def test_filename_with_suffix(self) -> None:
        """Test extracting date from filename with suffix."""
        result = extract_date_from_filename("ecg_2020-09-05_1.csv")
        assert result == datetime(2020, 9, 5)

    def test_invalid_filename(self) -> None:
        """Test invalid filename returns None."""
        result = extract_date_from_filename("invalid.csv")
        assert result is None


class TestParseRecordedDate:
    """Tests for parse_recorded_date function."""

    def test_standard_format(self) -> None:
        """Test parsing standard date format with timezone."""
        result = parse_recorded_date("2020-02-05 09:14:40 -0400")
        assert result is not None
        assert result.year == 2020
        assert result.month == 2
        assert result.day == 5

    def test_date_only(self) -> None:
        """Test parsing date without time."""
        result = parse_recorded_date("2020-02-05")
        assert result is not None
        assert result.year == 2020


class TestParseECGMetadata:
    """Tests for parse_ecg_metadata function."""

    def test_parse_valid_file(self, ecg_file_path: Path) -> None:
        """Test parsing metadata from valid ECG file."""
        result = parse_ecg_metadata(ecg_file_path)

        assert result["name"] == "Criss Capps"
        assert result["classification"] == "Sinus Rhythm"
        assert result["sample_rate_hz"] == pytest.approx(512.844, rel=0.01)
        assert result["recorded_date"] is not None

    def test_raises_error_for_missing_file(self, tmp_path: Path) -> None:
        """Test that missing file raises ECGParseError."""
        with pytest.raises(ECGParseError):
            parse_ecg_metadata(tmp_path / "nonexistent.csv")


class TestParseECGWaveform:
    """Tests for parse_ecg_waveform function."""

    def test_parse_valid_file(self, ecg_file_path: Path) -> None:
        """Test parsing waveform from valid ECG file."""
        result = parse_ecg_waveform(ecg_file_path)

        assert len(result) > 0
        assert "sample_index" in result.columns
        assert "lead_i_mv" in result.columns
        assert "timestamp_ms" in result.columns
        assert "source_file" in result.columns

        # Check waveform values are numeric
        assert result["lead_i_mv"].dtype in [float, "float64"]

        # Check sample indices are sequential
        assert list(result["sample_index"]) == list(range(len(result)))

    def test_raises_error_for_missing_file(self, tmp_path: Path) -> None:
        """Test that missing file raises ECGParseError."""
        with pytest.raises(ECGParseError):
            parse_ecg_waveform(tmp_path / "nonexistent.csv")


class TestParseECGFull:
    """Tests for parse_ecg_full function."""

    def test_parse_valid_file(self, ecg_file_path: Path) -> None:
        """Test parsing complete ECG file."""
        result = parse_ecg_full(ecg_file_path)

        assert "metadata" in result
        assert "waveform" in result
        assert "filepath" in result

        # Verify metadata
        assert result["metadata"]["name"] == "Criss Capps"

        # Verify waveform
        assert len(result["waveform"]) > 0


# Integration tests
class TestAggregateAllECG:
    """Integration tests for aggregate_all_ecg function."""

    def test_aggregate_multiple_files(self, ecg_dir_path: Path) -> None:
        """Test aggregating multiple ECG files."""
        from analysis.ecg_parser import aggregate_all_ecg

        result = aggregate_all_ecg(ecg_dir_path)

        # Should have combined data from multiple files
        assert len(result) > 0
        assert result["source_file"].nunique() > 1

        # Check required columns
        assert "lead_i_mv" in result.columns
        assert "sample_index" in result.columns
        assert "recorded_date" in result.columns
