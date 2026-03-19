"""
ECG (Electrocardiogram) Data Parsing Module

Parses Apple Watch ECG CSV exports into structured formats suitable for analysis.
The ECG files contain metadata headers followed by waveform data for Lead I.

Example file structure:
    Name,Criss Capps
    Date of Birth,"Feb 5, 1952"
    Recorded Date,2020-02-05 09:14:40 -0400
    Classification,Sinus Rhythm
    Symptoms,
    Software Version,1.51
    Device,"Watch5,4"
    Sample Rate,512.844 hertz

    Lead,Lead I
    Unit,µV

    -48.673
    -59.681
    ...
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


class ECGParseError(Exception):
    """Raised when ECG file parsing fails."""

    pass


METADATA_FIELDS = [
    "name",
    "date_of_birth",
    "recorded_date",
    "classification",
    "symptoms",
    "software_version",
    "device",
    "sample_rate_hz",
]

SAMPLE_RATE_PATTERN = re.compile(r"([\d.]+)\s*hertz")


def parse_metadata_line(line: str) -> tuple[str, str]:
    """Parse a single metadata line from ECG header.

    Args:
        line: Raw line from CSV header (e.g., "Name,Criss Capps")

    Returns:
        Tuple of (field_name, value)
    """
    if "," not in line:
        return "", ""

    # Handle quoted values that may contain commas
    if line.startswith('"'):
        parts = line.split('"', 2)
        field = parts[1] if len(parts) > 1 else ""
        value = parts[2].lstrip(", ") if len(parts) > 2 else ""
    else:
        parts = line.split(",", 1)
        field = parts[0].strip().lower().replace(" ", "_")
        value = parts[1].strip().lstrip('"').rstrip('"') if len(parts) > 1 else ""

    return field, value


def extract_sample_rate_hz(sample_rate_str: str) -> Optional[float]:
    """Extract numeric sample rate from string.

    Args:
        sample_rate_str: Raw sample rate string (e.g., "512.844 hertz")

    Returns:
        Sample rate as float, or None if parsing fails
    """
    match = SAMPLE_RATE_PATTERN.search(sample_rate_str)
    if match:
        return float(match.group(1))
    return None


def parse_recorded_date(date_str: str) -> Optional[datetime]:
    """Parse ECG recorded date string.

    Args:
        date_str: Date string (e.g., "2020-02-05 09:14:40 -0400")

    Returns:
        Parsed datetime object, or None if parsing fails
    """
    formats = [
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue

    return None


def parse_ecg_metadata(filepath: Path) -> dict:
    """Parse metadata header from ECG CSV file.

    Args:
        filepath: Path to ECG CSV file

    Returns:
        Dictionary containing parsed metadata fields

    Raises:
        ECGParseError: If file cannot be read or has invalid structure
    """
    metadata: dict[str, str | float | datetime | None] = {
        k: None for k in METADATA_FIELDS
    }
    metadata["sample_rate_hz"] = None  # Override with float

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError as e:
        raise ECGParseError(f"Cannot read ECG file {filepath}: {e}")

    # Parse metadata lines (lines 1-8 in the file)
    for i, line in enumerate(lines[:8]):
        line = line.strip()
        if not line:
            continue

        field, value = parse_metadata_line(line)

        # Map to standardized field names
        field_mapping = {
            "name": "name",
            "date_of_birth": "date_of_birth",
            "recorded_date": "recorded_date",
            "classification": "classification",
            "symptoms": "symptoms",
            "software_version": "software_version",
            "device": "device",
            "sample_rate": "sample_rate",
        }

        if field in field_mapping:
            std_field = field_mapping[field]

            # Special parsing for certain fields
            if std_field == "sample_rate":
                metadata["sample_rate_hz"] = extract_sample_rate_hz(value)
            elif std_field == "recorded_date":
                metadata[std_field] = parse_recorded_date(value)
            else:
                metadata[std_field] = value if value else None

    # Extract filename-based date as fallback
    filename_date = extract_date_from_filename(filepath.name)
    if metadata["recorded_date"] is None and filename_date:
        metadata["recorded_date"] = filename_date

    return metadata


def extract_date_from_filename(filename: str) -> Optional[datetime]:
    """Extract date from ECG filename.

    Filenames follow pattern: ecg_YYYY-MM-DD[_*].csv

    Args:
        filename: ECG filename

    Returns:
        Extracted date, or None if no date found
    """
    date_pattern = re.compile(r"ecg_(\d{4})-(\d{2})-(\d{2})")
    match = date_pattern.search(filename)

    if match:
        year, month, day = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            return None

    return None


def find_data_start_line(lines: list[str]) -> int:
    """Find the line number where waveform data starts.

    Args:
        lines: List of lines from ECG file

    Returns:
        Line index where numeric data begins
    """
    for i, line in enumerate(lines):
        stripped = line.strip()
        # Skip metadata header, empty lines, and lead/unit headers
        if not stripped or stripped.startswith(",") or stripped.startswith("Lead"):
            continue

        # Check if line contains numeric data (starts with digit or minus)
        try:
            float(stripped)
            return i
        except ValueError:
            continue

    return -1


def parse_ecg_waveform(filepath: Path) -> pd.DataFrame:
    """Parse waveform data from ECG CSV file.

    Args:
        filepath: Path to ECG CSV file

    Returns:
        DataFrame with columns:
            - sample_index: Sequential sample number
            - lead_i_mv: Lead I voltage in millivolts
            - timestamp_ms: Milliseconds from start
            - source_file: Original filename
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError as e:
        raise ECGParseError(f"Cannot read ECG file {filepath}: {e}")

    # Get metadata for sample rate
    metadata = parse_ecg_metadata(filepath)
    sample_rate = metadata.get("sample_rate_hz") or 512.0  # Default Apple Watch rate

    # Find where data starts
    data_start = find_data_start_line(lines)

    if data_start < 0:
        raise ECGParseError(f"No waveform data found in {filepath}")

    # Parse waveform values
    waveform_values: list[float] = []
    for line in lines[data_start:]:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            waveform_values.append(float(stripped))
        except ValueError:
            # Stop at non-numeric data (end of waveform section)
            break

    # Create DataFrame
    n_samples = len(waveform_values)
    timestamps_ms = [i * 1000.0 / sample_rate for i in range(n_samples)]

    df = pd.DataFrame(
        {
            "sample_index": range(n_samples),
            "lead_i_mv": waveform_values,
            "timestamp_ms": timestamps_ms,
            "source_file": filepath.name,
        }
    )

    return df


def parse_ecg_full(filepath: Path) -> dict:
    """Parse complete ECG file including metadata and waveform.

    Args:
        filepath: Path to ECG CSV file

    Returns:
        Dictionary with keys:
            - metadata: Parsed metadata dictionary
            - waveform: DataFrame with waveform data
            - filepath: Original file path
    """
    metadata = parse_ecg_metadata(filepath)
    waveform = parse_ecg_waveform(filepath)

    return {
        "metadata": metadata,
        "waveform": waveform,
        "filepath": str(filepath),
    }


def aggregate_all_ecg(directory: Path) -> pd.DataFrame:
    """Aggregate all ECG files from a directory into a single DataFrame.

    Args:
        directory: Directory containing ECG CSV files

    Returns:
        Combined DataFrame with all ECG data, including metadata columns
    """
    ecg_files = sorted(directory.glob("ecg_*.csv"))

    if not ecg_files:
        raise ECGParseError(f"No ECG files found in {directory}")

    all_records: list[dict] = []
    all_waveforms: list[pd.DataFrame] = []

    for filepath in ecg_files:
        try:
            parsed = parse_ecg_full(filepath)

            # Add metadata to waveform
            waveform = parsed["waveform"]
            for key, value in parsed["metadata"].items():
                waveform[key] = value

            all_waveforms.append(waveform)

        except ECGParseError as e:
            print(f"Warning: Skipping {filepath.name}: {e}")
            continue

    if not all_waveforms:
        raise ECGParseError("No valid ECG files could be parsed")

    return pd.concat(all_waveforms, ignore_index=True)
