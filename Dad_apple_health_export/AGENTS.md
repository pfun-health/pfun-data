# AGENTS.md - Apple Health Data Export Analysis

## Overview

This repository contains Apple Health export data for analysis. The data is organized as follows:

- `clinical-records/` - FHIR R4 formatted JSON clinical records
- `electrocardiograms/` - ECG waveform data in CSV format
- `workout-routes/` - GPS route data from workouts
- `export.xml` - Full health data export in XML format
- `export_cda.xml` - CDA (Clinical Document Architecture) format export

## Repository Context

- **Type**: Health data analysis project
- **Data Format**: FHIR R4 JSON, CSV, XML
- **Owner**: Dad's Apple Health data (anonymized for analysis)
- **Date Range**: Data spans from ~2020 to present

## Build/Test Commands

```bash
# Python environment setup
python -m venv venv && source venv/bin/activate
pip install pandas numpy matplotlib seaborn jupyter

# Run analysis pipeline
python scripts/analyze_health_data.py

# Run Jupyter notebook
jupyter notebook notebooks/health_data_exploration.ipynb

# Run a single test (using pytest)
pytest tests/ -v

# Run specific test file
pytest tests/test_ecg_parser.py -v

# Run single test function
pytest tests/test_ecg_parser.py::test_parse_metadata -v
```

## Code Style Guidelines

### General Principles

1. **Never modify original data files** - All transformations create new output files
2. **Preserve data provenance** - Track source file and transformation logic
3. **Handle missing values explicitly** - Use NaN, None, or appropriate sentinel values
4. **Type annotations required** - All Python functions must have type hints
5. **Documentation mandatory** - Docstrings for all functions handling health data

### Project Structure

```
project/
├── analysis/                    # Core analysis modules
│   ├── __init__.py             # Package exports
│   ├── ecg_parser.py           # ECG CSV parsing
│   ├── fhir_parser.py         # FHIR JSON parsing
│   ├── data_munging.py        # Data cleaning utilities
│   └── summary.py             # Summary statistics
├── clinical-records/            # Original FHIR data (read-only)
├── electrocardiograms/          # Original ECG files
├── notebooks/                   # Jupyter notebooks
├── scripts/                    # Entry point scripts
│   └── analyze_health_data.py
├── output/                     # Generated reports (gitignored)
└── AGENTS.md                   # This file
```

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Files | snake_case | `ecg_parser.py` |
| Classes | PascalCase | `ECGParseError` |
| Functions | snake_case | `parse_ecg_waveform()` |
| Variables | snake_case | `patient_id`, `ecg_readings` |
| Constants | UPPER_SNAKE | `MAX_HEART_RATE`, `SAMPLE_RATE_HZ` |
| JSON keys | FHIR standard | `resourceType`, `status`, `effectiveDateTime` |

### Imports (Python)

```python
# Standard library first
from __future__ import annotations
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# Third-party libraries
import numpy as np
import pandas as pd

# Local modules
from .data_munging import handle_missing_values
```

### Function Template

```python
def parse_ecg_metadata(filepath: Path) -> dict:
    """Parse metadata header from ECG CSV file.
    
    Args:
        filepath: Path to ECG CSV file
        
    Returns:
        Dictionary containing parsed metadata fields
        
    Raises:
        ECGParseError: If file cannot be read or has invalid structure
    """
    # Implementation
    pass
```

### Error Handling

```python
# Specific exception handling for data operations
class HealthDataError(Exception):
    """Base exception for health data processing."""
    pass

class ECGParseError(HealthDataError):
    """Raised when ECG file parsing fails."""
    pass

class FHIRParseError(HealthDataError):
    """Raised when FHIR resource parsing fails."""
    pass

def parse_clinical_record(filepath: Path) -> dict:
    """Parse a clinical record with proper error handling."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            record = json.load(f)
    except json.JSONDecodeError as e:
        raise FHIRParseError(f"Invalid JSON in {filepath}: {e}")
    
    if "resourceType" not in record:
        raise FHIRParseError(f"Missing resourceType in {filepath}")
    
    return record
```

## FHIR Resource Types Present

The clinical-records directory contains these FHIR resource types:
- `Condition-*` - Medical conditions/diagnoses
- `Observation-*` - Vital signs, lab results (may contain components like BP systolic/diastolic)
- `DiagnosticReport-*` - Test reports (lab panels)
- `Immunization-*` - Vaccination records
- `AllergyIntolerance-*` - Allergy information
- `DocumentReference-*` - Document references (scanned documents)
- `Procedure-*` - Medical procedures (including COVID vaccines)

## ECG Data Format

Apple Watch ECG CSV files have a specific format:
```
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
```

Key parsing notes:
- Metadata in first ~8 lines
- Empty lines separate sections
- Waveform data starts after "Lead,Lead I" and "Unit,µV" headers
- Only Lead I is recorded (single lead)
- Sample rate: ~512 Hz (Apple Watch)

## Key Module Functions

### ECG Parser (`analysis/ecg_parser.py`)
- `parse_ecg_metadata()` - Parse header metadata
- `parse_ecg_waveform()` - Parse waveform data
- `parse_ecg_full()` - Parse complete file
- `aggregate_all_ecg()` - Aggregate all ECG files

### FHIR Parser (`analysis/fhir_parser.py`)
- `load_fhir_resource()` - Load single FHIR JSON
- `parse_fhir_resource()` - Parse by resourceType
- `parse_observation()` - Handle observations (with components)
- `aggregate_all_fhir()` - Aggregate all FHIR files
- `get_resource_type_counts()` - Count by type

### Data Munging (`analysis/data_munging.py`)
- `convert_datetime_columns()` - Ensure datetime type
- `handle_missing_values()` - Handle NaN with strategy
- `standardize_column_names()` - Rename columns
- `add_age_column()` - Calculate ages
- `validate_numeric_range()` - Check value bounds
- `detect_outliers_iqr()` - IQR-based outlier detection
- `filter_by_date_range()` - Temporal filtering

### Summary (`analysis/summary.py`)
- `get_ecg_summary()` - ECG statistics
- `get_fhir_summary()` - FHIR statistics
- `generate_data_quality_report()` - Completeness metrics
- `create_timeline_summary()` - Events over time

## Privacy Considerations

- This is real health data - handle with care
- Do not commit outputs containing PHI
- Use anonymized/de-identified data for sharing
- Audit log all data access and transformations
- The `output/` directory is gitignored
