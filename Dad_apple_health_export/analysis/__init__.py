"""
Health Data Analysis Package

A set of tools for parsing, cleaning, and analyzing Apple Health export data
including ECG waveforms and FHIR-formatted clinical records.
"""

from analysis.ecg_parser import (
    ECGParseError,
    aggregate_all_ecg,
    parse_ecg_full,
    parse_ecg_metadata,
    parse_ecg_waveform,
)

from analysis.fhir_parser import (
    FHIRParseError,
    aggregate_all_fhir,
    get_resource_type_counts,
    parse_fhir_resource,
)

from analysis.data_munging import (
    DataMungingError,
    add_age_column,
    bin_numeric_column,
    calculate_age_at_date,
    compute_summary_statistics,
    convert_datetime_columns,
    detect_outliers_iqr,
    export_to_csv,
    filter_by_date_range,
    flag_outliers,
    handle_missing_values,
    normalize_categorical,
    remove_duplicate_records,
    resample_time_series,
    standardize_column_names,
    validate_numeric_range,
)

from analysis.summary import (
    aggregate_by_month,
    aggregate_by_year,
    create_timeline_summary,
    create_vital_signs_summary,
    generate_data_quality_report,
    get_ecg_summary,
    get_fhir_summary,
)

__all__ = [
    # ECG Parser
    "ECGParseError",
    "aggregate_all_ecg",
    "parse_ecg_full",
    "parse_ecg_metadata",
    "parse_ecg_waveform",
    # FHIR Parser
    "FHIRParseError",
    "aggregate_all_fhir",
    "get_resource_type_counts",
    "parse_fhir_resource",
    # Data Munging
    "DataMungingError",
    "add_age_column",
    "bin_numeric_column",
    "calculate_age_at_date",
    "compute_summary_statistics",
    "convert_datetime_columns",
    "detect_outliers_iqr",
    "export_to_csv",
    "filter_by_date_range",
    "flag_outliers",
    "handle_missing_values",
    "normalize_categorical",
    "remove_duplicate_records",
    "resample_time_series",
    "standardize_column_names",
    "validate_numeric_range",
    # Summary
    "aggregate_by_month",
    "aggregate_by_year",
    "create_timeline_summary",
    "create_vital_signs_summary",
    "generate_data_quality_report",
    "get_ecg_summary",
    "get_fhir_summary",
]
