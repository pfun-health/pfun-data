"""
Summary Statistics and Aggregation Module

Provides functions for computing summary statistics, aggregations,
and overview statistics for health data analysis.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


def get_ecg_summary(ecg_df: pd.DataFrame) -> dict:
    """Generate summary statistics for ECG data.

    Args:
        ecg_df: DataFrame with ECG waveform data

    Returns:
        Dictionary with summary statistics
    """
    summary: dict[str, int | float | str | datetime | None] = {}

    # Basic counts
    summary["total_recordings"] = ecg_df["source_file"].nunique()
    summary["total_samples"] = len(ecg_df)

    # Date range
    if "recorded_date" in ecg_df.columns:
        dates = ecg_df["recorded_date"].dropna()
        if len(dates) > 0:
            summary["earliest_recording"] = dates.min()
            summary["latest_recording"] = dates.max()

    # Classifications
    if "classification" in ecg_df.columns:
        summary["classifications"] = ecg_df["classification"].value_counts().to_dict()

    # Lead I statistics
    if "lead_i_mv" in ecg_df.columns:
        lead_stats = ecg_df["lead_i_mv"].describe()
        summary["lead_i_stats"] = lead_stats.to_dict()

        # Peak-to-peak amplitude
        summary["lead_i_peak_to_peak"] = (
            ecg_df["lead_i_mv"].max() - ecg_df["lead_i_mv"].min()
        )

    # Sample rate info
    if "sample_rate_hz" in ecg_df.columns:
        sample_rates = ecg_df["sample_rate_hz"].dropna().unique()
        summary["sample_rates_hz"] = sample_rates.tolist()

    # Duration per recording
    if "timestamp_ms" in ecg_df.columns and "source_file" in ecg_df.columns:
        durations = (
            ecg_df.groupby("source_file")["timestamp_ms"].max() / 1000
        )  # Convert to seconds
        summary["recording_durations_sec"] = durations.to_dict()
        summary["avg_recording_duration_sec"] = durations.mean()

    return summary


def get_fhir_summary(fhir_df: pd.DataFrame) -> dict:
    """Generate summary statistics for FHIR clinical records.

    Args:
        fhir_df: DataFrame with parsed FHIR records

    Returns:
        Dictionary with summary statistics
    """
    summary: dict[str, int | float | datetime | None | dict] = {}

    # Resource type counts
    if "resource_type" in fhir_df.columns:
        summary["resource_type_counts"] = (
            fhir_df["resource_type"].value_counts().to_dict()
        )
        summary["total_records"] = len(fhir_df)

    # Date ranges by resource type
    datetime_columns = [
        "effective_datetime",
        "observation_datetime",
        "onset_datetime",
        "condition_onset_datetime",
        "occurrence_datetime",
        "event_datetime",
        "recorded_date",
        "documentation_datetime",
        "issued_datetime",
        "report_issued_datetime",
        "date",
        "performed_start",
    ]

    for col in datetime_columns:
        if col in fhir_df.columns:
            dates = pd.to_datetime(fhir_df[col], errors="coerce").dropna()
            if len(dates) > 0:
                summary[f"earliest_{col}"] = dates.min()
                summary[f"latest_{col}"] = dates.max()
                break

    # Condition summaries
    if "resource_type" in fhir_df.columns:
        conditions = fhir_df[fhir_df["resource_type"] == "Condition"]
        if len(conditions) > 0:
            summary["condition_count"] = len(conditions)

            if "clinical_status" in conditions.columns:
                summary["condition_status_counts"] = (
                    conditions["clinical_status"].value_counts().to_dict()
                )

            if "condition_display" in conditions.columns:
                top_conditions = (
                    conditions["condition_display"].value_counts().head(10).to_dict()
                )
                summary["top_conditions"] = top_conditions

    # Observation summaries
    observations = fhir_df[fhir_df["resource_type"] == "Observation"]
    if len(observations) > 0:
        summary["observation_count"] = len(observations)

        if "category" in observations.columns:
            summary["observation_categories"] = (
                observations["category"].value_counts().to_dict()
            )

        if "component_display" in observations.columns:
            top_components = (
                observations["component_display"].value_counts().head(10).to_dict()
            )
            summary["top_observation_types"] = top_components

    # Immunization summaries
    immunizations = fhir_df[fhir_df["resource_type"] == "Immunization"]
    if len(immunizations) > 0:
        summary["immunization_count"] = len(immunizations)

        if "vaccine_display" in immunizations.columns:
            summary["vaccines_given"] = (
                immunizations["vaccine_display"].value_counts().to_dict()
            )

    # Allergy summaries
    allergies = fhir_df[fhir_df["resource_type"] == "AllergyIntolerance"]
    if len(allergies) > 0:
        summary["allergy_count"] = len(allergies)

        if "allergy_display" in allergies.columns:
            summary["allergies"] = allergies["allergy_display"].value_counts().to_dict()

    # Procedure summaries
    procedures = fhir_df[fhir_df["resource_type"] == "Procedure"]
    if len(procedures) > 0:
        summary["procedure_count"] = len(procedures)

        if "procedure_display" in procedures.columns:
            summary["procedures"] = (
                procedures["procedure_display"].value_counts().to_dict()
            )

    return summary


def aggregate_by_year(
    df: pd.DataFrame,
    datetime_column: str,
    value_column: str,
    aggregation: str = "count",
) -> pd.DataFrame:
    """Aggregate data by year.

    Args:
        df: Input DataFrame
        datetime_column: Column containing datetime values
        value_column: Column to aggregate
        aggregation: Aggregation method ('count', 'sum', 'mean', etc.)

    Returns:
        DataFrame aggregated by year
    """
    df = df.copy()
    df["year"] = pd.to_datetime(df[datetime_column], errors="coerce").dt.year

    df = df.dropna(subset=["year"])

    if aggregation == "count":
        result = df.groupby("year").size().reset_index(name=value_column)
    else:
        result = df.groupby("year")[value_column].agg(aggregation).reset_index()

    return result


def aggregate_by_month(
    df: pd.DataFrame,
    datetime_column: str,
    value_column: str,
    aggregation: str = "count",
) -> pd.DataFrame:
    """Aggregate data by year-month.

    Args:
        df: Input DataFrame
        datetime_column: Column containing datetime values
        value_column: Column to aggregate
        aggregation: Aggregation method ('count', 'sum', 'mean', etc.)

    Returns:
        DataFrame aggregated by year-month
    """
    df = df.copy()
    dates = pd.to_datetime(df[datetime_column], errors="coerce")
    df["year_month"] = dates.dt.to_period("M")

    df = df.dropna(subset=["year_month"])

    if aggregation == "count":
        result = df.groupby("year_month").size().reset_index(name=value_column)
    else:
        result = df.groupby("year_month")[value_column].agg(aggregation).reset_index()

    result["year_month"] = result["year_month"].astype(str)
    return result


def create_vital_signs_summary(
    observations_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create summary of vital sign measurements.

    Args:
        observations_df: DataFrame with Observation records

    Returns:
        DataFrame with vital signs summary statistics
    """
    # Filter to vital signs category
    vital_signs = observations_df[observations_df.get("category", "") == "Vital Signs"]

    if len(vital_signs) == 0:
        return pd.DataFrame()

    # Group by component type and compute statistics
    if "component_display" in vital_signs.columns and "value" in vital_signs.columns:
        summary = (
            vital_signs.groupby("component_display")["value"]
            .agg(["count", "mean", "min", "max", "std"])
            .reset_index()
        )
        summary.columns = ["vital_type", "count", "mean", "min", "max", "std"]
        return summary

    return pd.DataFrame()


def create_timeline_summary(
    fhir_df: pd.DataFrame,
    datetime_column: str,
) -> pd.DataFrame:
    """Create a timeline summary of events by month.

    Args:
        fhir_df: DataFrame with FHIR records
        datetime_column: Column containing datetime values

    Returns:
        DataFrame with monthly counts by resource type
    """
    df = fhir_df.copy()
    dates = pd.to_datetime(df[datetime_column], errors="coerce")
    df["year_month"] = dates.dt.to_period("M")

    df = df.dropna(subset=["year_month", "resource_type"])

    # Create pivot table
    timeline = pd.crosstab(df["year_month"], df["resource_type"])
    timeline = timeline.reset_index()
    timeline["year_month"] = timeline["year_month"].astype(str)

    return timeline


def generate_data_quality_report(
    df: pd.DataFrame,
) -> dict:
    """Generate data quality report for a DataFrame.

    Args:
        df: Input DataFrame

    Returns:
        Dictionary with data quality metrics
    """
    report: dict[str, int | float | dict] = {}

    # Basic info
    report["total_rows"] = len(df)
    report["total_columns"] = len(df.columns)

    # Missing values
    missing = df.isnull().sum()
    report["missing_values"] = missing[missing > 0].to_dict()
    report["missing_pct"] = {
        col: round((count / len(df)) * 100, 2)
        for col, count in missing.items()
        if count > 0
    }

    # Completeness score
    total_cells = len(df) * len(df.columns)
    filled_cells = df.notna().sum().sum()
    report["completeness_score"] = round((filled_cells / total_cells) * 100, 2)

    # Duplicate rows
    report["duplicate_rows"] = df.duplicated().sum()

    # Column-specific quality
    report["column_types"] = df.dtypes.astype(str).to_dict()

    # Numeric columns - outliers
    numeric_cols = df.select_dtypes(include=["number"]).columns
    report["numeric_summary"] = {}
    for col in numeric_cols:
        stats = df[col].describe()
        report["numeric_summary"][col] = {
            "count": int(stats.get("count", 0)),
            "mean": round(stats.get("mean", 0), 4)
            if pd.notna(stats.get("mean"))
            else None,
            "std": round(stats.get("std", 0), 4)
            if pd.notna(stats.get("std"))
            else None,
            "min": stats.get("min"),
            "max": stats.get("max"),
        }

    return report
