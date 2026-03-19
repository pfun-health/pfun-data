"""
Data Munging and Transformation Utilities

Provides common data cleaning, transformation, and validation functions
for health data analysis. These functions ensure data quality and
consistency across different source formats.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd


class DataMungingError(Exception):
    """Raised when data transformation operations fail."""

    pass


# Standard column renames for consistency
ECG_COLUMN_RENAMES: dict[str, str] = {
    "lead_i_mv": "lead_i_microvolts",
    "timestamp_ms": "time_milliseconds",
    "recorded_date": "ecg_datetime",
    "date_of_birth": "patient_dob",
    "sample_rate_hz": "sampling_rate_hz",
}

FHIR_COLUMN_RENAMES: dict[str, str] = {
    "effective_datetime": "observation_datetime",
    "onset_datetime": "condition_onset_datetime",
    "occurrence_datetime": "event_datetime",
    "recorded_date": "documentation_datetime",
    "issued_datetime": "report_issued_datetime",
}


def standardize_column_names(
    df: pd.DataFrame, rename_mapping: dict[str, str]
) -> pd.DataFrame:
    """Standardize DataFrame column names using provided mapping.

    Args:
        df: Input DataFrame
        rename_mapping: Dictionary mapping old names to new names

    Returns:
        DataFrame with standardized column names
    """
    df = df.copy()
    existing_renames = {
        old: new for old, new in rename_mapping.items() if old in df.columns
    }
    return df.rename(columns=existing_renames)


def convert_datetime_columns(
    df: pd.DataFrame, columns: list[str], infer_first: bool = True
) -> pd.DataFrame:
    """Convert specified columns to datetime type.

    Args:
        df: Input DataFrame
        columns: List of column names to convert
        infer_first: If True, infer format before forcing conversion

    Returns:
        DataFrame with converted datetime columns
    """
    df = df.copy()

    for col in columns:
        if col in df.columns:
            if df[col].dtype == object or infer_first:
                try:
                    df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
                except Exception:
                    pass  # Keep original type if conversion fails

    return df


def handle_missing_values(
    df: pd.DataFrame,
    strategy: str = "preserve",
    columns: Optional[list[str]] = None,
    fill_value: Any = None,
) -> pd.DataFrame:
    """Handle missing values in DataFrame according to strategy.

    Args:
        df: Input DataFrame
        strategy: One of 'preserve', 'drop', 'fill', 'flag'
        columns: Columns to apply strategy to (None = all)
        fill_value: Value to use for 'fill' strategy

    Returns:
        DataFrame with handled missing values
    """
    df = df.copy()
    target_columns = columns if columns else df.columns.tolist()

    if strategy == "drop":
        return df.dropna(subset=target_columns)

    elif strategy == "fill":
        if fill_value is not None:
            df[target_columns] = df[target_columns].fillna(fill_value)
        return df

    elif strategy == "flag":
        for col in target_columns:
            if col in df.columns:
                df[f"{col}_was_missing"] = df[col].isna()
        return df

    else:  # preserve
        return df


def remove_duplicate_records(
    df: pd.DataFrame,
    subset: Optional[list[str]] = None,
    keep: str = "first",
) -> pd.DataFrame:
    """Remove duplicate records from DataFrame.

    Args:
        df: Input DataFrame
        subset: Columns to consider for duplicates (None = all)
        keep: Which duplicate to keep ('first', 'last', False)

    Returns:
        DataFrame with duplicates removed
    """
    return df.drop_duplicates(subset=subset, keep=keep)


def normalize_categorical(
    df: pd.DataFrame,
    columns: list[str],
    lowercase: bool = True,
    strip_whitespace: bool = True,
) -> pd.DataFrame:
    """Normalize categorical/string columns.

    Args:
        df: Input DataFrame
        columns: Columns to normalize
        lowercase: Convert to lowercase
        strip_whitespace: Remove leading/trailing whitespace

    Returns:
        DataFrame with normalized columns
    """
    df = df.copy()

    for col in columns:
        if col in df.columns and df[col].dtype == object:
            if strip_whitespace:
                df[col] = df[col].str.strip()
            if lowercase:
                df[col] = df[col].str.lower()

    return df


def validate_numeric_range(
    df: pd.DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    set_to_na: bool = True,
) -> pd.DataFrame:
    """Validate and optionally flag/correct out-of-range numeric values.

    Args:
        df: Input DataFrame
        column: Column to validate
        min_value: Minimum valid value
        max_value: Maximum valid value
        set_to_na: If True, set out-of-range values to NaN

    Returns:
        DataFrame with validated values
    """
    df = df.copy()

    if column not in df.columns:
        return df

    mask = pd.Series(True, index=df.index)

    if min_value is not None:
        mask = mask & (df[column] >= min_value)

    if max_value is not None:
        mask = mask & (df[column] <= max_value)

    if set_to_na:
        df.loc[~mask, column] = np.nan
    else:
        # Add validation flag column
        df[f"{column}_out_of_range"] = ~mask

    return df


def calculate_age_at_date(
    dob: datetime,
    reference_date: datetime,
) -> Optional[float]:
    """Calculate age in years from date of birth to reference date.

    Args:
        dob: Date of birth
        reference_date: Reference date for age calculation

    Returns:
        Age in years (float), or None if calculation fails
    """
    if pd.isna(dob) or pd.isna(reference_date):
        return None

    try:
        age = (reference_date - dob).days / 365.25
        return round(age, 2)
    except (TypeError, AttributeError):
        return None


def add_age_column(
    df: pd.DataFrame,
    dob_column: str,
    reference_column: str,
    new_column: str = "age_years",
) -> pd.DataFrame:
    """Add age calculation column to DataFrame.

    Args:
        df: Input DataFrame
        dob_column: Date of birth column name
        reference_column: Reference date column name
        new_column: Name for new age column

    Returns:
        DataFrame with added age column
    """
    df = df.copy()

    # Ensure datetime types
    df = convert_datetime_columns(df, [dob_column, reference_column])

    df[new_column] = df.apply(
        lambda row: calculate_age_at_date(
            row.get(dob_column), row.get(reference_column)
        ),
        axis=1,
    )

    return df


def extract_year_from_datetime(
    df: pd.DataFrame,
    datetime_column: str,
    new_column: str = "year",
) -> pd.DataFrame:
    """Extract year from datetime column.

    Args:
        df: Input DataFrame
        datetime_column: Name of datetime column
        new_column: Name for new year column

    Returns:
        DataFrame with added year column
    """
    df = df.copy()

    if datetime_column in df.columns:
        df = convert_datetime_columns(df, [datetime_column])
        df[new_column] = df[datetime_column].dt.year

    return df


def bin_numeric_column(
    df: pd.DataFrame,
    column: str,
    bins: list[float],
    labels: Optional[list[str]] = None,
    include_lowest: bool = True,
) -> pd.DataFrame:
    """Bin a numeric column into categorical bins.

    Args:
        df: Input DataFrame
        column: Column to bin
        bins: Bin edge values
        labels: Labels for bins (None = use bin edges)
        include_lowest: Include lowest edge in first bin

    Returns:
        DataFrame with added binned column
    """
    df = df.copy()

    if column not in df.columns:
        return df

    binned_col_name = f"{column}_binned"

    if labels is None:
        labels = [f"{bins[i]}-{bins[i + 1]}" for i in range(len(bins) - 1)]

    df[binned_col_name] = pd.cut(
        df[column],
        bins=bins,
        labels=labels,
        include_lowest=include_lowest,
        ordered=True,
    )

    return df


def filter_by_date_range(
    df: pd.DataFrame,
    datetime_column: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Filter DataFrame by date range.

    Args:
        df: Input DataFrame
        datetime_column: Name of datetime column to filter on
        start_date: Start of date range (inclusive)
        end_date: End of date range (inclusive)

    Returns:
        Filtered DataFrame
    """
    df = df.copy()

    if datetime_column not in df.columns:
        return df

    df = convert_datetime_columns(df, [datetime_column])
    mask = pd.Series(True, index=df.index)

    if start_date is not None:
        mask = mask & (df[datetime_column] >= start_date)

    if end_date is not None:
        mask = mask & (df[datetime_column] <= end_date)

    return df[mask]


def compute_summary_statistics(
    df: pd.DataFrame,
    numeric_columns: Optional[list[str]] = None,
    group_column: Optional[str] = None,
) -> pd.DataFrame:
    """Compute summary statistics for numeric columns.

    Args:
        df: Input DataFrame
        numeric_columns: Columns to summarize (None = all numeric)
        group_column: Column to group by (optional)

    Returns:
        DataFrame with summary statistics
    """
    if numeric_columns is None:
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()

    if not numeric_columns:
        return pd.DataFrame()

    if group_column and group_column in df.columns:
        grouped = df.groupby(group_column)[numeric_columns]
        return grouped.agg(["count", "mean", "std", "min", "max", "median"])
    else:
        return df[numeric_columns].describe()


def resample_time_series(
    df: pd.DataFrame,
    time_column: str,
    value_column: str,
    freq: str = "1S",
    aggregation: str = "mean",
) -> pd.DataFrame:
    """Resample time series data to new frequency.

    Args:
        df: Input DataFrame
        time_column: Name of datetime column
        value_column: Name of value column to resample
        freq: Pandas frequency string (e.g., '1S', '1T', '1H')
        aggregation: Aggregation method ('mean', 'sum', 'max', 'min')

    Returns:
        Resampled DataFrame
    """
    df = df.copy()
    df = convert_datetime_columns(df, [time_column])

    df = df.set_index(time_column)

    agg_methods = {
        "mean": "mean",
        "sum": "sum",
        "max": "max",
        "min": "min",
    }

    resampled = (
        df[value_column].resample(freq).agg(agg_methods.get(aggregation, "mean"))
    )

    return resampled.reset_index()


def detect_outliers_iqr(
    df: pd.DataFrame,
    column: str,
    multiplier: float = 1.5,
) -> pd.Series:
    """Detect outliers using IQR (Interquartile Range) method.

    Args:
        df: Input DataFrame
        column: Column to check for outliers
        multiplier: IQR multiplier for bounds (default 1.5)

    Returns:
        Boolean Series indicating outliers
    """
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1

    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR

    return (df[column] < lower_bound) | (df[column] > upper_bound)


def flag_outliers(
    df: pd.DataFrame,
    column: str,
    new_column: str = "is_outlier",
) -> pd.DataFrame:
    """Add outlier flag column to DataFrame.

    Args:
        df: Input DataFrame
        column: Column to check for outliers
        new_column: Name for new flag column

    Returns:
        DataFrame with outlier flag column
    """
    df = df.copy()
    df[new_column] = detect_outliers_iqr(df, column)
    return df


def export_to_csv(
    df: pd.DataFrame,
    output_path: Path,
    include_index: bool = False,
) -> None:
    """Export DataFrame to CSV with proper error handling.

    Args:
        df: DataFrame to export
        output_path: Path for output file
        include_index: Whether to include index column

    Raises:
        DataMungingError: If export fails
    """
    try:
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(output_path, index=include_index)
    except OSError as e:
        raise DataMungingError(f"Failed to export to {output_path}: {e}")
