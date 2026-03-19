#!/usr/bin/env python3
"""
Main Script for Health Data Analysis

Demonstrates usage of the health data analysis package for parsing,
cleaning, and summarizing ECG and FHIR clinical records from Apple Health export.

Usage:
    python scripts/analyze_health_data.py

Or when installed as a package:
    from analysis import aggregate_all_ecg, aggregate_all_fhir
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.ecg_parser import aggregate_all_ecg, parse_ecg_full, parse_ecg_metadata
from analysis.fhir_parser import aggregate_all_fhir, get_resource_type_counts
from analysis.data_munging import (
    convert_datetime_columns,
    export_to_csv,
    handle_missing_values,
    standardize_column_names,
)
from analysis.summary import (
    get_ecg_summary,
    get_fhir_summary,
    generate_data_quality_report,
    create_timeline_summary,
)


def main() -> None:
    """Run complete health data analysis pipeline."""

    # Define paths
    base_dir = Path(__file__).parent.parent
    ecg_dir = base_dir / "electrocardiograms"
    fhir_dir = base_dir / "clinical-records"
    output_dir = base_dir / "output"

    # Ensure output directory exists
    output_dir.mkdir(exist_ok=True)

    print("=" * 60)
    print("HEALTH DATA ANALYSIS PIPELINE")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # =============================================================
    # STEP 1: Parse and aggregate ECG data
    # =============================================================
    print("[1/5] Parsing ECG data...")

    try:
        ecg_df = aggregate_all_ecg(ecg_dir)
        print(f"    - Parsed {len(ecg_df):,} ECG waveform samples")
        print(f"    - Unique recordings: {ecg_df['source_file'].nunique()}")

        # Standardize column names
        ecg_column_renames = {
            "lead_i_mv": "lead_i_microvolts",
            "recorded_date": "ecg_datetime",
        }
        ecg_df = standardize_column_names(ecg_df, ecg_column_renames)

        # Convert datetime columns
        ecg_df = convert_datetime_columns(ecg_df, ["ecg_datetime", "date_of_birth"])

        # Export raw aggregated data
        export_to_csv(ecg_df, output_dir / "ecg_aggregated.csv")
        print("    - Exported: output/ecg_aggregated.csv")

    except Exception as e:
        print(f"    - ERROR: {e}")
        ecg_df = None

    print()

    # =============================================================
    # STEP 2: Parse and aggregate FHIR clinical records
    # =============================================================
    print("[2/5] Parsing FHIR clinical records...")

    try:
        fhir_df = aggregate_all_fhir(fhir_dir)
        print(f"    - Parsed {len(fhir_df):,} FHIR records")

        # Standardize column names
        fhir_column_renames = {
            "effective_datetime": "observation_datetime",
            "onset_datetime": "condition_onset_datetime",
        }
        fhir_df = standardize_column_names(fhir_df, fhir_column_renames)

        # Convert datetime columns
        datetime_cols = [
            "observation_datetime",
            "condition_onset_datetime",
            "occurrence_datetime",
            "recorded_date",
            "issued_datetime",
            "date",
            "performed_start",
            " abatement_datetime",
        ]
        fhir_df = convert_datetime_columns(fhir_df, datetime_cols)

        # Handle missing values
        fhir_df = handle_missing_values(fhir_df, strategy="flag")

        # Export raw aggregated data
        export_to_csv(fhir_df, output_dir / "fhir_aggregated.csv")
        print("    - Exported: output/fhir_aggregated.csv")

    except Exception as e:
        print(f"    - ERROR: {e}")
        fhir_df = None

    print()

    # =============================================================
    # STEP 3: Generate ECG summary
    # =============================================================
    if ecg_df is not None:
        print("[3/5] Generating ECG summary...")

        ecg_summary = get_ecg_summary(ecg_df)

        # Print key ECG stats
        print(f"    - Total recordings: {ecg_summary.get('total_recordings', 'N/A')}")
        print(f"    - Total samples: {ecg_summary.get('total_samples', 'N/A'):,}")

        if "earliest_recording" in ecg_summary:
            print(
                f"    - Date range: {ecg_summary['earliest_recording']} to {ecg_summary['latest_recording']}"
            )

        if "classifications" in ecg_summary:
            print("    - Classifications:")
            for cls, count in ecg_summary["classifications"].items():
                print(f"        {cls}: {count}")

        if "lead_i_stats" in ecg_summary:
            stats = ecg_summary["lead_i_stats"]
            print(
                f"    - Lead I amplitude (µV): mean={stats.get('mean', 'N/A'):.2f}, "
                f"std={stats.get('std', 'N/A'):.2f}"
            )

        # Save ECG summary
        with open(output_dir / "ecg_summary.json", "w") as f:
            # Convert datetime objects to strings for JSON serialization
            summary_json = {
                k: str(v) if isinstance(v, (datetime,)) else v
                for k, v in ecg_summary.items()
            }
            json.dump(summary_json, f, indent=2, default=str)
        print("    - Exported: output/ecg_summary.json")

    print()

    # =============================================================
    # STEP 4: Generate FHIR summary
    # =============================================================
    if fhir_df is not None:
        print("[4/5] Generating FHIR summary...")

        fhir_summary = get_fhir_summary(fhir_df)

        # Print key FHIR stats
        print(f"    - Total records: {fhir_summary.get('total_records', 'N/A'):,}")

        if "resource_type_counts" in fhir_summary:
            print("    - Records by type:")
            for res_type, count in fhir_summary["resource_type_counts"].items():
                print(f"        {res_type}: {count}")

        if "condition_count" in fhir_summary:
            print(f"    - Total conditions: {fhir_summary['condition_count']}")
            if "top_conditions" in fhir_summary:
                print("    - Top conditions:")
                for cond, count in list(fhir_summary["top_conditions"].items())[:5]:
                    print(f"        {cond}: {count}")

        if "observation_count" in fhir_summary:
            print(f"    - Total observations: {fhir_summary['observation_count']}")

        if "immunization_count" in fhir_summary:
            print(f"    - Total immunizations: {fhir_summary['immunization_count']}")
            if "vaccines_given" in fhir_summary:
                print("    - Vaccines given:")
                for vaccine, count in fhir_summary["vaccines_given"].items():
                    print(f"        {vaccine}: {count}")

        # Save FHIR summary
        with open(output_dir / "fhir_summary.json", "w") as f:
            json.dump(fhir_summary, f, indent=2, default=str)
        print("    - Exported: output/fhir_summary.json")

    print()

    # =============================================================
    # STEP 5: Generate data quality reports
    # =============================================================
    print("[5/5] Generating data quality reports...")

    if ecg_df is not None:
        ecg_quality = generate_data_quality_report(ecg_df)
        with open(output_dir / "ecg_quality_report.json", "w") as f:
            json.dump(ecg_quality, f, indent=2, default=str)
        print("    - Exported: output/ecg_quality_report.json")
        print(
            f"    - ECG completeness: {ecg_quality.get('completeness_score', 'N/A')}%"
        )

    if fhir_df is not None:
        fhir_quality = generate_data_quality_report(fhir_df)
        with open(output_dir / "fhir_quality_report.json", "w") as f:
            json.dump(fhir_quality, f, indent=2, default=str)
        print("    - Exported: output/fhir_quality_report.json")
        print(
            f"    - FHIR completeness: {fhir_quality.get('completeness_score', 'N/A')}%"
        )

    print()
    print("=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("Output files written to:", output_dir)


if __name__ == "__main__":
    main()
