"""
FHIR Clinical Records Parsing Module

Parses FHIR R4 JSON clinical records from the clinical-records directory.
Handles multiple resource types: Condition, Observation, DiagnosticReport,
Immunization, AllergyIntolerance, DocumentReference, and Procedure.

FHIR resources follow standard structure with fields like:
    - resourceType: Type of resource
    - id: Unique identifier
    - status: Current status (e.g., "final", "active")
    - effectiveDateTime/onsetDateTime/occurrenceDateTime: When recorded
    - code: What the resource represents (with coding systems like LOINC, SNOMED)
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd


class FHIRParseError(Exception):
    """Raised when FHIR resource parsing fails."""

    pass


# FHIR Resource type to handler function mapping
FHIR_RESOURCE_TYPES = [
    "Condition",
    "Observation",
    "DiagnosticReport",
    "Immunization",
    "AllergyIntolerance",
    "DocumentReference",
    "Procedure",
]


def load_fhir_resource(filepath: Path) -> dict[str, Any]:
    """Load a single FHIR JSON resource file.

    Args:
        filepath: Path to FHIR JSON file

    Returns:
        Parsed FHIR resource dictionary

    Raises:
        FHIRParseError: If file cannot be parsed as JSON or is invalid FHIR
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            resource = json.load(f)
    except json.JSONDecodeError as e:
        raise FHIRParseError(f"Invalid JSON in {filepath}: {e}")

    # Validate required FHIR fields
    if "resourceType" not in resource:
        raise FHIRParseError(f"Missing resourceType in {filepath}")

    return resource


def extract_coding_info(coding_field: Optional[list]) -> dict[str, Optional[str]]:
    """Extract coding information from FHIR CodeableConcept field.

    Args:
        coding_field: FHIR coding array or None

    Returns:
        Dictionary with 'code', 'display', and 'system' keys
    """
    if not coding_field or not isinstance(coding_field, list):
        return {"code": None, "display": None, "system": None}

    # Prefer the first coding with a code value
    for coding in coding_field:
        if isinstance(coding, dict):
            return {
                "code": coding.get("code"),
                "display": coding.get("display"),
                "system": coding.get("system"),
            }

    return {"code": None, "display": None, "system": None}


def parse_fhir_date(date_value: str | None) -> Optional[datetime]:
    """Parse FHIR date/datetime string to datetime object.

    Handles various FHIR date formats:
        - YYYY
        - YYYY-MM
        - YYYY-MM-DD
        - YYYY-MM-DDThh:mm:ss
        - YYYY-MM-DDThh:mm:ss+/-hh:mm

    Args:
        date_value: FHIR date string

    Returns:
        datetime object, or None if parsing fails
    """
    if not date_value:
        return None

    # Try full datetime first
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
        "%Y-%m",
        "%Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_value[: len("%Y-%m-%dT%H:%M:%S") + 5], fmt)
        except (ValueError, TypeError):
            try:
                return datetime.strptime(date_value[:10], fmt)
            except (ValueError, TypeError):
                continue

    return None


def parse_condition(resource: dict[str, Any], filepath: Path) -> dict[str, Any]:
    """Parse FHIR Condition resource.

    Condition represents a clinical condition, problem, diagnosis, or health concern.

    Args:
        resource: FHIR Condition resource dictionary
        filepath: Source file path

    Returns:
        Flattened condition record
    """
    code_info = extract_coding_info(resource.get("code", {}).get("coding"))
    clinical_status = extract_coding_info(
        resource.get("clinicalStatus", {}).get("coding")
    )
    verification_status = extract_coding_info(
        resource.get("verificationStatus", {}).get("coding")
    )

    return {
        "source_file": filepath.name,
        "resource_id": resource.get("id"),
        "resource_type": "Condition",
        "status": resource.get("status"),
        "clinical_status": clinical_status.get("display"),
        "clinical_status_code": clinical_status.get("code"),
        "verification_status": verification_status.get("display"),
        "condition_code": code_info.get("code"),
        "condition_display": code_info.get("display"),
        "condition_system": code_info.get("system"),
        "condition_text": resource.get("code", {}).get("text"),
        "onset_datetime": parse_fhir_date(resource.get("onsetDateTime")),
        "recorded_date": parse_fhir_date(resource.get("recordedDate")),
        " abatement_datetime": parse_fhir_date(resource.get("abatementDateTime")),
    }


def parse_observation(resource: dict[str, Any], filepath: Path) -> list[dict[str, Any]]:
    """Parse FHIR Observation resource.

    Observations are measurements or simple assertions about a patient.
    May contain components (e.g., systolic/diastolic for blood pressure).

    Args:
        resource: FHIR Observation resource dictionary
        filepath: Source file path

    Returns:
        List of observation records (one per component, plus main if value exists)
    """
    base_record = {
        "source_file": filepath.name,
        "resource_id": resource.get("id"),
        "resource_type": "Observation",
        "status": resource.get("status"),
        "category": None,
        "observation_code": None,
        "observation_display": None,
        "observation_system": None,
        "observation_text": None,
        "effective_datetime": parse_fhir_date(resource.get("effectiveDateTime")),
        "issued_datetime": parse_fhir_date(resource.get("issued")),
    }

    # Extract category
    category_list = resource.get("category", [])
    if category_list and isinstance(category_list, list):
        category_info = extract_coding_info(category_list[0].get("coding"))
        base_record["category"] = category_info.get("display")

    # Extract main observation code
    code_info = extract_coding_info(resource.get("code", {}).get("coding"))
    base_record["observation_code"] = code_info.get("code")
    base_record["observation_display"] = code_info.get("display")
    base_record["observation_system"] = code_info.get("system")
    base_record["observation_text"] = resource.get("code", {}).get("text")

    records = []

    # Parse value (if present as simple value)
    value_quantity = resource.get("valueQuantity", {})
    if value_quantity:
        record = base_record.copy()
        record["component_code"] = code_info.get("code") or "VALUE"
        record["component_display"] = code_info.get("display") or "Value"
        record["component_system"] = code_info.get("system")
        record["value"] = value_quantity.get("value")
        record["unit"] = value_quantity.get("unit")
        record["value_code"] = value_quantity.get("code")
        records.append(record)

    # Parse components (e.g., systolic/diastolic BP)
    components = resource.get("component", [])
    if components:
        for component in components:
            comp_code_info = extract_coding_info(
                component.get("code", {}).get("coding")
            )
            comp_value = component.get("valueQuantity", {})

            record = base_record.copy()
            record["component_code"] = comp_code_info.get("code")
            record["component_display"] = comp_code_info.get("display")
            record["component_system"] = comp_code_info.get("system")
            record["value"] = comp_value.get("value")
            record["unit"] = comp_value.get("unit")
            record["value_code"] = comp_value.get("code")
            records.append(record)

    # If no value or components, return base record
    if not records:
        records.append(base_record)

    return records


def parse_immunization(resource: dict[str, Any], filepath: Path) -> dict[str, Any]:
    """Parse FHIR Immunization resource.

    Immunization represents vaccine administration.

    Args:
        resource: FHIR Immunization resource dictionary
        filepath: Source file path

    Returns:
        Flattened immunization record
    """
    vaccine_info = extract_coding_info(resource.get("vaccineCode", {}).get("coding"))
    route_info = extract_coding_info(resource.get("route", {}).get("coding"))

    manufacturer = resource.get("manufacturer", {})
    if isinstance(manufacturer, dict):
        manufacturer_display = manufacturer.get("display")
    else:
        manufacturer_display = None

    return {
        "source_file": filepath.name,
        "resource_id": resource.get("id"),
        "resource_type": "Immunization",
        "status": resource.get("status"),
        "vaccine_code": vaccine_info.get("code"),
        "vaccine_display": vaccine_info.get("display"),
        "vaccine_system": vaccine_info.get("system"),
        "vaccine_text": resource.get("vaccineCode", {}).get("text"),
        "occurrence_datetime": parse_fhir_date(resource.get("occurrenceDateTime")),
        "lot_number": resource.get("lotNumber"),
        "manufacturer": manufacturer_display,
        "route": route_info.get("display"),
        "primary_source": resource.get("primarySource"),
        "is_subpotent": resource.get("isSubpotent"),
    }


def parse_allergy_intolerance(
    resource: dict[str, Any], filepath: Path
) -> dict[str, Any]:
    """Parse FHIR AllergyIntolerance resource.

    AllergyIntolerance represents allergies and intolerances.

    Args:
        resource: FHIR AllergyIntolerance resource dictionary
        filepath: Source file path

    Returns:
        Flattened allergy record
    """
    code_info = extract_coding_info(resource.get("code", {}).get("coding"))
    clinical_status = extract_coding_info(
        resource.get("clinicalStatus", {}).get("coding")
    )
    verification_status = extract_coding_info(
        resource.get("verificationStatus", {}).get("coding")
    )

    return {
        "source_file": filepath.name,
        "resource_id": resource.get("id"),
        "resource_type": "AllergyIntolerance",
        "clinical_status": clinical_status.get("display"),
        "verification_status": verification_status.get("display"),
        "allergy_code": code_info.get("code"),
        "allergy_display": code_info.get("display"),
        "allergy_system": code_info.get("system"),
        "allergy_text": resource.get("code", {}).get("text"),
        "recorded_date": parse_fhir_date(resource.get("recordedDate")),
        "onset_datetime": parse_fhir_date(resource.get("onsetDateTime")),
    }


def parse_diagnostic_report(resource: dict[str, Any], filepath: Path) -> dict[str, Any]:
    """Parse FHIR DiagnosticReport resource.

    DiagnosticReport is a record of a diagnostic report (e.g., lab results).

    Args:
        resource: FHIR DiagnosticReport resource dictionary
        filepath: Source file path

    Returns:
        Flattened diagnostic report record
    """
    code_info = extract_coding_info(resource.get("code", {}).get("coding"))
    category_list = resource.get("category", [])

    category_display = None
    if category_list and isinstance(category_list, list):
        category_info = extract_coding_info(category_list[0].get("coding"))
        category_display = category_info.get("display")

    # Extract result references
    results = resource.get("result", [])
    result_refs = [r.get("reference") for r in results if isinstance(r, dict)]

    return {
        "source_file": filepath.name,
        "resource_id": resource.get("id"),
        "resource_type": "DiagnosticReport",
        "status": resource.get("status"),
        "category": category_display,
        "report_code": code_info.get("code"),
        "report_display": code_info.get("display"),
        "report_system": code_info.get("system"),
        "report_text": resource.get("code", {}).get("text"),
        "effective_datetime": parse_fhir_date(resource.get("effectiveDateTime")),
        "issued_datetime": parse_fhir_date(resource.get("issued")),
        "result_references": result_refs,
        "performer_reference": None,  # Simplified
    }


def parse_procedure(resource: dict[str, Any], filepath: Path) -> dict[str, Any]:
    """Parse FHIR Procedure resource.

    Procedure represents a medical procedure performed.

    Args:
        resource: FHIR Procedure resource dictionary
        filepath: Source file path

    Returns:
        Flattened procedure record
    """
    code_info = extract_coding_info(resource.get("code", {}).get("coding"))

    # Handle performedPeriod
    performed_period = resource.get("performedPeriod", {})
    if isinstance(performed_period, dict):
        performed_start = parse_fhir_date(performed_period.get("start"))
        performed_end = parse_fhir_date(performed_period.get("end"))
    else:
        performed_start = parse_fhir_date(resource.get("performedDateTime"))
        performed_end = None

    return {
        "source_file": filepath.name,
        "resource_id": resource.get("id"),
        "resource_type": "Procedure",
        "status": resource.get("status"),
        "procedure_code": code_info.get("code"),
        "procedure_display": code_info.get("display"),
        "procedure_system": code_info.get("system"),
        "procedure_text": resource.get("code", {}).get("text"),
        "performed_start": performed_start,
        "performed_end": performed_end,
    }


def parse_document_reference(
    resource: dict[str, Any], filepath: Path
) -> dict[str, Any]:
    """Parse FHIR DocumentReference resource.

    DocumentReference represents a reference to a clinical document.

    Args:
        resource: FHIR DocumentReference resource dictionary
        filepath: Source file path

    Returns:
        Flattened document reference record
    """
    type_info = extract_coding_info(resource.get("type", {}).get("coding"))

    # Extract content attachments
    contents = resource.get("content", [])
    content_info = []
    for c in contents:
        if isinstance(c, dict) and "attachment" in c:
            att = c["attachment"]
            content_info.append(
                {
                    "url": att.get("url"),
                    "content_type": att.get("contentType"),
                    "title": att.get("title"),
                }
            )

    return {
        "source_file": filepath.name,
        "resource_id": resource.get("id"),
        "resource_type": "DocumentReference",
        "status": resource.get("status"),
        "description": resource.get("description"),
        "document_type_code": type_info.get("code"),
        "document_type_display": type_info.get("display"),
        "document_type_system": type_info.get("system"),
        "date": parse_fhir_date(resource.get("date")),
        "author_references": [
            a.get("reference")
            for a in resource.get("author", [])
            if isinstance(a, dict)
        ],
        "content_info": content_info,
    }


def parse_fhir_resource(filepath: Path) -> list[dict[str, Any]]:
    """Parse a FHIR resource file based on its resourceType.

    Args:
        filepath: Path to FHIR JSON file

    Returns:
        List of flattened record dictionaries
    """
    resource = load_fhir_resource(filepath)
    resource_type = resource.get("resourceType")

    parsers = {
        "Condition": parse_condition,
        "Observation": parse_observation,
        "Immunization": parse_immunization,
        "AllergyIntolerance": parse_allergy_intolerance,
        "DiagnosticReport": parse_diagnostic_report,
        "Procedure": parse_procedure,
        "DocumentReference": parse_document_reference,
    }

    parser = parsers.get(resource_type or "")
    if parser:
        return parser(resource, filepath)
    else:
        raise FHIRParseError(f"Unknown resourceType '{resource_type}' in {filepath}")


def aggregate_all_fhir(directory: Path) -> pd.DataFrame:
    """Aggregate all FHIR resources from a directory into a DataFrame.

    Args:
        directory: Directory containing FHIR JSON files

    Returns:
        Combined DataFrame with all parsed FHIR records
    """
    json_files = sorted(directory.glob("*.json"))

    if not json_files:
        raise FHIRParseError(f"No JSON files found in {directory}")

    all_records: list[dict[str, Any]] = []

    for filepath in json_files:
        try:
            records = parse_fhir_resource(filepath)
            if isinstance(records, list):
                all_records.extend(records)
            else:
                all_records.append(records)
        except FHIRParseError as e:
            print(f"Warning: Skipping {filepath.name}: {e}")
            continue

    if not all_records:
        raise FHIRParseError("No valid FHIR files could be parsed")

    return pd.DataFrame(all_records)


def get_resource_type_counts(directory: Path) -> pd.Series:
    """Count FHIR resources by resourceType.

    Args:
        directory: Directory containing FHIR JSON files

    Returns:
        Series with resourceType counts
    """
    json_files = list(directory.glob("*.json"))

    type_counts: dict[str, int] = {}
    for filepath in json_files:
        try:
            resource = load_fhir_resource(filepath)
            res_type = resource.get("resourceType", "Unknown")
            type_counts[res_type] = type_counts.get(res_type, 0) + 1
        except FHIRParseError:
            continue

    return pd.Series(type_counts).sort_values(ascending=False)
