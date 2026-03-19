"""
Tests for FHIR Parser Module

Run with: pytest tests/test_fhir_parser.py -v
Run single test: pytest tests/test_fhir_parser.py::test_parse_observation -v
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Import from parent package
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.fhir_parser import (
    FHIRParseError,
    extract_coding_info,
    parse_fhir_date,
    load_fhir_resource,
    parse_condition,
    parse_observation,
    parse_immunization,
    parse_allergy_intolerance,
    parse_diagnostic_report,
    parse_procedure,
    aggregate_all_fhir,
    get_resource_type_counts,
)


# Test fixtures
@pytest.fixture
def observation_file_path() -> Path:
    """Return path to a sample Observation file."""
    return (
        Path(__file__).parent.parent
        / "clinical-records"
        / "Observation-0010757D-8DDD-4348-B5EB-B1AB91A27E64.json"
    )


@pytest.fixture
def condition_file_path() -> Path:
    """Return path to a sample Condition file."""
    return (
        Path(__file__).parent.parent
        / "clinical-records"
        / "Condition-008052D4-8D67-4392-8AE4-43A78CFBC395.json"
    )


@pytest.fixture
def immunization_file_path() -> Path:
    """Return path to a sample Immunization file."""
    return (
        Path(__file__).parent.parent
        / "clinical-records"
        / "Immunization-17F45166-2BE4-44F8-B35C-94A0837953D2.json"
    )


@pytest.fixture
def fhir_dir_path() -> Path:
    """Return path to clinical-records directory."""
    return Path(__file__).parent.parent / "clinical-records"


class TestExtractCodingInfo:
    """Tests for extract_coding_info function."""

    def test_valid_coding(self) -> None:
        """Test extracting valid coding."""
        coding = [
            {
                "code": "8480-6",
                "display": "Systolic blood pressure",
                "system": "http://loinc.org",
            }
        ]
        result = extract_coding_info(coding)

        assert result["code"] == "8480-6"
        assert result["display"] == "Systolic blood pressure"
        assert result["system"] == "http://loinc.org"

    def test_none_input(self) -> None:
        """Test with None input."""
        result = extract_coding_info(None)

        assert result["code"] is None
        assert result["display"] is None
        assert result["system"] is None

    def test_empty_list(self) -> None:
        """Test with empty list."""
        result = extract_coding_info([])

        assert result["code"] is None


class TestParseFHIRDate:
    """Tests for parse_fhir_date function."""

    def test_full_datetime_with_timezone(self) -> None:
        """Test parsing full datetime with timezone."""
        result = parse_fhir_date("2020-02-05T09:14:40-04:00")
        assert result is not None
        assert result.year == 2020
        assert result.month == 2
        assert result.day == 5

    def test_date_only(self) -> None:
        """Test parsing date only."""
        result = parse_fhir_date("2020-02-05")
        assert result is not None
        assert result.year == 2020
        assert result.month == 2

    def test_none_input(self) -> None:
        """Test with None input."""
        result = parse_fhir_date(None)
        assert result is None

    def test_invalid_format(self) -> None:
        """Test with invalid format."""
        result = parse_fhir_date("not-a-date")
        assert result is None


class TestLoadFHIRResource:
    """Tests for load_fhir_resource function."""

    def test_load_valid_file(self, observation_file_path: Path) -> None:
        """Test loading valid FHIR file."""
        result = load_fhir_resource(observation_file_path)

        assert result["resourceType"] == "Observation"
        assert "id" in result
        assert "status" in result

    def test_raises_error_for_missing_resource_type(self, tmp_path: Path) -> None:
        """Test that missing resourceType raises FHIRParseError."""
        # Create invalid JSON file
        invalid_file = tmp_path / "invalid.json"
        invalid_file.write_text('{"id": "test"}')

        with pytest.raises(FHIRParseError, match="Missing resourceType"):
            load_fhir_resource(invalid_file)

    def test_raises_error_for_invalid_json(self, tmp_path: Path) -> None:
        """Test that invalid JSON raises FHIRParseError."""
        invalid_file = tmp_path / "invalid.json"
        invalid_file.write_text("{invalid json}")

        with pytest.raises(FHIRParseError, match="Invalid JSON"):
            load_fhir_resource(invalid_file)


class TestParseObservation:
    """Tests for parse_observation function."""

    def test_parse_blood_pressure_observation(
        self, observation_file_path: Path
    ) -> None:
        """Test parsing blood pressure observation with components."""
        resource = load_fhir_resource(observation_file_path)
        result = parse_observation(resource, observation_file_path)

        # Should return list of records (one per component)
        assert isinstance(result, list)
        assert len(result) >= 1

        # Check first record has required fields
        record = result[0]
        assert record["resource_type"] == "Observation"
        assert record["source_file"] == observation_file_path.name
        assert "component_display" in record

        # Check blood pressure components
        displays = [r["component_display"] for r in result]
        assert "Systolic blood pressure" in displays
        assert "Diastolic blood pressure" in displays


class TestParseCondition:
    """Tests for parse_condition function."""

    def test_parse_condition(self, condition_file_path: Path) -> None:
        """Test parsing Condition resource."""
        resource = load_fhir_resource(condition_file_path)
        result = parse_condition(resource, condition_file_path)

        assert result["resource_type"] == "Condition"
        assert result["source_file"] == condition_file_path.name
        assert result["condition_display"] is not None
        assert result["clinical_status"] == "Active"


class TestParseImmunization:
    """Tests for parse_immunization function."""

    def test_parse_immunization(self, immunization_file_path: Path) -> None:
        """Test parsing Immunization resource."""
        resource = load_fhir_resource(immunization_file_path)
        result = parse_immunization(resource, immunization_file_path)

        assert result["resource_type"] == "Immunization"
        assert result["source_file"] == immunization_file_path.name
        assert result["vaccine_display"] == "Tdap"
        assert result["status"] == "completed"


class TestParseAllergyIntolerance:
    """Tests for parse_allergy_intolerance function."""

    def test_parse_allergy(self) -> None:
        """Test parsing AllergyIntolerance resource."""
        allergy_file = (
            Path(__file__).parent.parent
            / "clinical-records"
            / "AllergyIntolerance-6A3CD53E-087B-4454-9093-315BFF07F1C0.json"
        )
        resource = load_fhir_resource(allergy_file)
        result = parse_allergy_intolerance(resource, allergy_file)

        assert result["resource_type"] == "AllergyIntolerance"
        assert result["clinical_status"] == "Active"


class TestParseDiagnosticReport:
    """Tests for parse_diagnostic_report function."""

    def test_parse_diagnostic_report(self) -> None:
        """Test parsing DiagnosticReport resource."""
        report_file = (
            Path(__file__).parent.parent
            / "clinical-records"
            / "DiagnosticReport-0969A182-006C-4BDA-AC4B-F485C5FE0BCF.json"
        )
        resource = load_fhir_resource(report_file)
        result = parse_diagnostic_report(resource, report_file)

        assert result["resource_type"] == "DiagnosticReport"
        assert result["report_display"] is not None
        assert len(result["result_references"]) > 0


class TestParseProcedure:
    """Tests for parse_procedure function."""

    def test_parse_procedure(self) -> None:
        """Test parsing Procedure resource."""
        procedure_file = (
            Path(__file__).parent.parent
            / "clinical-records"
            / "Procedure-29B9AD5E-9C5F-447E-9376-0BB317ECE425.json"
        )
        resource = load_fhir_resource(procedure_file)
        result = parse_procedure(resource, procedure_file)

        assert result["resource_type"] == "Procedure"
        assert result["status"] == "completed"


# Integration tests
class TestGetResourceTypeCounts:
    """Integration tests for get_resource_type_counts function."""

    def test_count_all_types(self, fhir_dir_path: Path) -> None:
        """Test counting all resource types."""
        result = get_resource_type_counts(fhir_dir_path)

        # Should have multiple types
        assert len(result) > 0

        # Each count should be positive
        assert (result > 0).all()

        # Should include expected types
        assert "Observation" in result.index
        assert "Condition" in result.index


class TestAggregateAllFHIR:
    """Integration tests for aggregate_all_fhir function."""

    def test_aggregate_all_files(self, fhir_dir_path: Path) -> None:
        """Test aggregating all FHIR files."""
        result = aggregate_all_fhir(fhir_dir_path)

        # Should have combined data
        assert len(result) > 0

        # Check required columns
        assert "resource_type" in result.columns
        assert "source_file" in result.columns

        # Should have multiple resource types
        assert result["resource_type"].nunique() > 1
