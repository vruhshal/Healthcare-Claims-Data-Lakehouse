"""
test_data_pipeline.py
Tests for the healthcare claims data generator and silver transform logic.
Run: pytest tests/ -v
"""

import pytest
import json
import os
import sys
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data'))
from generate_claims_data import make_patient, make_claim, generate_hl7_message


class TestPatientGenerator:

    def test_patient_has_required_fields(self):
        patient = make_patient("PAT000001")
        assert patient["id"] == "PAT000001"
        assert patient["resourceType"] == "Patient"
        assert "name" in patient
        assert "gender" in patient
        assert "birthDate" in patient

    def test_patient_gender_valid(self):
        genders = {make_patient(f"P{i}")["gender"] for i in range(50)}
        assert genders.issubset({"male", "female"})

    def test_patient_birth_date_format(self):
        patient = make_patient("PAT999")
        dob = patient["birthDate"]
        # Should be YYYY-MM-DD
        assert re.match(r"\d{4}-\d{2}-\d{2}", dob), f"Bad date format: {dob}"

    def test_patient_country_is_in(self):
        patient = make_patient("PAT001")
        country = patient["address"][0]["country"]
        assert country == "IN"


class TestClaimGenerator:

    def test_claim_has_required_fields(self):
        claim = make_claim("PAT000001", "CLM00000001")
        required = [
            "id", "patient_id", "status", "service_date",
            "provider_name", "diagnosis_code", "billed_amount",
            "approved_amount", "insurance_plan"
        ]
        for field in required:
            assert field in claim, f"Missing field: {field}"

    def test_claim_status_is_valid(self):
        statuses = {make_claim("PAT001", f"CLM{i:05}")["status"] for i in range(100)}
        assert statuses.issubset({"APPROVED", "DENIED", "PENDING", "ADJUSTED"})

    def test_billed_amount_positive(self):
        for i in range(20):
            claim = make_claim("PAT001", f"CLM{i:05}")
            assert claim["billed_amount"] > 0

    def test_approved_amount_less_than_billed(self):
        for i in range(20):
            claim = make_claim("PAT001", f"CLM{i:05}")
            if claim["approved_amount"] > 0:
                assert claim["approved_amount"] <= claim["billed_amount"]

    def test_service_date_format(self):
        claim = make_claim("PAT001", "CLM00001")
        date_str = claim["service_date"]
        assert re.match(r"\d{4}-\d{2}-\d{2}", date_str), f"Bad date: {date_str}"

    def test_diagnosis_code_is_icd10_like(self):
        for i in range(20):
            claim = make_claim("PAT001", f"CLM{i:05}")
            code = claim["diagnosis_code"]
            # ICD-10 starts with a letter followed by digits
            assert re.match(r"^[A-Z]\d", code), f"Bad ICD-10 code: {code}"

    def test_source_field_is_fhir(self):
        claim = make_claim("PAT001", "CLM00001")
        assert claim["_source"] == "FHIR_R4"


class TestHL7Generator:

    def test_hl7_has_required_segments(self):
        msg = generate_hl7_message("PAT000001")
        assert "MSH|" in msg
        assert "PID|" in msg
        assert "PV1|" in msg

    def test_hl7_patient_id_present(self):
        msg = generate_hl7_message("PAT000001")
        assert "PAT000001" in msg

    def test_hl7_is_multiline(self):
        msg = generate_hl7_message("PAT001")
        lines = msg.strip().split("\n")
        assert len(lines) >= 4


class TestDataVolume:

    def test_generate_correct_number_of_patients(self):
        """Quick integration test: generate 10 patients and check output."""
        import tempfile, subprocess
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [sys.executable,
                 os.path.join(os.path.dirname(__file__), '..', 'data', 'generate_claims_data.py'),
                 "--num-patients", "10",
                 "--output", tmp],
                capture_output=True, text=True
            )
            assert result.returncode == 0, result.stderr

            with open(os.path.join(tmp, "fhir", "patients.json")) as f:
                patients = json.load(f)
            assert len(patients) == 10

            with open(os.path.join(tmp, "fhir", "claims.json")) as f:
                claims = json.load(f)
            # Each patient has 1-5 claims
            assert 10 <= len(claims) <= 50

            hl7_files = os.listdir(os.path.join(tmp, "hl7"))
            assert len(hl7_files) == 10
