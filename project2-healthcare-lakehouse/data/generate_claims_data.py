"""
generate_claims_data.py
Generates fake FHIR-style healthcare claims data using Faker.
Run: python generate_claims_data.py --num-patients 500 --output data/
"""

import json
import random
import argparse
import os
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("en_IN")
random.seed(42)

# ── ICD-10 diagnosis codes (common ones) ──────────────────────────────────────
DIAGNOSIS_CODES = [
    ("E11.9",  "Type 2 diabetes mellitus"),
    ("I10",    "Essential hypertension"),
    ("J18.9",  "Pneumonia, unspecified"),
    ("M54.5",  "Low back pain"),
    ("K21.0",  "GERD with oesophagitis"),
    ("F32.1",  "Major depressive disorder"),
    ("I25.10", "Coronary artery disease"),
    ("N18.3",  "Chronic kidney disease stage 3"),
    ("J45.901","Unspecified asthma"),
    ("Z23",    "Encounter for immunization"),
]

# ── Procedure codes ───────────────────────────────────────────────────────────
PROCEDURE_CODES = [
    ("99213", "Office visit - established patient"),
    ("99214", "Office visit - moderate complexity"),
    ("80053", "Comprehensive metabolic panel"),
    ("71046", "Chest X-ray 2 views"),
    ("93000", "ECG with interpretation"),
    ("36415", "Routine venipuncture"),
    ("90686", "Influenza vaccine"),
    ("45378", "Colonoscopy diagnostic"),
]

PROVIDERS = [
    "Apollo Hospitals", "Fortis Healthcare", "Max Super Speciality",
    "AIIMS Delhi", "Manipal Hospitals", "Narayana Health",
]


def make_patient(patient_id: str) -> dict:
    gender = random.choice(["male", "female"])
    first = fake.first_name_male() if gender == "male" else fake.first_name_female()
    return {
        "resourceType": "Patient",
        "id": patient_id,
        "name": [{"family": fake.last_name(), "given": [first]}],
        "gender": gender,
        "birthDate": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "address": [{"city": fake.city(), "state": fake.state(), "country": "IN"}],
        "telecom": [{"system": "phone", "value": fake.phone_number()}],
    }


def make_claim(patient_id: str, claim_id: str) -> dict:
    diag_code, diag_desc = random.choice(DIAGNOSIS_CODES)
    proc_code, proc_desc = random.choice(PROCEDURE_CODES)
    billed = round(random.uniform(500, 85000), 2)
    approved = round(billed * random.uniform(0.5, 0.95), 2)
    service_date = fake.date_between(start_date="-1y", end_date="today")

    return {
        "resourceType": "Claim",
        "id": claim_id,
        "patient_id": patient_id,
        "status": random.choices(
            ["APPROVED", "DENIED", "PENDING", "ADJUSTED"],
            weights=[65, 15, 10, 10]
        )[0],
        "service_date": service_date.isoformat(),
        "provider_name": random.choice(PROVIDERS),
        "provider_npi": f"NPI{random.randint(1000000000, 9999999999)}",
        "diagnosis_code": diag_code,
        "diagnosis_description": diag_desc,
        "procedure_code": proc_code,
        "procedure_description": proc_desc,
        "billed_amount": billed,
        "approved_amount": approved if random.random() > 0.15 else 0.0,
        "patient_responsibility": round(billed - approved, 2),
        "insurance_plan": random.choice([
            "PMJAY", "CGHS", "ESIC", "Star Health", "HDFC Ergo"
        ]),
        "_source": "FHIR_R4",
        "_ingested_at": datetime.utcnow().isoformat(),
    }


def generate_hl7_message(patient_id: str) -> str:
    """Generate a simplified HL7 v2 ADT^A01 admission message."""
    now = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    dob = fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y%m%d")
    gender = random.choice(["M", "F"])
    name = f"{fake.last_name()}^{fake.first_name()}"

    return "\n".join([
        f"MSH|^~\\&|HIS|HOSPITAL|ADT|FACILITY|{now}||ADT^A01|{fake.uuid4()}|P|2.5",
        f"EVN|A01|{now}",
        f"PID|1||{patient_id}^^^HOSP||{name}||{dob}|{gender}|||{fake.city()}^^{fake.state()}^^IN",
        f"PV1|1|I|ICU^101^A|U||{fake.uuid4()}^{fake.last_name()}^{fake.first_name()}",
        f"DG1|1||{random.choice(DIAGNOSIS_CODES)[0]}^{random.choice(DIAGNOSIS_CODES)[1]}|",
    ])


def main(num_patients: int, output_dir: str):
    os.makedirs(f"{output_dir}/fhir", exist_ok=True)
    os.makedirs(f"{output_dir}/hl7", exist_ok=True)

    all_claims = []
    all_patients = []

    for i in range(num_patients):
        patient_id = f"PAT{str(i+1).zfill(6)}"
        patient = make_patient(patient_id)
        all_patients.append(patient)

        # Each patient has 1-5 claims
        num_claims = random.randint(1, 5)
        for j in range(num_claims):
            claim_id = f"CLM{str(i*10+j).zfill(8)}"
            claim = make_claim(patient_id, claim_id)
            all_claims.append(claim)

        # Also generate an HL7 message for this patient
        hl7_msg = generate_hl7_message(patient_id)
        with open(f"{output_dir}/hl7/{patient_id}.hl7", "w") as f:
            f.write(hl7_msg)

    # Save FHIR data
    with open(f"{output_dir}/fhir/patients.json", "w") as f:
        json.dump(all_patients, f, indent=2)

    with open(f"{output_dir}/fhir/claims.json", "w") as f:
        json.dump(all_claims, f, indent=2)

    print(f"Generated:")
    print(f"  {len(all_patients)} patients  → {output_dir}/fhir/patients.json")
    print(f"  {len(all_claims)} claims     → {output_dir}/fhir/claims.json")
    print(f"  {num_patients} HL7 files   → {output_dir}/hl7/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-patients", type=int, default=500)
    parser.add_argument("--output", type=str, default="data")
    args = parser.parse_args()
    main(args.num_patients, args.output)
