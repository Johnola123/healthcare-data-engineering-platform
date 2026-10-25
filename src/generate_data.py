"""Deterministic synthetic records. NOT real patient data."""
import csv
import random
from datetime import date, timedelta
from pathlib import Path


def generate(destination: Path, n_patients: int = 250, n_encounters: int = 1200, seed: int = 42):
    destination.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    patients = []
    for pid in range(1, n_patients + 1):
        patients.append({'patient_id': f'P{pid:05d}', 'age': rng.randint(18, 88),
                         'region': rng.choice(['North', 'South', 'East', 'West'])})
    encounters = []
    labs = []
    start = date(2026, 1, 1)
    for i in range(1, n_encounters + 1):
        pid = rng.choice(patients)['patient_id']
        dt = start + timedelta(days=rng.randrange(240))
        cost = round(rng.uniform(50, 1800), 2)
        encounters.append({'encounter_id': f'E{i:06d}', 'patient_id': pid,
                          'encounter_date': dt.isoformat(), 'department': rng.choice(['General', 'Oncology', 'Cardiology']),
                          'total_cost': cost})
        labs.append({'lab_id': f'L{i:06d}', 'encounter_id': f'E{i:06d}',
                     'test_name': 'hemoglobin', 'result_value': round(rng.uniform(8, 18), 1)})
    for filename, rows in [('patients.csv', patients), ('encounters.csv', encounters), ('labs.csv', labs)]:
        with (destination / filename).open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader(); writer.writerows(rows)
    return {'patients': len(patients), 'encounters': len(encounters), 'labs': len(labs)}
