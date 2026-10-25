"""Idempotent local medallion ETL with explicit data quality gates."""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb
from src.generate_data import generate

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'data' / 'raw'
DB = ROOT / 'data' / 'processed' / 'health_analytics.duckdb'


def ingest():
    counts = generate(RAW)
    manifest = {}
    for filename in ('patients.csv', 'encounters.csv', 'labs.csv'):
        contents = (RAW / filename).read_bytes()
        manifest[filename] = hashlib.sha256(contents).hexdigest()
    (RAW / 'manifest.json').write_text(json.dumps({'counts': counts, 'sha256': manifest}, indent=2))
    return counts


def bronze(conn):
    for entity in ('patients', 'encounters', 'labs'):
        file = (RAW / f'{entity}.csv').as_posix()
        if not Path(file).exists():
            raise FileNotFoundError(f'Missing source: {file}. Run ingest first.')
        conn.execute(f"CREATE OR REPLACE TABLE bronze_{entity} AS SELECT * FROM read_csv_auto(?, all_varchar=true)", [file])


def silver_gold(conn):
    conn.execute((ROOT / 'sql' / 'models.sql').read_text())


def validate(conn):
    checks = {
        'unique_patient_ids': 'SELECT COUNT(*) - COUNT(DISTINCT patient_id) FROM silver_patients',
        'unique_encounter_ids': 'SELECT COUNT(*) - COUNT(DISTINCT encounter_id) FROM silver_encounters',
        'unique_lab_ids': 'SELECT COUNT(*) - COUNT(DISTINCT lab_id) FROM silver_labs',
        'missing_patient_ids': 'SELECT COUNT(*) FROM silver_encounters WHERE patient_id IS NULL',
        'orphan_encounters': 'SELECT COUNT(*) FROM silver_encounters e LEFT JOIN silver_patients p USING(patient_id) WHERE p.patient_id IS NULL',
        'orphan_labs': 'SELECT COUNT(*) FROM silver_labs l LEFT JOIN silver_encounters e USING(encounter_id) WHERE e.encounter_id IS NULL',
        'invalid_costs': 'SELECT COUNT(*) FROM silver_encounters WHERE total_cost IS NULL OR total_cost < 0',
        'invalid_dates': 'SELECT COUNT(*) FROM silver_encounters WHERE encounter_date IS NULL',
        'invalid_ages': 'SELECT COUNT(*) FROM silver_patients WHERE age IS NULL OR age < 0 OR age > 120',
        'invalid_labs': 'SELECT COUNT(*) FROM silver_labs WHERE result_value IS NULL',
        'missing_gold_rows': 'SELECT CASE WHEN COUNT(*)=0 THEN 1 ELSE 0 END FROM gold_daily_utilization',
    }
    results = {name: int(conn.execute(sql).fetchone()[0]) for name, sql in checks.items()}
    report = {'checked_at_utc': datetime.now(timezone.utc).isoformat(), 'checks': results,
              'status': 'PASS' if all(v == 0 for v in results.values()) else 'FAIL'}
    ROOT.joinpath('data','processed').mkdir(parents=True, exist_ok=True)
    (ROOT / 'data' / 'processed' / 'quality_report.json').write_text(json.dumps(report, indent=2))
    if report['status'] == 'FAIL':
        raise ValueError(f'Data quality gate failed: {results}')
    return report


def run(stage='all', db_path=DB):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    if stage in ('all','ingest'):
        ingest()
    if stage == 'ingest':
        return
    conn = duckdb.connect(str(db_path))
    try:
        if stage in ('all','bronze'):
            bronze(conn)
        if stage in ('all','model'):
            silver_gold(conn)
        if stage in ('all','quality'):
            print(json.dumps(validate(conn), indent=2))
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--stage', choices=['all','ingest','bronze','model','quality'], default='all')
    parser.add_argument('--db', default=str(DB))
    args = parser.parse_args()
    run(args.stage, args.db)
