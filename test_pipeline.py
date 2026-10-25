import json
import duckdb
import pytest
from src import pipeline


def test_deterministic_ingest(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, 'RAW', tmp_path / 'raw')
    first = pipeline.ingest()
    hashes = json.loads((pipeline.RAW / 'manifest.json').read_text())['sha256']
    second = pipeline.ingest()
    assert first == second == {'patients': 250, 'encounters': 1200, 'labs': 1200}
    assert hashes == json.loads((pipeline.RAW / 'manifest.json').read_text())['sha256']


def test_end_to_end_and_idempotency(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, 'RAW', tmp_path / 'raw')
    monkeypatch.setattr(pipeline, 'ROOT', tmp_path)
    (tmp_path / 'sql').mkdir()
    original_sql = (__import__('pathlib').Path(__file__).resolve().parents[1] / 'sql' / 'models.sql').read_text()
    (tmp_path / 'sql' / 'models.sql').write_text(original_sql)
    db = tmp_path / 'test.duckdb'
    pipeline.run('all', db)
    conn = duckdb.connect(str(db))
    n1 = conn.execute('SELECT COUNT(*) FROM silver_encounters').fetchone()[0]
    assert n1 == 1200
    assert conn.execute('SELECT SUM(encounter_count) FROM gold_daily_utilization').fetchone()[0] == 1200
    conn.close()
    pipeline.run('all', db)
    conn = duckdb.connect(str(db))
    assert conn.execute('SELECT COUNT(*) FROM silver_encounters').fetchone()[0] == n1
    assert json.loads((tmp_path / 'data' / 'processed' / 'quality_report.json').read_text())['status'] == 'PASS'
    conn.close()


def test_quality_gate_rejects_bad_data(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, 'ROOT', tmp_path)
    (tmp_path / 'data' / 'processed').mkdir(parents=True)
    conn = duckdb.connect()
    conn.execute("CREATE TABLE silver_patients AS SELECT 'P1'::VARCHAR patient_id, 35::INTEGER age")
    conn.execute("CREATE TABLE silver_encounters AS SELECT 'E1'::VARCHAR encounter_id, 'MISSING'::VARCHAR patient_id, CURRENT_DATE encounter_date, -1::DOUBLE total_cost")
    conn.execute("CREATE TABLE silver_labs AS SELECT 'L1'::VARCHAR lab_id, 'E1'::VARCHAR encounter_id, 11::DOUBLE result_value")
    conn.execute("CREATE TABLE gold_daily_utilization AS SELECT 1 x")
    with pytest.raises(ValueError, match='Data quality gate failed'):
        pipeline.validate(conn)
    conn.close()
