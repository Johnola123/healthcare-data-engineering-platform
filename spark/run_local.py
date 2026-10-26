"""Local Spark medallion extension. No Databricks or Delta dependency required.
Usage: python -m spark.run_local --input data/raw --output data/spark
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, Window, functions as F


def run(input_dir: str, output_dir: str):
    spark = (SparkSession.builder.master('local[2]').appName('HealthcarePortfolio')
             .config('spark.sql.shuffle.partitions', '4').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    try:
        bronze = {}
        for entity in ('patients', 'encounters', 'labs'):
            path = Path(input_dir) / f'{entity}.csv'
            if not path.is_file():
                raise FileNotFoundError(f'{path} missing; run python -m src.pipeline --stage all')
            bronze[entity] = spark.read.option('header', True).csv(str(path))
            bronze[entity].write.mode('overwrite').parquet(str(out / 'bronze' / entity))
        def dedup(df, key):
            # Deterministic enough for synthetic unique-key input. QC detects duplicates before dedup.
            return df.dropDuplicates([key])
        p = dedup(bronze['patients'], 'patient_id').select(
            'patient_id', F.col('age').cast('int').alias('age'), F.trim('region').alias('region'))
        e = dedup(bronze['encounters'], 'encounter_id').select(
            'encounter_id','patient_id',F.to_date('encounter_date').alias('encounter_date'),
            F.trim('department').alias('department'),F.col('total_cost').cast('double').alias('total_cost'))
        l = dedup(bronze['labs'], 'lab_id').select(
            'lab_id','encounter_id',F.trim('test_name').alias('test_name'),
            F.col('result_value').cast('double').alias('result_value'))
        silver = {'patients':p,'encounters':e,'labs':l}
        for entity, df in silver.items():
            df.write.mode('overwrite').parquet(str(out / 'silver' / entity))
        daily = e.groupBy('encounter_date','department').agg(
            F.count('*').alias('encounter_count'),F.countDistinct('patient_id').alias('unique_patients'),
            F.round(F.sum('total_cost'),2).alias('total_cost'),F.round(F.avg('total_cost'),2).alias('avg_cost'))
        labs = e.join(l,'encounter_id','inner').groupBy('department','test_name').agg(
            F.count('*').alias('lab_count'),F.round(F.avg('result_value'),2).alias('avg_result'))
        daily.write.mode('overwrite').parquet(str(out / 'gold' / 'daily_utilization'))
        labs.write.mode('overwrite').parquet(str(out / 'gold' / 'department_lab_summary'))
        checks = {
            'unique_patient_ids': bronze['patients'].count() - bronze['patients'].select('patient_id').distinct().count(),
            'unique_encounter_ids': bronze['encounters'].count() - bronze['encounters'].select('encounter_id').distinct().count(),
            'unique_lab_ids': bronze['labs'].count() - bronze['labs'].select('lab_id').distinct().count(),
            'missing_patient_ids': e.filter(F.col('patient_id').isNull()).count(),
            'orphan_encounters': e.join(p,'patient_id','left_anti').count(),
            'orphan_labs': l.join(e,'encounter_id','left_anti').count(),
            'invalid_costs': e.filter(F.col('total_cost').isNull() | (F.col('total_cost')<0)).count(),
            'invalid_dates': e.filter(F.col('encounter_date').isNull()).count(),
            'invalid_ages': p.filter(F.col('age').isNull() | (F.col('age')<0) | (F.col('age')>120)).count(),
            'invalid_labs': l.filter(F.col('result_value').isNull()).count(),
            'missing_gold_rows': int(daily.count()==0),
        }
        silver_count = e.count()
        gold_count = int(daily.agg(F.sum('encounter_count')).first()[0] or 0)
        checks['silver_gold_reconciliation'] = abs(silver_count-gold_count)
        report = {'generated_at_utc': datetime.now(timezone.utc).isoformat(),
                  'counts': {f'bronze_{key}': df.count() for key,df in bronze.items()},
                  'silver_encounters':silver_count,'gold_encounters':gold_count,
                  'checks':checks,'status':'PASS' if all(v==0 for v in checks.values()) else 'FAIL'}
        (out/'spark_quality_report.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report, indent=2))
        if report['status']!='PASS':
            raise ValueError('Spark data-quality checks failed; inspect spark_quality_report.json')
        return report
    finally:
        spark.stop()

if __name__=='__main__':
    ap=argparse.ArgumentParser()
    ap.add_argument('--input',default='data/raw')
    ap.add_argument('--output',default='data/spark')
    args=ap.parse_args()
    run(args.input,args.output)
