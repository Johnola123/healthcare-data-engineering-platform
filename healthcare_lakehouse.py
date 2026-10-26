# Databricks notebook source
# Upload patients.csv, encounters.csv, labs.csv to a Unity Catalog Volume first.
# Run inside a Databricks notebook (spark, dbutils are supplied by Databricks).
# Adapt the catalog, schema, volume and storage to your account.

# COMMAND ----------
CATALOG = 'workspace'    # change to the catalog you can access
SCHEMA = 'default'       # change to your existing or new schema
VOLUME = 'health_portfolio'  # create this Volume in Catalog Explorer
BASE = f'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}'

from pyspark.sql import functions as F

# COMMAND ----------
raw = {}
for name in ('patients','encounters','labs'):
    raw[name] = spark.read.option('header',True).csv(f'{BASE}/{name}.csv')
    raw[name].write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(
        f'{CATALOG}.{SCHEMA}.hc_bronze_{name}')

# COMMAND ----------
p = raw['patients'].dropDuplicates(['patient_id']).select('patient_id',F.col('age').cast('int').alias('age'),F.trim('region').alias('region'))
e = raw['encounters'].dropDuplicates(['encounter_id']).select('encounter_id','patient_id',F.to_date('encounter_date').alias('encounter_date'),F.trim('department').alias('department'),F.col('total_cost').cast('double').alias('total_cost'))
l = raw['labs'].dropDuplicates(['lab_id']).select('lab_id','encounter_id',F.trim('test_name').alias('test_name'),F.col('result_value').cast('double').alias('result_value'))
for name,df in {'patients':p,'encounters':e,'labs':l}.items():
    df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{CATALOG}.{SCHEMA}.hc_silver_{name}')

# COMMAND ----------
daily = e.groupBy('encounter_date','department').agg(F.count('*').alias('encounter_count'),F.countDistinct('patient_id').alias('unique_patients'),F.round(F.sum('total_cost'),2).alias('total_cost'),F.round(F.avg('total_cost'),2).alias('avg_cost'))
summary = e.join(l,'encounter_id').groupBy('department','test_name').agg(F.count('*').alias('lab_count'),F.round(F.avg('result_value'),2).alias('avg_result'))
daily.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{CATALOG}.{SCHEMA}.hc_gold_daily_utilization')
summary.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(f'{CATALOG}.{SCHEMA}.hc_gold_department_lab_summary')

# COMMAND ----------
checks = {
    'unique_patient_ids':raw['patients'].count()-raw['patients'].select('patient_id').distinct().count(),
    'unique_encounter_ids':raw['encounters'].count()-raw['encounters'].select('encounter_id').distinct().count(),
    'unique_lab_ids':raw['labs'].count()-raw['labs'].select('lab_id').distinct().count(),
    'missing_patient_ids':e.filter(F.col('patient_id').isNull()).count(),
    'orphan_encounters':e.join(p,'patient_id','left_anti').count(),
    'orphan_labs':l.join(e,'encounter_id','left_anti').count(),
    'invalid_costs':e.filter(F.col('total_cost').isNull() | (F.col('total_cost')<0)).count(),
    'invalid_dates':e.filter(F.col('encounter_date').isNull()).count(),
    'invalid_ages':p.filter(F.col('age').isNull() | (F.col('age')<0) | (F.col('age')>120)).count(),
    'invalid_labs':l.filter(F.col('result_value').isNull()).count(),
    'missing_gold_rows':int(daily.count()==0),
    'silver_gold_reconciliation':abs(e.count()-int(daily.agg(F.sum('encounter_count')).first()[0] or 0))
}
print('Checks:',checks)
assert all(value==0 for value in checks.values()), f'Quality gate FAILED: {checks}'
print('PASS: Databricks Delta Lake healthcare quality checks')
display(spark.table(f'{CATALOG}.{SCHEMA}.hc_gold_daily_utilization').orderBy('encounter_date','department'))
