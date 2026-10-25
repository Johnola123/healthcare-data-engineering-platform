-- Bronze records preserve source fields; silver handles types and deduplication.
CREATE OR REPLACE TABLE silver_patients AS
SELECT DISTINCT patient_id, TRY_CAST(age AS INTEGER) AS age, TRIM(region) AS region
FROM bronze_patients
QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY patient_id) = 1;

CREATE OR REPLACE TABLE silver_encounters AS
SELECT encounter_id, patient_id, TRY_CAST(encounter_date AS DATE) AS encounter_date,
       TRIM(department) AS department, TRY_CAST(total_cost AS DOUBLE) AS total_cost
FROM bronze_encounters
QUALIFY ROW_NUMBER() OVER (PARTITION BY encounter_id ORDER BY encounter_id) = 1;

CREATE OR REPLACE TABLE silver_labs AS
SELECT lab_id, encounter_id, TRIM(test_name) AS test_name,
       TRY_CAST(result_value AS DOUBLE) AS result_value
FROM bronze_labs
QUALIFY ROW_NUMBER() OVER (PARTITION BY lab_id ORDER BY lab_id) = 1;

-- Gold analytic marts for dashboarding and quality reporting.
CREATE OR REPLACE TABLE gold_daily_utilization AS
SELECT encounter_date, department, COUNT(*) AS encounter_count,
       COUNT(DISTINCT patient_id) AS unique_patients,
       ROUND(SUM(total_cost),2) AS total_cost,
       ROUND(AVG(total_cost),2) AS avg_cost
FROM silver_encounters
GROUP BY 1,2;

CREATE OR REPLACE TABLE gold_department_lab_summary AS
SELECT e.department, l.test_name, COUNT(*) AS lab_count,
       ROUND(AVG(l.result_value),2) AS avg_result
FROM silver_encounters e INNER JOIN silver_labs l USING (encounter_id)
GROUP BY 1,2;
