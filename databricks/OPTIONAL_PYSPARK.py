# Databricks/PySpark extension. Run in a Databricks notebook after uploading the
# generated CSVs to a Volume; update base_path below. Not run by local CLI.
from pyspark.sql import functions as F
base_path = "/Volumes/main/default/health_portfolio"
raw_encounters = spark.read.option("header", True).csv(f"{base_path}/encounters.csv")
silver = (raw_encounters.dropDuplicates(["encounter_id"])
          .withColumn("encounter_date", F.to_date("encounter_date"))
          .withColumn("total_cost", F.col("total_cost").cast("double")))
silver.write.format("delta").mode("overwrite").saveAsTable("main.default.portfolio_encounters_silver")
(silver.groupBy("encounter_date", "department")
 .agg(F.count("*").alias("encounter_count"), F.sum("total_cost").alias("total_cost"))
 .write.format("delta").mode("overwrite").saveAsTable("main.default.portfolio_daily_utilization_gold"))
