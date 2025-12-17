-- Databricks notebook source
USE CATALOG workspace;
USE SCHEMA la_crime;

SELECT
  s.area_code, s.area_name, s.rpt_dist_no, s.location, s.cross_street,
  l.location_key
FROM silver_cleansed_la_crime s
LEFT JOIN silver_location l
  ON  s.area_code    = l.area_code
  AND s.area_name    = l.area_name
  AND s.rpt_dist_no  = l.rpt_dist_no
  AND s.location     = l.location
  AND s.cross_street = l.cross_street
WHERE l.location_key IS NULL
LIMIT 50;

-- COMMAND ----------

select time_occ from workspace.la_crime.bronze_la_crime where time_occ < 100 ; 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.la_crime.dim_time")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/la_crime/dataextract/dim_time")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.la_crime.dim_date")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/la_crime/dataextract/dim_date")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.la_crime.dim_crime")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/la_crime/dataextract/dim_crime")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.la_crime.dim_arrest_status")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/la_crime/dataextract/dim_arrest_status")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.la_crime.dim_location")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/la_crime/dataextract/dim_location")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.la_crime.dim_weapon")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/la_crime/dataextract/dim_weapon")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.la_crime.dim_victim")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/la_crime/dataextract/dim_victim")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.la_crime.fact_crime_incident")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/la_crime/dataextract/fact_crime_incident")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("workspace.la_crime.silver_cleansed_la_crime")
-- MAGIC
-- MAGIC df.coalesce(1).write.mode("overwrite") \
-- MAGIC   .option("header", "true") \
-- MAGIC   .csv("/Volumes/workspace/la_crime/dataextract/silver_cleansed_la_crime")