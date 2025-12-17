-- Databricks notebook source
CREATE OR REPLACE STREAMING TABLE bronze_la_crime
TBLPROPERTIES(
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  dr_no,
  time_occ,
  area_name,
  rpt_dist_no,
  crm_cd,
  crm_cd_desc,
  mocodes,
  vict_sex,
  vict_descent,
  premis_cd,
  premis_desc,
  weapon_desc,
  status,
  status_desc,
  crm_cd_2,
  crm_cd_3,
  crm_cd_4,
  location,
  cross_street,
  lat,
  lon,
  date_rptd,
  date_occ,
  victim_age,
  age_band,
  is_arrest,
  is_adult_arrest,
  is_juvenile_arrest,
  weapon_used_cd,
  area_code,
  crm_cd_merged,

  current_timestamp() AS load_dt,
  _metadata.file_path  AS source_file_path,
  _metadata.file_name  AS source_file_name

FROM STREAM cloud_files(
  '/Volumes/workspace/la_crime/datastore',  
  'csv',
  map(
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.schemaLocation', '/Volumes/workspace/la_crime/datastore/bronze_la_crime_schema',
    'header', 'true'
  )
);


-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_cleansed_la_crime
AS
WITH base AS (
  SELECT
    dr_no,
    time_occ,
    area_name,
    rpt_dist_no,
    crm_cd,
    crm_cd_desc,
    mocodes,
    vict_sex,
    vict_descent,
    premis_cd,
    premis_desc,
    weapon_desc,
    status,
    status_desc,
    crm_cd_2,
    crm_cd_3,
    crm_cd_4,
    location,
    cross_street,
    lat,
    lon,
    date_rptd,
    date_occ,
    victim_age,
    age_band,
    is_arrest,
    weapon_used_cd,
    area_code,
    crm_cd_merged,
    load_dt,
    source_file_path,
    source_file_name,

    CAST(time_occ AS STRING) AS time_str
  FROM STREAM(bronze_la_crime)
)

SELECT
  dr_no,
  time_occ,
  CASE
    WHEN time_str IS NULL OR time_str = '' THEN NULL
    WHEN length(time_str) = 4 THEN CAST(substring(time_str, 1, 2) AS INT)
    WHEN length(time_str) = 3 THEN CAST(substring(time_str, 1, 1) AS INT)
    WHEN length(time_str) <= 2 THEN 0
    ELSE NULL
END AS hours,

CASE
    WHEN time_str IS NULL OR time_str = '' THEN NULL
    WHEN length(time_str) = 4 THEN CAST(substring(time_str, 3, 2) AS INT)
    WHEN length(time_str) = 3 THEN CAST(substring(time_str, 2, 2) AS INT)
    WHEN length(time_str) <= 2 THEN CAST(time_str AS INT)
    ELSE NULL
END AS minutes,

  area_name,
  rpt_dist_no,
  crm_cd,
  crm_cd_desc,
  mocodes,
  vict_sex,
  vict_descent,
  premis_cd,
  premis_desc,
  weapon_desc,
  status,
  status_desc,
  crm_cd_2,
  crm_cd_3,
  crm_cd_4,
  location,
  cross_street,
  lat,
  lon,
  date_rptd,
  date_occ,
  victim_age,
  age_band,
  is_arrest,
  weapon_used_cd,
  area_code,
  crm_cd_merged,
  load_dt,
  source_file_path,
  source_file_name
FROM base;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_date
AS
WITH days AS (
  SELECT explode(
    sequence(
      date_sub(current_date(), 6 * 365),  
      current_date(),
      interval 1 day
    )
  ) AS d
)
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,

  CAST(d AS DATE)                         AS full_date,

  year(d)                                 AS year,
  quarter(d)                              AS quarter,

  month(d)                                AS month_num,
  date_format(d, 'MMMM')                  AS month_name,
  date_format(d, 'yyyy-MM')               AS year_month,

  dayofmonth(d)                           AS day,
  dayofweek(d)                            AS day_of_week,   

  CASE
    WHEN dayofweek(d) IN (1, 7) THEN 'Y'
    ELSE 'N'
  END                                     AS is_weekend
FROM days;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_time
AS
WITH hm AS (
  SELECT
    h.hour,
    m.minute
  FROM (SELECT explode(sequence(0, 23, 1)) AS hour) h
  CROSS JOIN (SELECT explode(sequence(0, 59, 1)) AS minute) m
)
SELECT
    (hour * 100) + minute       AS time_key,
    hour,
    minute,

    CASE
        WHEN hour BETWEEN 0  AND 5  THEN 'Night'
        WHEN hour BETWEEN 6  AND 11 THEN 'Morning'
        WHEN hour BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN hour BETWEEN 17 AND 20 THEN 'Evening'
        WHEN hour BETWEEN 21 AND 23 THEN 'Late Night'
        ELSE 'Unknown'
    END AS part_of_day
FROM hm;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_location
AS
WITH agg AS (
  SELECT
    area_code,
    area_name,
    rpt_dist_no,
    location,
    cross_street,
    CAST(ROUND(AVG(lat),6)AS decimal(9,6)) AS latitude,
    CAST(ROUND(AVG(lon),6)AS decimal(9,6)) AS longitude
  FROM LIVE.silver_cleansed_la_crime
  WHERE area_code IS NOT NULL
    AND area_name IS NOT NULL
  GROUP BY
    area_code,
    area_name,
    rpt_dist_no,
    location,
    cross_street
)
SELECT
  DENSE_RANK() OVER (
    ORDER BY
      area_code,
      area_name,
      rpt_dist_no,
      location
  ) AS location_key,
  area_code,
  area_name,
  rpt_dist_no,
  location,
  cross_street,
  latitude,
  longitude
FROM agg;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_crime
AS
WITH primary_codes AS (
    SELECT DISTINCT
        TRIM(crm_cd)      AS crm_cd,
        TRIM(crm_cd_desc) AS crm_cd_desc
    FROM LIVE.silver_cleansed_la_crime
    WHERE crm_cd IS NOT NULL
      AND crm_cd_desc IS NOT NULL
),

secondary_codes AS (
    SELECT DISTINCT TRIM(crm_cd_2) AS crm_cd
    FROM LIVE.silver_cleansed_la_crime
    WHERE crm_cd_2 IS NOT NULL

    UNION

    SELECT DISTINCT TRIM(crm_cd_3) AS crm_cd
    FROM LIVE.silver_cleansed_la_crime
    WHERE crm_cd_3 IS NOT NULL

    UNION

    SELECT DISTINCT TRIM(crm_cd_4) AS crm_cd
    FROM LIVE.silver_cleansed_la_crime
    WHERE crm_cd_4 IS NOT NULL
),

all_codes AS (
    SELECT
        p.crm_cd,
        p.crm_cd_desc
    FROM primary_codes p

    UNION

    SELECT
        s.crm_cd,
        'Additional Crime' AS crm_cd_desc
    FROM secondary_codes s
    LEFT ANTI JOIN primary_codes p
      ON p.crm_cd = s.crm_cd      
)

SELECT
    DENSE_RANK() OVER (ORDER BY crm_cd) AS crime_key,
    crm_cd,
    crm_cd_desc
FROM all_codes;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_victim
AS
WITH valid_victims AS (
    SELECT DISTINCT
        victim_age,
        age_band,
        vict_sex as victim_sex
    FROM  LIVE.silver_cleansed_la_crime
    WHERE victim_age IS NOT NULL
      AND age_band IS NOT NULL
      AND vict_sex IS NOT NULL
)

SELECT
    CONCAT(CAST(victim_age AS STRING), victim_sex) AS victim_key,
    victim_age,
    age_band,
    victim_sex
FROM valid_victims;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_weapon
AS
WITH valid_weapons AS (
    SELECT DISTINCT
        CAST(weapon_used_cd AS STRING) AS weapon_used_cd,
        TRIM(weapon_desc)              AS weapon_desc
    FROM LIVE.silver_cleansed_la_crime
    WHERE weapon_used_cd IS NOT NULL
      AND weapon_desc IS NOT NULL
      AND TRIM(weapon_desc) != ''
)

SELECT
    DENSE_RANK() OVER (ORDER BY weapon_used_cd, weapon_desc) AS weapon_key,
    weapon_used_cd,
    weapon_desc
FROM valid_weapons;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_arrest_status
AS
WITH base AS (
  SELECT DISTINCT
    status,
    status_desc
  FROM LIVE.silver_cleansed_la_crime
  WHERE status IS NOT NULL
    AND status_desc IS NOT NULL
)

SELECT
  DENSE_RANK() OVER (ORDER BY status, status_desc) AS arrest_status_key,
  status,
  status_desc
FROM base;

-- COMMAND ----------

-- GOLD DIM_DATE
CREATE OR REFRESH LIVE TABLE dim_date
TBLPROPERTIES ("quality" = "gold")
AS
SELECT *
FROM LIVE.silver_date;


-- GOLD DIM_TIME
CREATE OR REFRESH LIVE TABLE dim_time
TBLPROPERTIES ("quality" = "gold")
AS
SELECT *
FROM LIVE.silver_time;


-- GOLD DIM_LOCATION 
CREATE OR REFRESH LIVE TABLE dim_location
TBLPROPERTIES ("quality" = "gold")
AS
SELECT *
FROM LIVE.silver_location;


-- GOLD DIM_CRIME
CREATE OR REFRESH LIVE TABLE dim_crime
TBLPROPERTIES ("quality" = "gold")
AS
SELECT *
FROM LIVE.silver_crime;


-- GOLD DIM_VICTIM
CREATE OR REFRESH LIVE TABLE dim_victim
TBLPROPERTIES ("quality" = "gold")
AS
SELECT *
FROM LIVE.silver_victim;


-- GOLD DIM_WEAPON
CREATE OR REFRESH LIVE TABLE dim_weapon
TBLPROPERTIES ("quality" = "gold")
AS
SELECT *
FROM LIVE.silver_weapon;


-- GOLD DIM_ARREST_STATUS 
CREATE OR REFRESH LIVE TABLE dim_arrest_status
TBLPROPERTIES ("quality" = "gold")
AS
SELECT *
FROM LIVE.silver_arrest_status;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_crime_incident
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
DENSE_RANK() OVER (ORDER BY s.dr_no,s.date_occ) AS crime_incident_id,
  s.dr_no                                              AS dr_no,
  d_rpt.date_key                                       AS date_rptd_key,
  d.date_key                                           AS date_occ_key,
  t.time_key                                           AS time_key,
  l.location_key                                       AS location_key,
  c.crime_key                                          AS crime_key,
  v.victim_key                                         AS victim_key,
  w.weapon_key                                         AS weapon_key,
  a.arrest_status_key                                  AS arrest_status_key,
  s.is_arrest                                          AS is_arrest
FROM LIVE.silver_cleansed_la_crime s

-- date (DATE_OCC)
LEFT JOIN LIVE.dim_date d
  ON s.date_occ = d.full_date

-- time (hours from TIME_OCC)
LEFT JOIN LIVE.dim_time t
  ON s.time_occ = t.time_key

LEFT JOIN LIVE.dim_date d_rpt
  ON s.date_rptd = d_rpt.full_date

-- location
LEFT JOIN LIVE.dim_location l
  ON  s.area_code    = l.area_code
  AND s.area_name    = l.area_name
  AND s.rpt_dist_no  = l.rpt_dist_no
  AND s.location     = l.location
  AND TRIM(s.cross_street) <=> l.cross_street 

-- crime
LEFT JOIN LIVE.dim_crime c
  ON s.crm_cd = c.crm_cd

-- victim
LEFT JOIN LIVE.dim_victim v
  ON  s.victim_age = v.victim_age
  AND s.vict_sex   = v.victim_sex

-- weapon
LEFT JOIN LIVE.dim_weapon w
  ON  CAST(s.weapon_used_cd AS STRING) = w.weapon_used_cd
  AND s.weapon_desc                   = w.weapon_desc

-- arrest status
LEFT JOIN LIVE.dim_arrest_status a
  ON  s.status      = a.status
  AND s.status_desc = a.status_desc;
