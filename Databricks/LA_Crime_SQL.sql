/*
 * ER/Studio Data Architect SQL Code Generation
 * Project :      group_9.DM1
 *
 * Date Created : Saturday, November 22, 2025 00:29:39
 * Target DBMS : Databricks
 */

/* 
 * TABLE: DIM_ARREST_STATUS 
 */

CREATE TABLE DIM_ARREST_STATUS
(
    status_key     int          NOT NULL,
    status_code    string,
    is_arrest      boolean,
    status_desc    string,
    arrest_type    string,
    CONSTRAINT PK_DIM_STATUS PRIMARY KEY (status_key) 
)
;

/* 
 * TABLE: DIM_CRIME 
 */

CREATE TABLE DIM_CRIME
(
    crime_key      int         NOT NULL,
    crm_cd         string,
    crm_cd_desc    string,
    CONSTRAINT PK_DIM_CRIME PRIMARY KEY (crime_key) 
)
;

/* 
 * TABLE: DIM_DATE 
 */

CREATE TABLE DIM_DATE
(
    date_key            int         NOT NULL,
    `-- e.g. 20241117`  date        NOT NULL,
    year                int         NOT NULL,
    quarter             int         NOT NULL,
    month_num           int         NOT NULL,
    month_name          string      NOT NULL,
    week_of_year        int,
    day_of_week         int,
    `-- 1..7`           string,
    is_weekend          int,
    CONSTRAINT PK_DIM_DATE PRIMARY KEY (date_key) 
)
;

/* 
 * TABLE: DIM_LOCATION 
 */

CREATE TABLE DIM_LOCATION
(
    location_key          int                NOT NULL,
    area_name             string,
    area                  int,
    location              string,
    `-- rounded address`  string,
    lat                   decimal(9, 6),
    lon                   decimal(9, 6),
    rpt_dist_no           string,
    CONSTRAINT PK_DIM_LOCATION PRIMARY KEY (location_key) 
)
;

/* 
 * TABLE: DIM_TIME 
 */

CREATE TABLE DIM_TIME
(
    time_key             int         NOT NULL,
    `-- surrogate 1..N`  string,
    minute               int,
    time_band            string,
    CONSTRAINT PK_DIM_TIME PRIMARY KEY (time_key) 
)
;

/* 
 * TABLE: DIM_VICTIM 
 */

CREATE TABLE DIM_VICTIM
(
    victim_key    int         NOT NULL,
    vict_age      int,
    age_band      string,
    vict_sex      string,
    CONSTRAINT PK_DIM_VICTIM PRIMARY KEY (victim_key) 
)
;

/* 
 * TABLE: DIM_WEAPON 
 */

CREATE TABLE DIM_WEAPON
(
    weapon_key        int         NOT NULL,
    weapon_used_cd    string,
    weapon_desc       string,
    CONSTRAINT PK_DIM_WEAPON PRIMARY KEY (weapon_key) 
)
;

/* 
 * TABLE: FACT_CRIME_INCIDENT 
 */

CREATE TABLE FACT_CRIME_INCIDENT
(
    crime_id              int         NOT NULL,
    dr_no                 string      NOT NULL,
    date_occ_key          int,
    time_key              int,
    location_key          int,
    crime_key             int,
    victim_key            int,
    weapon_key            int,
    status_key            int,
    is_arrest             bigint,
    is_adult_arrest       string,
    is_juvenile_arrest    string,
    CONSTRAINT PK_FACT_CRIME PRIMARY KEY (crime_id) 
)
;

/* 
 * TABLE: FACT_CRIME_INCIDENT 
 */

ALTER TABLE FACT_CRIME_INCIDENT ADD CONSTRAINT FK_FACT_CRIME_CODE 
    FOREIGN KEY (crime_key)
    REFERENCES DIM_CRIME
;

ALTER TABLE FACT_CRIME_INCIDENT ADD CONSTRAINT FK_FACT_DATE_RPTD 
    FOREIGN KEY (date_occ_key)
    REFERENCES DIM_DATE
;

ALTER TABLE FACT_CRIME_INCIDENT ADD CONSTRAINT FK_FACT_LOCATION 
    FOREIGN KEY (location_key)
    REFERENCES DIM_LOCATION
;

ALTER TABLE FACT_CRIME_INCIDENT ADD CONSTRAINT FK_FACT_STATUS 
    FOREIGN KEY (status_key)
    REFERENCES DIM_ARREST_STATUS
;

ALTER TABLE FACT_CRIME_INCIDENT ADD CONSTRAINT FK_FACT_TIME 
    FOREIGN KEY (time_key)
    REFERENCES DIM_TIME
;

ALTER TABLE FACT_CRIME_INCIDENT ADD CONSTRAINT FK_FACT_VICTIM 
    FOREIGN KEY (victim_key)
    REFERENCES DIM_VICTIM
;

ALTER TABLE FACT_CRIME_INCIDENT ADD CONSTRAINT FK_FACT_WEAPON 
    FOREIGN KEY (weapon_key)
    REFERENCES DIM_WEAPON
;


