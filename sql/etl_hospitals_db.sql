use database HEALTHCARE__LOCATIONS__STATISTICS__AUSTRALIA__FREE;
use schema HEALTHCARE_AUS_FREE;

CREATE DATABASE IF NOT EXISTS PIGEON_DOLPHIN_PROJECT_DB;
CREATE SCHEMA IF NOT EXISTS PIGEON_DOLPHIN_PROJECT_DB.STAGING;

CREATE OR REPLACE TABLE PIGEON_DOLPHIN_PROJECT_DB.STAGING.hospitals_staging AS
SELECT *
FROM HEALTHCARE__LOCATIONS__STATISTICS__AUSTRALIA__FREE.HEALTHCARE_AUS_FREE.AIHW_HOSPITAL_MAPPING;

USE DATABASE PIGEON_DOLPHIN_PROJECT_DB;
USE SCHEMA PIGEON_DOLPHIN_PROJECT_DB.STAGING;


CREATE SCHEMA IF NOT EXISTS PIGEON_DOLPHIN_PROJECT_DB.NORMALIZED;
USE SCHEMA PIGEON_DOLPHIN_PROJECT_DB.NORMALIZED;

CREATE OR REPLACE TABLE status AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY OPEN_CLOSED) AS status_id,
    OPEN_CLOSED,
    CASE WHEN OPEN_CLOSED = 'Open' THEN 1 ELSE 0 END AS open_closed_bool
FROM (SELECT DISTINCT OPEN_CLOSED FROM STAGING.hospitals_staging WHERE OPEN_CLOSED IS NOT NULL);


CREATE OR REPLACE TABLE location AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY STATE, LATITUDE, LONGITUDE) AS location_id,
    STATE,
    LATITUDE,
    LONGITUDE
FROM (SELECT DISTINCT STATE, LATITUDE, LONGITUDE FROM STAGING.hospitals_staging);

select * from location;
CREATE OR REPLACE TABLE network AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY LOCAL_HOSPITAL_NETWORK) AS network_id,
    LOCAL_HOSPITAL_NETWORK,
    PRIMARY_HEALTH_NETWORK_AREA
FROM (SELECT DISTINCT LOCAL_HOSPITAL_NETWORK, PRIMARY_HEALTH_NETWORK_AREA FROM STAGING.hospitals_staging);


CREATE OR REPLACE TABLE hospital_type AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY TYPE, SECTOR) AS type_id,
    TYPE,
    SECTOR
FROM (SELECT DISTINCT TYPE, SECTOR FROM STAGING.hospitals_staging WHERE TYPE IS NOT NULL);

select * from hospital_type;

CREATE OR REPLACE TABLE hospital AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY stg.CODE) AS hospital_ID,
    stg.CODE,
    stg.NAME,
    stg.DATA_SOURCE,
    stg.DATA_SUPPLIER,
    stg.DATE_UPDATED,
    loc.location_id AS location_location_id,
    stat.status_id AS status_status_id,
    ht.type_id AS hospital_type_type_id,
    net.network_id AS network_network_id
FROM STAGING.hospitals_staging stg
LEFT JOIN location loc ON stg.LATITUDE = loc.LATITUDE AND stg.LONGITUDE = loc.LONGITUDE
LEFT JOIN status stat ON stg.OPEN_CLOSED = stat.OPEN_CLOSED
LEFT JOIN hospital_type ht ON stg.TYPE = ht.TYPE AND IFNULL(stg.SECTOR, '') = IFNULL(ht.SECTOR, '')
LEFT JOIN network net ON IFNULL(stg.LOCAL_HOSPITAL_NETWORK, '') = IFNULL(net.LOCAL_HOSPITAL_NETWORK, '') 
    AND IFNULL(stg.PRIMARY_HEALTH_NETWORK_AREA, '') = IFNULL(net.PRIMARY_HEALTH_NETWORK_AREA, '');


CREATE SCHEMA IF NOT EXISTS PIGEON_DOLPHIN_PROJECT_DB.DIMENSIONAL;
USE SCHEMA PIGEON_DOLPHIN_PROJECT_DB.DIMENSIONAL;

CREATE TABLE dim_hospital AS 
SELECT hospital_ID AS dim_hospital_id, code AS hospital_code, name AS hospital_name, data_source, data_supplier FROM NORMALIZED.hospital;



CREATE OR REPLACE TABLE dim_location AS 
SELECT location_id AS dim_location_id, state, longitude, latitude FROM NORMALIZED.location;

CREATE OR REPLACE TABLE dim_type AS 
SELECT type_id AS dim_type_id, type, sector FROM NORMALIZED.hospital_type;



CREATE OR REPLACE TABLE dim_network AS 
SELECT network_id AS dim_networ_id, local_hospital_network, primary_health_network_area FROM NORMALIZED.network;

CREATE OR REPLACE TABLE dim_status AS 
SELECT status_id AS dim_status_id, open_closed, open_closed_bool FROM NORMALIZED.status;




CREATE OR REPLACE TABLE fact_hospitals AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY h.hospital_ID) AS fact_id, 
    h.hospital_ID AS dim_hospital_id,
    h.hospital_type_type_id AS dim_type_id,
    h.location_location_id AS dim_location_id,
    h.network_network_id AS dim_network_id,
    h.status_status_id AS dim_status_id,
    1 AS hospital_count, 
    RANK() OVER (PARTITION BY l.state ORDER BY h.name) AS rank_in_state,
    COUNT(*) OVER (PARTITION BY l.state) AS total_hospitals_in_state
FROM NORMALIZED.hospital h
LEFT JOIN NORMALIZED.location l ON h.location_location_id = l.location_id;
select * from NORMALIZED.hospital;
select * from fact_hospitals;