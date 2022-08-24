-- Databricks notebook source
--use tpcds1gb;

--show create table ship_mode;

USE msh_dlt_tpcds;

DROP TABLE IF EXISTS ship_mode;
CREATE TABLE ship_mode DEEP CLONE tpcds1gb.ship_mode;

DROP TABLE IF EXISTS ship_mode_staging;
CREATE TABLE ship_mode_staging ( 
		sm_ship_mode_sk INT, 
		sm_ship_mode_id STRING, 
		sm_type STRING, 
		sm_code STRING, 
		sm_carrier STRING, 
		sm_contract STRING) 
USING delta;

-- COMMAND ----------

-- we will add a new ship_mode and update another one
INSERT INTO ship_mode_staging VALUES(1, 'AAAAAAAABAAAAAAA', 'EXPRESS', 'AIR', 'UPS', 'M');
INSERT INTO ship_mode_staging VALUES(0, 'AAAAAAAAAAAAAAAA', 'PREMIUM', 'AIR', 'LUFTHANSA CARGO', '---');

-- COMMAND ----------

SELECT * FROM ship_mode ORDER BY sm_ship_mode_sk;

-- COMMAND ----------

SELECT * FROM ship_mode_staging ORDER BY sm_ship_mode_sk;

-- COMMAND ----------

-- Run MERGE
MERGE INTO ship_mode target
   USING ship_mode_staging source
			ON source.sm_ship_mode_sk=target.sm_ship_mode_sk
   WHEN MATCHED THEN UPDATE SET * 
   WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- Check the first 2 records
SELECT * FROM ship_mode ORDER BY sm_ship_mode_sk;

-- ID for UPS ship mode should be changed
-- a new ship mode should be add

-- COMMAND ----------


