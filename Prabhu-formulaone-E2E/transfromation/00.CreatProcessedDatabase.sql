-- Databricks notebook source
show databases;

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION '/mnt/sourceinput/databricks-course/processed'

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

DESC DATABASE f1_raw;

-- COMMAND ----------


