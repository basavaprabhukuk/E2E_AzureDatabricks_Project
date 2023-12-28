-- Databricks notebook source
DROP DATABASE IF EXISTS f1_presentation

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION '/mnt/sourceinput/databricks-course/presentation'

-- COMMAND ----------

show databases;

-- COMMAND ----------

desc database extended f1_raw

-- COMMAND ----------


