# Databricks notebook source
v_result=dbutils.notebook.run('1.ReadCircuitsCsv',0, {'v_data_source':'Databricks'})
v_result

# COMMAND ----------

v_result=dbutils.notebook.run('2.ReadRacesCsv',0, {'v_data_source':'Databricks'})
v_result

# COMMAND ----------

v_result=dbutils.notebook.run('3.ReadConstructersCsv',0, {'v_data_source':'Databricks'})
v_result

# COMMAND ----------

v_result=dbutils.notebook.run('4.ReadDriversJson',0, {'v_data_source':'Databricks'})
v_result

# COMMAND ----------

v_result=dbutils.notebook.run('5.ReadResultsJson',0, {'v_data_source':'Databricks'})
v_result

# COMMAND ----------

v_result=dbutils.notebook.run('6.ReadPitStopsMultiLineJson',0, {'v_data_source':'Databricks'})
v_result

# COMMAND ----------

v_result=dbutils.notebook.run('7.ReadLapTimesMultipleCsv',0, {'v_data_source':'dbutils.notebook'})
v_result

# COMMAND ----------

v_result=dbutils.notebook.run('8.ReadQualifyingMultipleMultilineJson',0, {'v_data_source':'dbutils.notebook'})
v_result

# COMMAND ----------


