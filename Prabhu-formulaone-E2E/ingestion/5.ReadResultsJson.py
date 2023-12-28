# Databricks notebook source
# MAGIC %run "/Prabhu_Projects/Prabhu-formulaone-E2E/includes/configurations"

# COMMAND ----------

# MAGIC %run "/Prabhu_Projects/Prabhu-formulaone-E2E/includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("v_data_source","")
v_data_source = dbutils.widgets.get("v_data_source")    

# COMMAND ----------

dbutils.widgets.text("v_file_date","2021-03-28")
p_file_date = dbutils.widgets.get("v_file_date")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

results_schema = StructType([ StructField("resultId", IntegerType() , False),
								  StructField("raceId", IntegerType() , True),
								  StructField("driverId", IntegerType() , True),
								  StructField("constructorId", IntegerType() , True),
								  StructField("number", IntegerType() , True),
								  StructField("grid", IntegerType() , True),
								  StructField("position", IntegerType() , True),
								  StructField("positionText", StringType() , True),
								  StructField("positionOrder", IntegerType() , True),
								  StructField("points", FloatType() , True),
								  StructField("laps", IntegerType() , True),
								  StructField("time", StringType() , True),
								  StructField("milliseconds", IntegerType() , True),
								  StructField("fastestLap", IntegerType() , True),
								  StructField("rank", IntegerType() , True),
								  StructField("fastestLapTime", StringType() , True),
								  StructField("fastestLapSpeed", FloatType() , True),
								  StructField("statusId", StringType() , True)

								])

# COMMAND ----------

results_df = spark.read.json(f"{mnt_raw}/{p_file_date}/results.json" , schema= results_schema )

# COMMAND ----------

results_df_ingestuon_date_df =  add_ingestion_date(results_df)

# COMMAND ----------

results_transform_df = results_df_ingestuon_date_df.withColumnRenamed("resultId","result_Id")\
                                 .withColumnRenamed("raceId","race_Id")\
                                 .withColumnRenamed("driverId","driver_Id")\
                                 .withColumnRenamed("constructorId","constructor_Id")\
                                 .withColumnRenamed("positionText","position_text")\
                                 .withColumnRenamed("positionOrder","position_order")\
                                 .withColumnRenamed("fastestLap","fastest_lap")\
                                 .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                 .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                 .drop(col('statusId'))\
                                 .withColumn("source_system" , lit(v_data_source) )\
                                 .withColumn("file_date", lit(p_file_date))
                                     
                                 

# COMMAND ----------

#results_transform_df.write.option("path", "dbfs:/mnt/sourceinput/databricks-course/processed/results").mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

overwrite_partition(results_transform_df, 'f1_processed', 'results', 'race_Id')

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC Select race_Id, count(race_Id) cnts from  f1_processed.results
# MAGIC group by race_Id

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


 

# COMMAND ----------


