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

pitstops_schema = StructType ([ StructField("raceId", IntegerType(), False ),
                                StructField("driverId", IntegerType(), True ),
                                StructField("stop", IntegerType(), True ),
                                StructField("lap", IntegerType(), True ),
                                StructField("time", StringType(), True ),
                                StructField("duration", StringType(), True ),
                                StructField("milliseconds", IntegerType(), True )
                                ])

# COMMAND ----------

pitstops_df= spark.read.schema(pitstops_schema).option("multiLine", True ).json(f"{mnt_raw}/{p_file_date}/pit_stops.json")

# COMMAND ----------


pitstops_ingestuon_date_df =  add_ingestion_date(pitstops_df)


# COMMAND ----------

pitstops_transform_df = pitstops_ingestuon_date_df.withColumnRenamed("raceId","race_Id")\
                                   .withColumnRenamed("driverId","driver_Id")\
                                   .withColumn("source_system" , lit(v_data_source) )\
                                    .withColumn("file_date", lit(p_file_date))
                                   

# COMMAND ----------

# pitstops_transform_df.write.option("path", "dbfs:/mnt/sourceinput/databricks-course/processed/pitstop").mode("overwrite").format("parquet").saveAsTable("f1_processed.pitstop")

# COMMAND ----------

overwrite_partition(pitstops_transform_df, 'f1_processed', 'pitstop', 'race_Id')

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC Select race_Id, count(race_Id) cnts from  f1_processed.pitstop
# MAGIC group by race_Id

# COMMAND ----------

dbutils.notebook.exit("Success")
