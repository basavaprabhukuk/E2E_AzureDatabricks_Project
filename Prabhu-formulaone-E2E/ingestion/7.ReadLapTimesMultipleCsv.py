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

lapTimes_schema = StructType([ StructField("raceId", IntegerType(), False ),
                                   StructField("driverId", IntegerType(), True ),
                                   StructField("lap", IntegerType() , True ),
                                   StructField("position", IntegerType(), True ),
                                   StructField("time", StringType(), True ),
                                   StructField("milliseconds", StringType(), True )
])

# COMMAND ----------

lapTimes_df = spark.read.schema(lapTimes_schema).csv(f"{mnt_raw}/{p_file_date}/lap_times/")

# COMMAND ----------


lapTimes_df_ingestuon_date_df =  add_ingestion_date(lapTimes_df)

# COMMAND ----------

lapTimes_trans_df = lapTimes_df_ingestuon_date_df.withColumnRenamed("raceId","race_Id")\
                               .withColumnRenamed("driverId","driver_Id")\
                               .withColumn("source_system" , lit(v_data_source) )\
                                .withColumn("file_date", lit(p_file_date))
                               
                                   


# COMMAND ----------

# lapTimes_trans_df.write.option("path", "dbfs:/mnt/sourceinput/databricks-course/processed/lap_times").mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

overwrite_partition(lapTimes_trans_df, 'f1_processed', 'lap_times', 'race_Id')

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC Select race_Id, count(race_Id) cnts from  f1_processed.lap_times
# MAGIC group by race_Id

# COMMAND ----------

dbutils.notebook.exit("Success")
