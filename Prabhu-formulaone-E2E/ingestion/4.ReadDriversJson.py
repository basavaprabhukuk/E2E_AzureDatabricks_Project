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

name_schema = StructType([ StructField("forename",StringType(), False),
                           StructField("surname",StringType(), False)
])

# COMMAND ----------

drivers_schema = StructType([StructField("driverId",IntegerType(), False),
                             StructField("driverRef",StringType(), False),
                             StructField("number",IntegerType(), False),
                             StructField("code",StringType(), False),
                             StructField("name",name_schema, False),
                             StructField("dob",DateType(), False),
                             StructField("nationality",StringType(), False),
                             StructField("url",StringType(), False)
])

# COMMAND ----------

drivers_df = spark.read.json(f"{mnt_raw}/{p_file_date}/drivers.json" , schema= drivers_schema )

# COMMAND ----------

drivers_df_inges_date= add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_trans_df = drivers_df_inges_date.withColumnRenamed("driverId", "driver_id")\
                             .withColumnRenamed("driverRef", "driver_ref")\
                             .drop(col('url'))\
                             .withColumn("fullname", concat( col('name.forename'), lit(' ') , col('name.surname')))\
                             .drop('name')\
                             .withColumn("source_system" , lit(v_data_source) )\
                             .withColumn("file_date" , lit(p_file_date) )

                             
                             
                              

# COMMAND ----------

drivers_trans_df.write.option("path", "dbfs:/mnt/sourceinput/databricks-course/processed/drivers").mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.parquet(f"{mnt_processed}/drivers").head(10))

# COMMAND ----------

dbutils.notebook.exit("Success")
