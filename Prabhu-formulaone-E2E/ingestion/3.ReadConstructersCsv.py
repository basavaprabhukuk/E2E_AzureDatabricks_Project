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

constructers_schema = StructType([ StructField("constructorId", IntegerType(), False ),
                                   StructField("constructorRef", StringType(), False ),
                                   StructField("name", StringType() , False ),
                                   StructField("nationality", StringType(), False ),
                                   StructField("url", StringType(), False )
])

# COMMAND ----------

constructers_df = spark.read.json(f"{mnt_raw}/{p_file_date}/constructors.json"  , schema= constructers_schema )

# COMMAND ----------

constructers_trans_df_1=add_ingestion_date(constructers_df)

# COMMAND ----------

constructers_trans_df = constructers_trans_df_1.withColumnRenamed("constructorId","constructor_Id")\
                                               .withColumnRenamed("constructorRef","constructor_ref")\
                                               .drop(col('url'))\
                                               .withColumn("source_system" , lit(v_data_source) )\
                                               .withColumn("file_date" , lit(p_file_date) )
                                       

# COMMAND ----------

constructers_trans_df.write.option("path", "dbfs:/mnt/sourceinput/databricks-course/processed/constructer").mode("overwrite").format("parquet").saveAsTable("f1_processed.constructer")

# COMMAND ----------

display(spark.read.parquet(f"{mnt_processed}/constructer").head(2))

# COMMAND ----------

dbutils.notebook.exit("Success")
