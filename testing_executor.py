# Databricks notebook source
# MAGIC %md
# MAGIC ### Importing Libraries

# COMMAND ----------

# MAGIC %pip install PyYAML

# COMMAND ----------

# get all imports 
from utils.utils import *
import concurrent.futures
import multiprocessing
import ast
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StringType, TimestampType, StructField

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Define Widgets

# COMMAND ----------

# define default widget values
def_config_filepath = "./test_config_files/"
def_result_table = "default.default.test_results"

# COMMAND ----------

# config filepath should be the filepath/directory containing all the yml files defining the tests you want to run
dbutils.widgets.text("config_filepath", def_config_filepath)

# result_table should be the name of the table where all the results will be pushed to, format catalog.schema.table
dbutils.widgets.text("result_table", def_result_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Values From Widgets

# COMMAND ----------

config_filepath = dbutils.widgets.get("config_filepath")
result_table = dbutils.widgets.get("result_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Metadata

# COMMAND ----------

# import all files in provided directory 
tests_metadata = import_yaml(config_filepath)

# map function name given as string in metadata to a function object in python
for test in tests_metadata:
  test['function_obj'] = locals()[test['function']]

# COMMAND ----------

print(f"Ingested tests: {len(tests_metadata)}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests

# COMMAND ----------

test_results = []

# run the tests in parallel to accelerate testing
with concurrent.futures.ThreadPoolExecutor() as executor:

  # depending on testing function specified in metadata file, run the specfied function for the correspondning test 
  future_to_result = {executor.submit(metadata['function_obj'], metadata, spark): metadata for metadata in tests_metadata}

  for future in concurrent.futures.as_completed(future_to_result):
    result = future_to_result[future]
    try:
      # get the data back from the test
      data = future.result()
      test_results.append(data)
    except Exception as e:
      print('%r generated an exception: %s' % (result, e))
      result['error'] = e
      test_results.append(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Result Dataframe 

# COMMAND ----------

# define schema for the result table
result_table_schema = StructType([
    StructField('name', StringType(), True),
    StructField('dataset', StringType(), True),
    StructField('table', StringType(), True),
    StructField('function', StringType(), True),
    StructField('select', StringType(), True),
    StructField('assert', StringType(), True),
    StructField('test_result', StringType(), True),
    StructField('error', StringType(), True),
    StructField('finish_time', TimestampType(), True),
]) 

# COMMAND ----------

# for the results that have just been returned, create a dataframe
test_result_df = spark.createDataFrame(test_results, schema=result_table_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert into Results table

# COMMAND ----------

# append the results to a managed table
test_result_df.write.mode("append").saveAsTable(result_table)

# COMMAND ----------

# show results
display(spark.sql(f"select * from {result_table}"))

# COMMAND ----------

# drop table if necessary
#spark.sql(f"drop table {result_table}")

# COMMAND ----------


