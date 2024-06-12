# Databricks notebook source
from sklearn import datasets
import pandas as pd

# COMMAND ----------

# MAGIC %sql 
# MAGIC use catalog hive_metastore;
# MAGIC create schema sample_data

# COMMAND ----------

iris = datasets.load_iris()
df = pd.DataFrame(data = iris.data, columns=[i.strip('(cm)').strip().replace(" ", "_") for i in iris.feature_names])
df['target'] = iris.target

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode('overwrite').saveAsTable("hive_metastore.sample_data.iris_data")
