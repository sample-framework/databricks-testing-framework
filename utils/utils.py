import os
import yaml
import datetime
import pyspark.sql.functions as f

def import_yaml(filepath:str) -> list:  
  """
  This function iterates through all the yml files in specified filepath 
  and retrieves the test definitions from the files. 

  Inputs:
    - filepath (str): filepath to the location where all test metadata is stored 
  Outputs:
    - tests (list): list of dictionaries that contains parsed metadata from yml files. 
  """

  files = os.listdir(filepath)
  tests = []
  for file in files:
    with open(filepath + file, "r") as f:
        contents = yaml.safe_load(f)
        tests += contents
  return tests



def test_custom(metadata, spark) -> dict:
  """ 
  This function is a custom test function that allows user to specify their own SQL code
  to define how the subset the data and specify their own assert statement to define how they want to test the data. 

  Inputs:
    - metadata (dict): dictionary contaning metadata about the test
      - metadata.select (str): SQL string that will be executed to subset the data for the test
      - metadata.assert (str): Python code string that contains a condition, which will be executed 
         to determine whether the test was successfull or not 
  
  Outputs:
    - metadata (dict): returns a dictionary with information about the test and its results
  """

  sql_string = metadata["select"]
  py_string = metadata["assert"]

  # try select
  try:
    df = spark.sql(sql_string)
  except Exception as E:
      metadata["test_result"] = "failed"
      metadata["error"] = f"select statement failed with exeption {E}"
      metadata["finish_time"] = datetime.datetime.now()
      return  metadata
    
  # try assert 
  if df:
    try:
      exec(f"global condition; condition = {py_string}")
      if condition:
        metadata["test_result"] = "passed"
        metadata["error"] = None
      else:
        metadata["test_result"] = "failed"
        metadata["error"] = f"Failed {py_string}"
    except Exception as E:
        metadata["test_result"] = "failed"
        metadata["error"] = f"select statement failed with exeption {E}"
      
  metadata["finish_time"] = datetime.datetime.now()
  return metadata

def test_completeness(metadata, spark):
  """
  This function is a standardized test that will check for completeness of 
  the columns in a given table. For specified columns it will check if there are any null values

  Inputs:
    - metadata (dict): dictionary containing metadata about the test
      - metadata.table (str): explicit name of the table that is being tested, format catalog.schema.table
      - metadata.columns_to_test (list):  list of column names that will be tested for completeness

  Outputs:
    - metadata (dict): returns a dictionary with information about the test and its results
  """
  table = metadata['table']
  column_list = metadata['columns_to_test']

  df = spark.table(table)
  null_count_cols = []
  for column in column_list:
    null_count =  df.filter(f.col(column).isNull()).count()
    if null_count > 0:
      null_count_cols.append(f"{column} : {null_count}")
    
  if len(null_count_cols) == 0:
    metadata['test_result'] = "passed"
  else:
    metadata['test_result'] = "failed"
    metadata['error'] = "Column : Null count, " + ", ".join(null_count_cols)

  metadata["finish_time"] = datetime.datetime.now()
  return metadata


def test_uniqueness(metadata, spark):
  """
  This function is a standardized test that will check for uniqueness of 
  the columns in a given table. For specified columns it will check if there are any null values

  Inputs:
    - metadata (dict): dictionary containing metadata about the test
      - metadata.table (str): explicit name of the table that is being tested, format catalog.schema.table
      - metadata.columns_to_test (str): list of column names that will be tested for uniqueness
  
  Outputs 
    - metadata (dict): returns a dictionary with information about the test and its results
  """
  table = metadata['table']
  column_list = metadata['columns_to_test']

  df = spark.table(table)
  distinct_count_cols = []
  for column in column_list:
    distinct_count =  df.select(column).distinct().count()
    normal_count = df.select(column).count()
    if distinct_count < normal_count:
      distinct_count_cols.append(f"{column} : {normal_count - distinct_count}")
    
  if len(distinct_count_cols) == 0:
    metadata['test_result'] = "passed"
  else:
    metadata['test_result'] = "failed"
    metadata['error'] = "Column : Duplicate count, " + ", ".join(distinct_count_cols)

  metadata["finish_time"] = datetime.datetime.now()
  return metadata






