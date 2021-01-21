"""
This file checks for data quality of dim and fact tables
after ETL operations
"""

import os
import configparser
import datetime
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import udf, col

config = configparser.ConfigParser()
config.read('config.cfg')

analytics_dir = config.get('OUTPUT', 'ANALYTICS_PREFIX')

def check_time_table(spark):
    """
    This method performs data quality check for the time
    dimension table
    """
    # get the path of time table
    time_path = analytics_dir + 'time/'
    
    # read the df
    time = spark.read.parquet(time_path)
    
    assert time.agg({"year": "max"}).collect()[0]["max(year)"] == 2016

def check_immigrations_table(spark):
    """
    This method performs data quality check for the immigrations
    fact table
    """
    # get the path of immigrations table
    immigration_path = analytics_dir + 'immigrations/'
    
    # read the immigrations table
    immigrations = spark.read.parquet(immigration_path)
    
    assert len(immigrations.where(col('id').isNull()).collect()) == 0

def check_temp_table(spark):
    """
    This method performs data quality check for the temperature
    dimension table
    """
    # get the path of temperature table
    temp_path = analytics_dir + 'temperature/'
    
    # read the temp table
    us_temp = spark.read.parquet(temp_path)
    
    assert us_temp.agg({"year": "max"}).collect()[0]["max(year)"] == 2013

def check_states_table(spark):
    """
    This method performs data quality check for the states
    dimension table
    """
    # get the path of states table
    states_path = analytics_dir + 'states/'
    
    # read the spark df
    states = spark.read.parquet(states_path)
    
    assert states.count() == 49
