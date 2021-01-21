"""
This module executes the following steps in the ETL pipeline
1. Create Staging tables from raw input
2. Extract fact and dimension tables from staging tables
3. Transform the raw data
4. Load the facts and dimesion tables
"""

import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from transformation.staging import *
from transformation.fact import *
from transformation.dim import *
from data_quality import *

def create_spark():
    """Creates and Returns a SparkSession object"""
    spark = SparkSession.builder.\
    config("spark.jars.packages",\
           "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0")\
    .enableHiveSupport().getOrCreate()
    return spark

if __name__ == "__main__":
    
    # set AWS CREDENTIALS env
    config = configparser.ConfigParser()
    config.read('config.cfg')
    os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    
    # create spark session
    spark = create_spark()
    
    # register all udfs
    # convert SAS date to datetime
    sas_to_dt = udf(lambda x: (datetime.datetime(1960, 1, 1).date() + \
                               datetime.timedelta(x)).isoformat() if x else None)
    spark.udf.register("SAS_TO_DT", sas_to_dt)
    
    # latitude with sign
    lat_as_double = udf(lambda x: np.float(x[:-1]) if 'N' in x else -1*np.float(x[:-1]))
    # longitude with sign
    long_as_double = udf(lambda x: np.float(x[:-1]) if 'E' in x else -1*np.float(x[:-1]))
    # register to use with spark sql
    spark.udf.register("LAT_AS_DOUBLE", lat_as_double)
    spark.udf.register("LONG_AS_DOUBLE", long_as_double)
    
    # call the staging script
    
    # create country_code, state_code, and city_code dfs
    create_staging_tables_from_labels()
    
    # visa category 
    create_and_save_visa_cat()
    
    # travel mode
    create_and_save_mode_table()
    
    # airports
    create_and_save_airports_table()
    
    # us states
    create_and_save_us_states_table()
    
    # us cities
    create_and_save_us_cities_table()
    
    # us temperature
    create_and_save_temperature_table()
    
    # immigrations
    create_and_save_immigrations_stg_table(spark)
    
    # call the fact table scripts
    create_and_save_immigrations_table(spark)
    
    # call the dim table scripts
    create_and_save_immigrants_table(spark)
    create_and_save_states_dim_table(spark)
    create_and_save_cities_dim_table(spark)
    create_and_save_temp_dim_table(spark)
    create_and_save_airports_dim_table(spark)
    create_and_save_time_dim_table(spark)
    
    # call the data quality check scripts
    check_time_table(spark)
    check_immigrations_table(spark)
    check_temp_table(spark)
    check_states_table(spark)
