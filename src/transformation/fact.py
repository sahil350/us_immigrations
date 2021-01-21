"""
This module creates the facts tables from staging files
and saves the tables in analytics directory
"""

import os
import configparser
import datetime
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import udf
from transformation.constants import staging_dir, analytics_dir, input_dir

def create_and_save_immigrations_table(spark):
    """
    This method extracts relevant fields from staging tables, creates 
    fact table and finally saves it in the path as per config file
    """
    # set paths
    staging_1_path = staging_dir + 'staging_1/'
    airports_path = os.path.join(input_dir, 'us_interantional_airport_codes.csv')
    
    # read dfs
    staging_1 = spark.read.parquet(staging_1_path)
    airports = spark.read.csv(airports_path, header=True)
    
    # register temp view
    staging_1.createOrReplaceTempView('staging_1')
    airports.createOrReplaceTempView('staging_5')

    # extract relevant fields for fact table
    immigrations = spark.sql("""
            SELECT DISTINCT CAST(staging_1.cicid AS INTEGER) AS id,
                   CAST(staging_1.i94yr AS INTEGER) AS year,
                   CAST(staging_1.i94mon AS INTEGER) AS month,
                   CAST(staging_1.i94cit AS INTEGER) AS country_code,
                   staging_5.airport_id AS airport_id,
                   staging_1.i94addr AS state_code,
                   staging_1.count AS count,
                   staging_1.i94port AS city_code,
                   SAS_TO_DT(staging_1.arrdate) AS arrival_date,
                   CAST(staging_1.cicid AS INTEGER) AS immigrant_id
            FROM staging_1 JOIN
            staging_5 ON (staging_1.i94port == staging_5.city_code 
                          AND
                          staging_1.i94addr == staging_5.state_code)
        """)

    # fact table save path
    immigrations_path = analytics_dir + 'immigrations/'
    
    # partition by year, month, and state and save
    immigrations.write.partitionBy('year', 'month', 'state_code')\
                .parquet(immigrations_path)
    
