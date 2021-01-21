"""
This module creates the dimensions tables from staging files
and saves the tables in analytics directory
"""

import os
import configparser
import datetime
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import udf
from constants import staging_dir, analytics_dir, input_dir

def create_and_save_immigrants_table(spark):
    """
    This method extracts relevant fields from staging tables, creates 
    immigrants dim table and finally saves it in the path as per config file
    """
    
    # get paths
    visa_path = os.path.join(input_dir, 'visa_category.csv')
    mode_path = os.path.join(input_dir, 'travel_mode.csv')
    country_path = os.path.join(input_dir, 'country_code.csv')
    staging_1_path = staging_path + 'staging_1/'
    
    # read dfs
    mode = spark.read.csv(mode_path, header=True)
    visa = spark.read.csv(visa_path, header=True)
    country = spark.read.csv(country_path, header=True)
    staging_1 = spark.read.parquet(staging_1_path)
    
    # register temporary view
    mode.createOrReplaceTempView('staging_7')
    visa.createOrReplaceTempView('staging_6')
    country.createOrReplaceTempView('staging_8')
    staging_1.createOrReplaceTempView('staging_1')
    
    # read the relevant columns for immigrant table
    immigrants = spark.sql("""
                SELECT DISTINCT CAST(staging_1.cicid AS INTEGER) AS immigrant_id,
                       staging_1.gender AS gender,
                       staging_1.visatype AS visa_type,
                       staging_1.occup AS occupation,
                       staging_7.mode As mode,
                       staging_6.category AS visa_category,
                       staging_8.country AS country
                FROM staging_1 JOIN
                     staging_7 ON (staging_1.i94mode == staging_7.id) JOIN
                     staging_6 ON (staging_1.i94visa == staging_6.id) JOIN
                     staging_8 ON (staging_1.i94cit == staging_8.id)
               """)
    
    # set the path to save immigrant dataframe
    immigrants_path = analytics_dir + 'immigrants/'
    
    # save the dataframe
    immigrants.write.parquet(immigrants_path)
    

def create_and_save_states_dim_table(spark):
    """
    """
    
    # set the states path
    states_path = os.path.join(input_dir, 'us_states.csv')
    
    # read the states df
    states = spark.read.csv(states_path, header=True)
    
    # register temp view
    states.createOrReplaceTempView('staging_2')
    
    # set the relevant columns
    states_dim = spark.sql("""
        SELECT `State Code` AS state_code,
               State AS state,
               `Total Population` AS population,
               `Female Population` AS female_population,
               `Number of Veterans` AS num_veterans,
               `Foreign-born` AS foreign_born,
               avg_households,
               `American Indian and Alaska Native` AS native,
               Asian AS asian,
               `Black or African-American` AS black,
               `Hispanic or Latino` AS hispanic,
               `White` AS white
        FROM staging_2
        """)
    
    # set the path to save dim states
    dim_states_path = analytics_dir + 'states/'
    
    # save dim states
    states_dim.write.parquet(dim_states_path)
    
def create_and_save_cities_dim_table(spark):
    """
    """
    
    # set the path
    cities_path = os.path.join(input_dir, 'us_cities.csv')
    
    # read the cities df from staging directory
    cities = spark.read.csv(cities_path, header=True)
    
    # register temporary view
    cities.createOrReplaceTempView('staging_3')
    
    # set the columns for dim table
    cities_dim = spark.sql("""
            SELECT code AS city_code,
                   city,
                   state_code,
                   State AS state,
                   `Total Population` AS total_population,
                   `Female Population` AS female_population,
                   `Number of Veterans` AS num_veterans,
                   `Foreign-born` AS  foreign_born,
                   `Average Household Size` AS avg_households,
                   `American Indian and Alaska Native` AS native,
                   Asian AS asian,
                   `Black or African-American` AS black,
                   `Hispanic or Latino` AS hispanic,
                   `White` AS white
            FROM staging_3
        """)
    
    # save the dim table
    dim_cities_path = analytics_dir + 'cities/'
    cities_dim.write.partitionBy('state_code').parquet(dim_cities_path)
    
def create_and_save_temp_dim_table(spark):
    """
    """
    
    # temperature staging path
    temperature_path = os.path.join(input_dir, 'us_temperature.csv')
    
    # read the df
    us_temp = spark.read.csv(temperature_path, header=True)
    
    # register temp table
    us_temp.createOrReplaceTempView('staging_4')
    
    # set the columns for temperature df
    temp_dim = spark.sql("""
        SELECT city_code,
               dt AS date,
               YEAR(dt) AS year,
               MONTH(dt) AS month,
               AverageTemperature AS avg_temp,
               AverageTemperatureUncertainty AS avg_temp_uncertainty,
               LAT_AS_DOUBLE(Latitude) AS latitude,
               LONG_AS_DOUBLE(Longitude) AS longitude
        FROM staging_4
    """)
    
    # save temperature df partitioned by year and month
    dim_temp_path = analytics_dir + 'temperature/'
    temp_dim.write.partitionBy('year', 'month').parquet(dim_temp_path)
    
def create_and_save_airports_dim_table(spark):
    """
    """
    
    # get the path
    airports_path = os.path.join(input_dir, 'us_interantional_airport_codes.csv')
    
    # read df
    airports = spark.read.csv(airports_path, header=True)
    
    # set the columns for dim table
    dim_airports = spark.sql("""
        SELECT airport_id,
               city_code,
               city,
               state_code,
               type,
               name,
               elevation_ft,
               gps_code,
               iata_code,
               local_code,
               ROUND(latitude, 2) AS latitude,
               ROUND(longitude, 2) AS longitude
        FROM staging_5
    """)
    
    # save airports dim table
    dim_airports_path = analytics_dir + 'airports/'
    airports.write.parquet(dim_airports_path)
    
def create_and_save_time_dim_table(spark):
    """
    """
    
    # get path
    staging_1_path = staging_path + 'staging_1/'
    
    # read df
    staging_1 = spark.read.parquet(staging_1_path)
    
    # register temp view
    staging_1.createOrReplaceTempView('staging_1')
    
    # extract arrival date
    ts = spark.sql("SELECT DISTINCT SAS_TO_DT(arrdate) AS date FROM staging_1")
    
    # register arrival date temp view
    ts.createOrReplaceTempView('TS')
    
    # set the relevant columns for dim table
    dim_time = spark.sql("""
        SELECT date AS arrival_date,
               YEAR(date) AS year,
               MONTH(date) AS month,
               DAY(date) AS day,
               WEEKOFYEAR(date) AS week,
               DAYOFWEEK(date) AS weekday
        FROM TS
    """)
    
    # save dim table partitioned by year and month
    dim_time_path = analytics_dir + 'time/'
    dim_time.write.partitionBy('year', 'month').parquet(dim_time_path)
    