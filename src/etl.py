"""
This module executes the following steps in the ETL pipeline
1. Create Staging tables from raw input
2. Extract fact and dimension tables from staging tables
3. Transform the raw data
4. Load the facts and dimesion tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf


if __name__ == "__main__":
    
    # create spark session
    
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
    
    
    # call the fact table scripts
    
    # call the dim table scripts
    
    # call the data quality check scripts
    
