"""
This file reads the configurations from config files, sets
them as python constants, and sets the environment variables
with AWS credentials
"""

import os
import configparser

config = configparser.ConfigParser()
config.read('config.cfg')

#input_dir
input_dir = config.get('INPUT', 'INPUT_DIR')

# staging_prefix
staging_dir = config.get('OUTPUT', 'STAGING_PREFIX')
analytics_dir = config.get('OUTPUT', 'ANALYTICS_PREFIX')

# AWS Creds
KEY = config.get('AWS', 'AWS_ACCESS_KEY_ID')
SECRET = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

# raw data path
raw_data_path = config.get('INPUT', 'RAW_DATA_PATH')

# get raw file paths
i94_label_path = config.get('DATA', 'LABEL_DESCRIPTION')
us_cities_path = config.get('DATA', 'US_CITIES')
temp_path = config.get('DATA', 'TEMPERATURE')
airports_path = config.get('DATA', 'AIPORTS')

# output_dir
output_dir = config.get('OUTPUT', 'OUTPUT_DIR')

# should save on s3
save_on_s3 = bool(config.get('OUTPUT', 'SAVE_ON_S3'))
if save_on_s3:
    s3_bucket = config.get('AWS', 'S3_BUCKET')
