"""
This module creates the staging tables from input files
and saves the tables in staging directory
"""

import re
import os
import pandas as pd
import numpy as np
import configparser
import boto3
from io import StringIO, BytesIO
from transformation.constants import s3_bucket, i94_label_path, us_cities_path
from transformation.constants import temp_path, airports_path, output_dir, save_on_s3
from transformation.constants import staging_dir, raw_data_path, KEY, SECRET


if save_on_s3:
    session = boto3.Session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
        )
    s3_resource = session.resource('s3')
    
def save_df_on_s3(df, path, index=True):
    """
    This method saves pandas dataframe at given s3 key
    INPUTS
    * df dataframe
    * path s3_key
    
    RETURNS
    True if successful
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=index)
    s3_resource.Object(s3_bucket, path).put(Body=csv_buffer.getvalue())
    return True

def regex_patterns():
    """
    This method return regex patterns for extracting country, state, and
    port tables from labels file
    
    RETURNS
    * pattern 
    * port_pattern 
    * state_pattern
    """
    
    # citi_code pattern string
    pattern_str = """
                    \s*         # Match any spaces
                    ([\d]+)     # create a group and match 1 or more numerics
                    \s+         # match one or more spaces
                    =           # match '='
                    \s+         
                    '(.*)'      # match everything within ''
                  """

    # citi code regex pattern
    pattern = re.compile(pattern_str, re.VERBOSE)

    # port regex pattern
    # extracts port code, and name
    port_pattern = re.compile("\s*'(\S+)'\s+=\s+'([\S\s#]+),\s*([\S\s#]+)'")

    # state regex pattern
    # extracts code, and name
    state_pattern = re.compile("\s*'([a-zA-Z0-9]+)'='([a-zA-Z\.\s]+)'")
    
    return pattern, port_pattern, state_pattern

def read_dicts_from_labels(citi_code, port, state):
    """
    This method extracts country, port, state data from labels file and sets
    the dictionaries in-place
    
    INPUTS
    * citi_code dictionary with keys `id` and `country`
    * port dictionary with keys `code`, `city`, `state_code`
    * state dictionary with keys `code` and `name`
    """
    # get all regex patterns
    pattern, port_pattern, state_pattern = regex_patterns()
    
    with open(i94_label_path, 'r') as fp:
        for i, line in enumerate(fp):
            if i > 8 and i < 245:
                match = re.search(pattern, line)
                citi_code['id'].append(match.group(1))
                citi_code['country'].append(match.group(2))
            if i > 301 and i < 893:
                match = re.search(port_pattern, line)
                try:
                    port['code'].append(match.group(1))
                    port['city'].append(match.group(2))
                    port['state_code'].append(match.group(3))
                except:
                    port['code'].append(None)
                    port['city'].append(None)
                    port['state_code'].append(None)
            if i > 980 and i < 1036:
                match = re.search(state_pattern, line)
                state['code'].append(match.group(1))
                state['name'].append(match.group(2))
    

def create_staging_tables_from_labels():
    """
    This method extracts country, ports, and state table from
    labels files and saves them as staging table on the path
    as provided in the config file
    """
    citi_code = {'id':[],
            'country':[]}

    port = {
        'code': [],
        'city': [],
        'state_code': []
    }

    state = {
        'code': [],
        'name': []
    }
    
    # fill dicts
    read_dicts_from_labels(citi_code, port, state)
    
    # citi code dataframe
    citi_code_df = pd.DataFrame(citi_code)
    citi_code_df = citi_code_df.set_index('id')
    citi_code_df.country = citi_code_df.country.str.capitalize()

    # airport dataframe
    port_df = pd.DataFrame(port)
    port_df = port_df.set_index('code')
    port_df['state_code'] = port_df.state_code.str.strip()
    port_df = port_df.dropna(how='all')
    
    values = ['AR (BPS)', 'CA (BPS)', 'CO #ARPT', 'FL #ARPT', 'LA (BPS)',
       'ME (BPS)', 'MT (BPS)', 'NM (BPS)', 'SC #ARPT', 'TX (BPS)',
       'VA #ARPT', 'VT (I-91)', 'VT (RT. 5)', 'VT (BP - SECTOR HQ)',
       'WASHINGTON #INTL', 'WA (BPS)']
    
    # clean state_code
    temp = np.where(port_df.state_code.isin(values), \
                    port_df.state_code.str[:2],\
                    np.where(port_df.state_code.str.len()==2, \
                             port_df.state_code, np.nan))

    us_state_codes = np.where(temp=='MX', np.nan, temp)
    port_df['state_code'] = us_state_codes
    port_df = port_df.dropna(how='any')
    
    # states dataframe
    states = pd.DataFrame(state)
    states = states.set_index('code')
    
    # output paths
    citi_code_path = os.path.join(output_dir,'country_code.csv')
    port_path = os.path.join(output_dir,'port_immigration.csv')
    states_path = os.path.join(output_dir,'state_code.csv')
    
    # save the dataframes
    if save_on_s3:
        save_df_on_s3(citi_code_df, citi_code_path)
        save_df_on_s3(port_df,port_path)
        save_df_on_s3(states,states_path)
    else:
        citi_code_df.to_csv(citi_code_path)
        port_df.to_csv(port_path)
        states.to_csv(states_path)
        
def create_and_save_visa_cat():
    """
    This method creates the visa_category table and saves 
    it at the appropriate path as per the config file
    """
    # visa category dataframe
    visa_category = pd.DataFrame({
        'id': [1,2,3],
        'category': ['Business', 'Pleasure', 'Student']
    })
    visa_category = visa_category.set_index('id')
    
    # set the path according to config file
    visa_category_path = os.path.join(output_dir,'visa_category.csv')
    
    if save_on_s3:
        save_df_on_s3(visa_category,visa_category_path)
    else:
        visa_category.to_csv(visa_category_path)
        
def create_and_save_mode_table():
    """
    This method creates the travel_mode table and saves 
    it at the appropriate path as per the config file
    """
    # travel mode dataframe

    travel_mode = pd.DataFrame({
        'id': [1,2,3,9],
        'mode': ['Air', 'Sea', 'land', 'Not reported']
    })

    travel_mode = travel_mode.set_index('id')
    
    # path to save table
    travel_mode_path = os.path.join(output_dir,'travel_mode.csv')
    
    if save_on_s3:
        save_df_on_s3(travel_mode,travel_mode_path)
    else:
        travel_mode.to_csv(travel_mode_path)
    
def create_and_save_airports_table():
    """
    This method creates the airports table and saves 
    it at the appropriate path as per the config file
    """
    # ports table path
    port_path = os.path.join(output_dir,'port_immigration.csv')
    
    if save_on_s3:
        obj = s3_resource.Object(s3_bucket, port_path).get('Body')
        us_city_code = pd.read_csv(BytesIO(obj['Body'].read()))
    else:
        us_city_code = pd.read_csv(port_path)
    
    # read airports df
    airports = pd.read_csv(airports_path)
    
    # extract us small, medium, and large airports in cities
    us_airports = airports[airports.iso_country.str.lower() == 'us']

    us_airports = us_airports[us_airports.type.isin(\
                            ['small_airport', 'medium_airport', 'large_airport'])]

    us_intl_airports = us_airports[us_airports.name.str.contains('International')]

    us_intl_airports = us_intl_airports[~us_intl_airports.municipality.isnull()]
    
    # split coordinates into latitude and longitude
    long_lat = us_intl_airports['coordinates'].str.split(',', expand=True)
    long_lat.columns = ['longitude', 'latitude']
    us_intl_final = pd.concat([us_intl_airports, long_lat], axis=1).\
                        drop('coordinates', axis=1)
    
    # merge with city_code df to get city codes in
    # airports df and extract columns for dim table
    us_city_code.city = us_city_code.city.str.lower()

    us_intl_final.municipality = us_intl_final.municipality.str.lower() 

    us_intl_final = us_city_code.merge(\
                        us_intl_final, left_on='city', right_on='municipality')\
                            [['ident', 'code', 'city', 'state_code',\
                              'type', 'name','elevation_ft', 'gps_code',\
                              'iata_code', 'local_code', 'latitude',\
                              'longitude']]

    us_intl_final.rename(columns={'code': 'city_code', 'ident': 'airport_id'}, \
                         inplace=True)
    
    # save staging table according to the path provided in config
    us_intl_path = os.path.join(output_dir, 'us_interantional_airport_codes.csv')
    if save_on_s3:
        save_df_on_s3(us_intl_final, us_intl_path, index=False)
    else:
        us_intl_final.to_csv(us_intl_path, index=False)
        
def create_and_save_us_states_table():
    """
    This method creates the us_states table and saves 
    it at the appropriate path as per the config file
    """
    # read us cities table
    us_cities = pd.read_csv(us_cities_path, sep=';')
    
    # relevant columns
    columns = ['City', 'State Code', 'State', 'Total Population',\
               'Female Population', 'Number of Veterans', 'Foreign-born',\
               'Average Household Size', 'Race', 'Count']
    
    us_cities = us_cities[columns]
    
    # extract number of households
    us_cities['num_households'] = np.round(us_cities['Total Population']/\
                                           us_cities['Average Household Size'])
    
    us_states_race = us_cities.groupby(['State Code', 'Race']).\
                    mean()['Count'].unstack()
    
    # percentage of each race
    us_states_race = us_states_race.div(us_states_race.sum(axis=1), axis=0)
    
    us_states = us_cities.drop('Race', axis=1).\
                drop_duplicates(['City', 'State Code']).\
                drop(['City', 'Count'], axis=1)
    
    us_states = us_states.groupby(['State Code', 'State']).agg({
                                'Total Population': 'sum',
                                'Female Population': 'sum',
                                'Number of Veterans': 'sum',
                                'Foreign-born': 'sum',
                                'num_households': 'sum'
                            })
    
    # calculate average households in state
    us_states['avg_households'] = np.round(us_states['Total Population']/\
                                           us_states['num_households'], 2)
    
    # join the race table with us table
    us_states = us_states.join(us_states_race)
    
    # save us states staging table as per the path in config file
    us_states_path = os.path.join(output_dir, 'us_states.csv')
    if save_on_s3:
        save_df_on_s3(us_states, us_states_path)
    else:
        us_states.to_csv(us_states_path)
        

def create_and_save_us_cities_table():
    """
    This method creates the us_cities table and saves 
    it at the appropriate path as per the config file
    """
    # read us_cities table
    us_cities = pd.read_csv(us_cities_path, sep=';')
    
    # read us_city_code
    # ports table path
    port_path = os.path.join(output_dir,'port_immigration.csv')
    
    if save_on_s3:
        obj = s3_resource.Object(s3_bucket, port_path).get('Body')
        us_city_code = pd.read_csv(BytesIO(obj['Body'].read()))
    else:
        us_city_code = pd.read_csv(port_path)
    
    # for calculating % of each race
    us_cities_race = us_cities[['City', 'State Code', 'Race', 'Count']]
    
    # 
    us_cities_race = us_cities_race.set_index(['City', 'State Code', \
                                               'Race']).unstack(-1)
    us_cities_race = us_cities_race['Count'].fillna(0)
    
    # each state and city is repeated five times for five races
    us_cities = us_cities.drop_duplicates(['City', 'State Code'])
    
    # to join with us_cities_race
    us_cities = us_cities.set_index(['City', 'State Code']).\
                          drop(['Race', 'Count'], axis=1)
    
    us_cities = us_cities.join(us_cities_race)
    
    # caluclate percentage
    for col in ['American Indian and Alaska Native', 'Asian',
       'Black or African-American', 'Hispanic or Latino', 'White']:
        us_cities[col] = us_cities[col]/us_cities['Total Population']
    
    us_cities = us_cities.reset_index()
    
    # for merging with us_city_code
    us_cities.City = us_cities.City.str.lower()
    
    us_cities = us_city_code.merge(us_cities, left_on=['city', 'state_code'], \
                               right_on=['City', 'State Code']).\
                         drop(['City', 'State Code'], axis=1)
    
    us_cities.city = us_cities.city.str.capitalize()
    
    # save according to the path in config file
    us_cities_path_save = os.path.join(output_dir, 'us_cities.csv')
    if save_on_s3:
        save_df_on_s3(us_cities, us_cities_path_save, index=False)
    else:
        us_cities.to_csv(us_cities_path_save, index=False)
    

def create_and_save_temperature_table():
    """
    This method creates the us_temperature table and saves 
    it at the appropriate path as per the config file
    """
    # read temperature df
    fname = temp_path
    df = pd.read_csv(fname)
    
    # read us_city_code
    # ports table path
    port_path = os.path.join(output_dir,'port_immigration.csv')
    
    if save_on_s3:
        obj = s3_resource.Object(s3_bucket, port_path).get('Body')
        us_city_code = pd.read_csv(BytesIO(obj['Body'].read()))
    else:
        us_city_code = pd.read_csv(port_path)
    
    # set datetime type for col dt
    df['dt'] = pd.to_datetime(df['dt'])
    
    # extract temperature begining 20th century
    temp_df = df[df.dt.dt.year > 1899]
    
    # extract us temperature
    us_temp_df = temp_df[temp_df.Country == 'United States']
    
    # for joining with us_city_code
    us_temp_df.City = us_temp_df.City.str.lower()
    
    # city code lower city
    us_city_code.city = us_city_code.city.str.lower()
    
    us_temp_df = us_city_code.merge(us_temp_df, left_on='city', right_on='City').\
        drop(['Country', 'City', 'city', 'state_code'], axis=1).\
        rename(columns={'code':'city_code'})

    # save according to path given
    us_temp_path = os.path.join(output_dir, 'us_temperature.csv')
    if save_on_s3:
        save_df_on_s3(us_temp_df, us_temp_path, index=False)
    else:
        us_temp_df.to_csv(us_temp_path, index=False)
        
def create_and_save_immigrations_stg_table(spark):
    """
    This method creates the immigrations table and saves 
    it at the appropriate path as per the config file
    """
    if 's3' in raw_data_path:
        df_spark = spark.read.parquet(raw_data_path)
    else:
        df_spark = spark.read.format('com.github.saurfang.sas.spark').\
                        load(raw_data_path)
    # create a temporary view
    df_spark.createOrReplaceTempView('immigrations_raw')
    
    # extract relevant columns
    staging_1 = spark.sql("""
        SELECT cicid, i94yr, i94mon, i94cit, i94port, arrdate, i94mode, i94addr, i94bir,
               i94visa, count, occup, gender, visatype
        FROM immigrations_raw
        """)
    
    # set the correct path
    staging_1_path = staging_dir + 'staging_1/'
    
    # dataframe as parquet
    staging_1.write.mode('overwrite').parquet(staging_1_path)
    
