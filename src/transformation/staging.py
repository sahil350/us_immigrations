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
from io import StringIO
from constants import s3_bucket, i94_label_path, us_cities_path
from constants import temp_path, airports_path, output_dir, save_on_s3


if save_on_s3:
    s3_resource = boto3.resource('s3')
    
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
    temp = np.where(port_df.state_code.isin(values), port_df.state_code.str[:2],\
                    np.where(port_df.state_code.str.len()==2, port_df.state_code, np.nan))

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
    



