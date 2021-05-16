import pandas as pd
import glob
import sqlalchemy as sqla
import psycopg2
import os
import sys
import psycopg2.extras as extras
from psycopg2 import OperationalError, errorcodes, errors
from sqlalchemy import create_engine

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


adv_ad_gaming_uk = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\adv_audiences_competitor_key_measures_uk.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)
adv_ad_gaming_us = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\adv_audiences_competitor_key_measures_us.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)
uk_non_endemic = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\uk_non_endemic_indexing.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)

#define geo IO code
uk_code = ['UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK']
us_code = ['US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US']


#------------------------------------------------------------------------------------UK------------------------------------------------------------------------------------#
#assign geo column and apply naming convention
uk = adv_ad_gaming_uk.assign(Geo=uk_code)
adv_ad_gaming_uk.rename(columns={'Media' : 'media_property', 'PC Composition Index UV' : 'pc_composition_index_uv', 'PC % Reach' : 'pc_percent_reach', 'PC Total Views (MM)' : 'pc_total_views_per_million', 'PC Average Views per Visit' : 'pc_avg_views_per_visit', 'PC Average Minutes per Visit' : "pc_avg_mins_per_visit"}, inplace=True)
adv_ad_gaming_uk.rename(columns={
    'Console Composition Index UV' : 'console_composition_index_uv',
    'Console % Reach' : 'console_percent_reach',
    'Console Total Views (MM)' : 'console_total_views_per_million',
    'Console Average Views per Visit' : 'console_avg_views_per_visit',
    'Console Average Minutes per Visit' : 'console_avg_mins_per_visit',
    'Mobile Composition Index UV' : 'mobile_composition_index_uv',
    'Mobile % Reach' : 'mobile_percent_reach',
    'Mobile Total Views (MM)' : 'mobile_total_views_per_million',
    'Mobile Average Views per Visit' : 'mobile_avg_views_per_visit',
    'Mobile Average Minutes per Visit' : 'mobile_avg_mins_per_visit',
    'eSports Composition Index UV' : 'esports_composition_index_uv',
    'eSports % Reach' : 'esports_percent_reach',
    'eSports Total Views (MM)' : 'esports_total_views_per_million',
    'eSports Average Views per Visit' : 'esports_avg_views_per_visit',
    'eSports Average Minutes per Visit' : 'esports_avg_mins_per_visit'
},inplace=True)
adv_ad_gaming_uk.drop(axis=0, index=0, inplace=True)

#------------------------------------------------------------------------------------US------------------------------------------------------------------------------------#
#assign geo column and apply naming convention
us = adv_ad_gaming_us.assign(Geo=us_code)
adv_ad_gaming_us.rename(columns={'PC Composition Index UV' : 'pc_composition_index_uv', 'Media' : 'media_property', 'PC % Reach' : 'pc_percent_reach', 'PC Total Views (MM)' : 'pc_total_views_per_million', 'PC Average Views per Visit' : 'pc_avg_views_per_visit', 'PC Average Minutes per Visit' : "pc_avg_mins_per_visit"}, inplace=True)
adv_ad_gaming_us.rename(columns={
    'Console Composition Index UV' : 'console_composition_index_uv',
    'Console % Reach' : 'console_percent_reach',
    'Console Total Views (MM)' : 'console_total_views_per_million',
    'Console Average Views per Visit' : 'console_avg_views_per_visit',
    'Console Average Minutes per Visit' : 'console_avg_mins_per_visit',
    'Mobile Composition Index UV' : 'mobile_composition_index_uv',
    'Mobile % Reach' : 'mobile_percent_reach',
    'Mobile Total Views (MM)' : 'mobile_total_views_per_million',
    'Mobile Average Views per Visit' : 'mobile_avg_views_per_visit',
    'Mobile Average Minutes per Visit' : 'mobile_avg_mins_per_visit',
    'eSports Composition Index UV' : 'esports_composition_index_uv',
    'eSports % Reach' : 'esports_percent_reach',
    'eSports Total Views (MM)' : 'esports_total_views_per_million',
    'eSports Average Views per Visit' : 'esports_avg_views_per_visit',
    'eSports Average Minutes per Visit' : 'esports_avg_mins_per_visit'
},inplace=True)
adv_ad_gaming_us.drop(axis=0, index=0, inplace=True)

#verify dataset integrity
try: 
    print(bcolors.OKBLUE + "\nData cleaned successfully and ready for upload" + bcolors.ENDC)
    print("\nDataset examples: \n " ,adv_ad_gaming_uk.head(4) , "\n" , adv_ad_gaming_us.head(4) ,"\n")
except ValueError as e: 
    print(bcolors.FAIL + "\nValueError detected " , e + bcolors.ENDC)


#initate file creation and concatination

try:
    print("\nInitating file creation...")
    us.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\gaming_index\us_site_key_measures_feb.csv", index=False)
    uk.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\gaming_index\uk_site_key_measures_feb.csv", index=False)
    path = r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\gaming_index" # use your path
    all_files = glob.glob(path + "/*.csv")
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    print(bcolors.OKCYAN + "\nFile creation successful!" + bcolors.ENDC)
except ImportError as ie: 
    print(bcolors.WARNING + "\nError found: ", ie, + bcolors.ENDC)
except NameError as ne: 
    print(bcolors.WARNING + "\nError found: ", ne, + bcolors.ENDC)
except ValueError as ve: 
    print(bcolors.WARNING + "\nError found: ", ve, + bcolors.ENDC)
except TypeError as te: 
    print(bcolors.WARNING + "\nError found: ", te, + bcolors.ENDC)

#clean concat dataset
combined_frame = pd.concat(li, axis=0, ignore_index=True)
combined_frame.drop(axis=0, index=[0,9], inplace=True)

print("\nConcat dataframe created: \n" , combined_frame.head(10))

#------------------------------------------------------------------------------------DATABASE CONNECTION------------------------------------------------------------------------------------#

engine = sqla.create_engine('postgresql://doadmin:d86f7w5kk4lei9pd@db-data-analysis-do-user-3211830-0.b.db.ondigitalocean.com:25060/dmp_dashboard', pool_pre_ping=True)

def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()    
    # get the line number when exception occured
    line_n = traceback.tb_lineno    
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type) 
    # psycopg2 extensions.Diagnostics object attribute

def connect(engine):
    conn = None
    try:
        print(bcolors.OKBLUE + 'Connecting to the PostgreSQL...........' + bcolors.ENDC)
        conn = engine
        print(bcolors.OKGREEN + "Connection successful.................." + bcolors.ENDC)
        
    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)        
        # set the connection to 'None' in case of error
        conn = None
    return conn


def copy_from_dataframe(conn, df):
    
    try:
        df.to_sql('combined_gaming_index',conn, if_exists='replace')
        print(bcolors.OKGREEN + "Data inserted using copy_from_dataframe successfully...." + bcolors.ENDC)
    except (Exception, psycopg2.DatabaseError) as error:
        show_psycopg2_exception(error)
        
        
# Connect to the database
conn = connect(engine)
conn.autocommit = True
# Run the mogrify() method
copy_from_dataframe(conn, combined_frame)

# Prepare sql query
def get_records(database_table):
    connection = engine
    query = pd.read_sql('select * from combined_gaming_index', connection)
    results = query
    return results
print(get_records('combined_gaming_index'))
