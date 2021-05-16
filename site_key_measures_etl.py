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

us_sites = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\site_key_measures_us_feb.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)
uk_sites = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\site_key_measures_uk_feb.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)
uk_sites.drop(axis=0, index=range(9,50), inplace=True)

#define geo IO code
uk_code = ['UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK']
us_code = ['US', 'US', 'US', 'US', 'US', 'US', 'US', 'US']

#clean dataframes
uk_sites.drop(axis=0, index=0, inplace=True)
us_sites.drop(axis=0, index=0, inplace=True)
us_sites.drop(axis=1, columns={'Mean Adult Age'}, inplace=True)


#------------------------------------------------------------------------------------UK------------------------------------------------------------------------------------#
#assign geo column and apply naming convention
uk = uk_sites.assign(Geo=uk_code)
uk.rename(columns={
    'Entity Type' : 'media_id', 
    'Tagging Status' : 'tagging_status',
    'Media' : 'media_property',
    'Total Unique Visitors/Viewers (000)' : 'total_uv_in_thousands',
    '% Reach' : 'percent_reach',
    'Total Views (MM)' : 'total_views_mm',
    'Total Visits (000)' : 'total_visits_in_thousands',
    'Average Views per Visit' : 'avg_views_per_visit',
    'Average Minutes per View' : 'avg_mins_per_view',
    'Average Minutes per Visit' : 'avg_mins_per_visit',
    'Average Minutes per Visitor' : 'avg_mins_per_visitor',
    'Geo' : 'geo'}, inplace=True
)


#verify temporary columns
try:
    uk.drop(columns={'media_id'}, inplace=True)
    print(bcolors.WARNING +'\nUndesired columns found and removed from UK sites key measures' + bcolors.ENDC)
except: 
    print(bcolors.OKGREEN + '\nUK sites key measure data is clean' + bcolors.ENDC)

#reformat data fields
uk['total_uv_in_thousands'] = uk['total_uv_in_thousands'].str.replace(',','')
uk['total_visits_in_thousands'] = uk['total_visits_in_thousands'].str.replace(',','')

#------------------------------------------------------------------------------------US------------------------------------------------------------------------------------#
#assign geo column and apply naming convention
us = us_sites.assign(Geo=us_code)
us.rename(columns={
    'Tagging Status' : 'tagging_status',
    'Media' : 'media_property',
    'Total Unique Visitors/Viewers (000)' : 'total_uv_in_thousands',
    '% Reach' : 'percent_reach',
    'Total Views (MM)' : 'total_views_mm',
    'Total Visits (000)' : 'total_visits_in_thousands',
    'Average Views per Visit' : 'avg_views_per_visit',
    'Average Minutes per View' : 'avg_mins_per_view',
    'Average Minutes per Visit' : 'avg_mins_per_visit',
    'Average Minutes per Visitor' : 'avg_mins_per_visitor',
    'Geo' : 'geo'}, inplace=True
)
try:
    us.drop(columns={'Media ID'}, inplace=True)
    print(bcolors.WARNING +'\nUndesired columns found and removed from US sites'+ bcolors.ENDC)
except: 
    print(bcolors.OKGREEN + '\nUS sites data is clean'+ bcolors.ENDC)

#reformat data fields
us['total_uv_in_thousands'] = us['total_uv_in_thousands'].str.replace(',','')
us['total_visits_in_thousands'] = us['total_visits_in_thousands'].str.replace(',','')

#reset index
uk.reset_index(inplace=True, drop=True)
us.reset_index(inplace=True, drop=True)

try: 
    print(bcolors.OKBLUE + "\nData cleaned successfully and ready for upload" + bcolors.ENDC)
    print("\nDataset examples:  \n", uk.head(4) , "\n" , us.head(4))
except ValueError as e: 
    print(bcolors.FAIL + "\nValueError detected " , e + bcolors.ENDC)

#initate file creation and concatination

try:
    print("\nInitating file creation...")
    us.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\site_rankings\us_site_key_measures_feb.csv", index=False)
    uk.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\site_rankings\uk_site_key_measures_feb.csv", index=False)
    path = r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\site_rankings" # use your path
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
combined_frame.drop(axis=1, columns={'Video Views (000)'}, inplace=True)

print(combined_frame)

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
        df.to_sql('combined_key_measures',conn, if_exists='replace')
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
    query = pd.read_sql('select * from combined_site_rankings_feb limit 10', connection)
    results = query
    return results
print(get_records('combined_site_rankings_feb'))
