import pandas as pd
import warnings
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


raw_us_core = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\us_core_key_measures_feb.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)
raw_uk_core = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\uk_core_key_measures_feb.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)
raw_us_key_measures = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\us_comp_key_measures_feb.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)
raw_uk_key_measures = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\uk_comp_key_measures_feb.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)
us_trend = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\media_trend_us_new.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)

#define geo IO code
uk_code = ['UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK']
us_code = ['US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US']

#unique id assigning functions(use when appropriate)
'''
x = 1
i = 1

while i < len(uk): 
    if (uk.media_property[i] == uk.media_property[i-i]):
        uk.loc[i,'id'] = x
        i = i+1
    else:
        x = x+1
        uk.loc[i, 'id'] = x
        i = i+1
uk.fillna(0, inplace=True)
uk['id'] = uk['id'].astype(int)
'''
#------------------------------------------------------------------------------------UK------------------------------------------------------------------------------------#
#assign geo column and apply naming convention
uk_core_demo = raw_uk_core.assign(Geo=uk_code)
uk_core_demo.rename(columns={
    'Tagging Status' : 'tagging_status',
    'Media' : 'media_property',
    'Total Unique Visitors/Viewers (000)' : 'total_uv_in_thousands',
    '% Reach' : 'percent_reach',
    'Composition Index UV' : 'composition_index_uv',
    'Geo' : 'geo'}, inplace=True
)

#verify temporary columns
uk_core_demo.drop(axis=0, index=[0,1], inplace=True)
try:
    uk_core_demo.drop(columns={'Media ID'}, inplace=True)
    print(bcolors.WARNING +'\nUndesired columns found and removed from UK core demographic key measures' + bcolors.ENDC)
except: 
    print(bcolors.OKGREEN + '\nUK core demographic key measure data is clean' + bcolors.ENDC)

#reset index
uk_core_demo.reset_index(inplace=True, drop=True)

#assign geo column and apply naming convention
uk_key_measures = raw_uk_key_measures.assign(Geo=uk_code)
uk_key_measures.rename(columns={
    'Tagging Status' : 'tagging_status',
    'Media' : 'media_property',
    'Total Unique Visitors/Viewers (000)' : 'total_uv_in_thousands',
    '% Reach' : 'percent_reach',
    'Composition Index UV' : 'composition_index_uv',
    'Geo' : 'geo'}, inplace=True
)
#reformat data fields
uk_key_measures['total_uv_in_thousands'] = uk_key_measures['total_uv_in_thousands'].str.replace(',','')
uk_key_measures['total_uv_in_thousands'] = uk_key_measures['total_uv_in_thousands'].str.replace(',','')

#verify temporary columns
uk_key_measures.drop(axis=0, index=[0,1], inplace=True)

try:
    uk_key_measures.drop(columns={'media_id'}, inplace=True)
    print(bcolors.WARNING +'\nUndesired columns found and removed from UK key measures'+ bcolors.ENDC)
except: 
    print(bcolors.OKGREEN + '\nUK key measure data is clean'+ bcolors.ENDC)

#reset index
uk_key_measures.reset_index(inplace=True, drop=True)

#------------------------------------------------------------------------------------US------------------------------------------------------------------------------------#
#assign geo column and apply naming convention
us_core_demo = raw_us_core.assign(Geo=us_code)
us_key_measures = raw_us_key_measures.assign(Geo=us_code)

us_core_demo.rename(columns={
    'Tagging Status' : 'tagging_status',
    'Media' : 'media_property',
    'Total Unique Visitors/Viewers (000)' : 'total_uv_in_thousands',
    '% Reach' : 'percent_reach',
    'Composition Index UV' : 'composition_index_uv',
    'Geo' : 'geo'}, inplace=True
)
#verify temporary columns
us_core_demo.drop(axis=0, index=[0,1], inplace=True)
try:
    us_core_demo.drop(columns={'Media ID'}, inplace=True)
    print(bcolors.WARNING +'\nUndesired columns found and removed from US core demographic key measures'+ bcolors.ENDC)
except: 
    print(bcolors.OKGREEN +'\nUS core demographic key measure data is clean'+ bcolors.ENDC)

#reset index
us_core_demo.reset_index(inplace=True, drop=True)

#assign geo column and apply naming convention
us_key_measures.rename(columns={
    'Tagging Status' : 'tagging_status',
    'Media' : 'media_property',
    'Total Unique Visitors/Viewers (000)' : 'total_uv_in_thousands',
    '% Reach' : 'percent_reach',
    'Composition Index UV' : 'composition_index_uv',
    'Geo' : 'geo'}, inplace=True
)
#reformat data fields
us_core_demo['total_uv_in_thousands'] = us_core_demo['total_uv_in_thousands'].str.replace(',','')
us_key_measures['total_uv_in_thousands'] = us_key_measures['total_uv_in_thousands'].str.replace(',','')

#verify temporary columns
us_key_measures.drop(axis=0, index=[0,1], inplace=True)
try:
    us_key_measures.drop(columns={'media_id'}, inplace=True)
    print(bcolors.WARNING +'\nUndesired columns found and removed from US key measures'+ bcolors.ENDC)
except: 
    print(bcolors.OKGREEN +'\nUS key measure data is clean' + bcolors.ENDC)

#reset index
us_key_measures.reset_index(inplace=True, drop=True)

try: 
    print(bcolors.OKBLUE + "\nData cleaned successfully and ready for upload" + bcolors.ENDC)
    print("\nDataset examples: \n " ,uk_core_demo.head(4) , "\n" , us_core_demo.head(4) , "\n", uk_key_measures.head(4) , "\n", us_key_measures.head(4) , "\n")
except ValueError as e: 
    print(bcolors.FAIL + "\nValueError detected " , e + bcolors.ENDC)

#initate file creation and concatination

try:
    print("\nInitating file creation...")
    us_core_demo.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\us_key_measures_feb.csv", index=False)
    uk_core_demo.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\uk_key_measures_feb.csv", index=False)
    us_key_measures.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\us_comp_key_measures_feb.csv", index=False)
    uk_key_measures.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\uk_comp_key_measures_feb.csv", index=False)
    path = r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data" # use your path
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
combined_frame.drop(axis=0, index=[0,1,10,11], inplace=True)

print("\nConcat dataframe created: \n" , combined_frame.head(10))

#------------------------------------------------------------------------------------DATABASE CONNECTION------------------------------------------------------------------------------------#

engine = sqla.create_engine('postgresql://dmp_dashboard79oygu43bw:ss244isuiqbmk9os@db-data-analysis-do-user-3211830-0.b.db.ondigitalocean.com:25060/dmp_dashboard', pool_pre_ping=True)

#Psycopg error logging
def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()    
    # get the line number when exception occured
    line_n = traceback.tb_lineno    
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("\npsycopg2 traceback:", traceback, "-- type:", err_type/) 
    # psycopg2 extensions.Diagnostics object attribute

#Initate database connection
def connect(engine):
    conn = None
    try:
        print(bcolors.OKBLUE + 'Connecting to the PostgreSQL...........' + bcolors.ENDC)
        conn = engine
        print(bcolors.OKGREEN + "Connection successful.................." + bcolors.ENDC)
        
    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception("\nOperationalError found: ", err)        
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
    query = pd.read_sql('select * from combined_key_measures limit 10', connection)
    results = query
    return results
print(get_records('combined_key_measures'))




