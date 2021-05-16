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


us_trend = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\media_trend_us_new.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)
uk_trend = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\media_trend_uk.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)

#define geo IO code
uk_code = ['UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK']
us_code = ['US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US', 'US']


#clean dataframes
try:
    us_trend.drop(columns={'Total Internet : Total Audience', 'quarterly report final(Custom)'}, inplace=True)
    uk_trend.drop(columns={'Total Internet : Total Audience  ', 'quarterly report final(Custom)'}, inplace=True)
    print(bcolors.WARNING +'\nUndesired columns found and removed from UK and US trend dataframes' + bcolors.ENDC)
except: 
    print(bcolors.OKGREEN + '\nUK and US trend data is clean' + bcolors.ENDC)

print(us_trend, uk_trend)

#Apply naming convention
us_trend.rename(columns={
    'Media' : 'date',
    'FANDOM Games percent reach' : 'fandom_games',
    'Future Games percent reach' : 'future_games', 
    'Enthusiast Gaming percent reach' : 'enthusiast_gaming',
    'IGN Entertainment (w/ history) percent reach' : 'ign_entertainment', 
    'Gamer Network percent reach' : 'gamer_network',
    'Network N percent reach' : 'network_n',
    'GAMESPOT.COM percent reach' : 'gamespot',
    'Playwire Media - Gamezone Group percent reach' : 'playwire_media'}, inplace=True)
uk_trend.rename(columns = {
    'Media' : 'date',
    'FANDOM Games' : 'fandom_games',
    'Future Games' : 'future_games', 
    'Enthusiast Gaming' : 'enthusiast_gaming',
    'IGN Entertainment (w/ history)' : 'ign_entertainment', 
    'Gamer Network' : 'gamer_network',
    'Network N' : 'network_n',
    'GAMESPOT.COM' : 'gamespot',
    'Playwire Media - Gamezone Group' : 'playwire_media'}, inplace=True)

#assign country code
uk = uk_trend.assign(geo=uk_code)
us = us_trend.assign(geo=us_code)
#reset index
uk.reset_index(inplace=True, drop=True)
us.reset_index(inplace=True, drop=True)

#initate file creation and concatination

try:
    print("\nInitating file creation...")
    us.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\media_trend\us_media_trend.csv", index=False)
    uk.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\media_trend\uk_media_trend.csv", index=False)
    path = r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\media_trend" # use your path
    all_files = glob.glob(path + "/*.csv")
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
except: 
    print(bcolors.WARNING + "\nUpload error found" + bcolors.ENDC)


#clean concat dataset
combined_frame = pd.concat(li, axis=0, ignore_index=True)
combined_frame.drop(axis=0, index=[0,1,10,11], inplace=True)

print("\nConcat dataframe created: \n" , combined_frame.head(10))

#------------------------------------------------------------------------------------DATABASE CONNECTION------------------------------------------------------------------------------------#

engine = sqla.create_engine('postgresql://dmp_dashboard79oygu43bw:ss244isuiqbmk9os@db-data-analysis-do-user-3211830-0.b.db.ondigitalocean.com:25060/dmp_dashboard', pool_pre_ping=True)

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
        df.to_sql('combined_media_trend',conn, if_exists='replace')
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
    query = pd.read_sql('select * from combined_media_trend', connection)
    results = query
    return results
print(get_records('combined_media_trend'))
