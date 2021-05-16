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



uk_non_endemic = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\uk_non_endemic_indexing.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)
us_non_endemic = pd.read_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\source_data\adv_audiences_competitor_key_measures_us_new.csv", index_col=None, warn_bad_lines=True, error_bad_lines=False)

#define geo IO code
uk_code = ['UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK', 'UK']
us_code = ['US', 'US', 'US', 'US', 'US', 'US', 'US', 'US']

#------------------------------------------------------------------------------------UK------------------------------------------------------------------------------------#
uk_non_endemic.drop(axis=0, index=[0,1], inplace=True)
uk_non_endemic.drop(axis=1, columns={'Hobbies: comp index', 'Hobbies: percent reach', 'Movies: comp index', 'Movies: percent reach'}, inplace=True)
uk = uk_non_endemic
uk = uk.assign(Geo=uk_code)
uk.rename(columns={
    'Media ID' : 'media_id',
    'Media' : 'media_property',
    'Social grade A: comp index' : 'social_grade_a_comp_index_uv',
    'Social grade A: percent reach' : 'social_grade_a_percent_reach',
    'Social grade B: comp index' : 'social_grade_b_comp_index_uv',
    'Social grade B: percent reach' : 'social_grade_b_percent_reach',
    'Social grade C: comp index' : 'social_grade_c_comp_index_uv',
    'Social grade C: percent reach' : 'social_grade_c_percent_reach',
    'Adults with kids: comp index' : 'adults_with_kids_comp_index_uv',
    'Adults with kids: percent reach' : 'adults_with_kids_percent_reach',
    'Food and drink : Non alcoholic: comp index' : 'non_alcoholic_drinks_comp_index_uv',
    'Food and drink : Non alcoholic: percent reach' : 'non_alcoholic_drinks_percent_reach',
    'Tech: Wearable tech: comp index' : 'wearable_tech_comp_index_uv',
    'Tech: Wearable tech: percent reach' : 'wearable_tech_percent_reach',
    'Tech: Smartphones: comp index' : 'smartphones_comp_index_uv',
    'Tech: Smartphones: percent reach' : 'smartphones_percent_reach',
    'Shopping: holiday shopping: comp index' : 'holiday_shopping_comp_index_uv',
    'Shopping: holiday shopping: percent reach' : 'holiday_shopping_percent_reach',
    'Shopping: sales and promos: comp index' : 'sales_and_promos_shopping_comp_index_uv',
    'Shopping: sales and promos: percent reach' : 'sales_and_promos_shopping_percent_reach',
    'Mens fashion: casual: comp index' : 'mens_casual_fashion_comp_index_uv',
    'Mens fashion: casual: percent reach' : 'mens_casual_percent_reach',
    'Womens fashion: casual: comp index' : 'womens_casual_fashion_comp_index_uv',
    'Womens fashion: casual: percent reach' : 'womens_casual_fashion_percent_reach',
    'automotive: car culture: comp index' : 'car_culture_comp_index_uv',
    'automotive: car culture: percent reach' : 'car_culture_percent_reach',
    'personal finance: Credit cards: comp index' : 'credit_cards_comp_index_uv',
    'personal finance: Credit cards: percent reach' : 'credit_cards_percent_reach', 
    'Geo' : 'geo'
}, inplace=True)

#verify temporary columns
try:
    uk.drop(columns={'media_id'}, inplace=True)
    print(bcolors.WARNING +'\nUndesired columns found and removed from UK non endemic indexing' + bcolors.ENDC)
except: 
    print(bcolors.OKGREEN + '\nUK non endemic indexing data is clean' + bcolors.ENDC)
    
#------------------------------------------------------------------------------------US------------------------------------------------------------------------------------#
#assign geo column and apply naming convention
us_non_endemic.drop(axis=0, index=[0,1], inplace=True)
us = us_non_endemic
us = us.assign(Geo=us_code)
us.rename(columns={
    'Media ID' : 'media_id',
    'Media' : 'media_property',
    'HH income < 25k: Composition Index UV' : 'hh_income_<25k_comp_index_uv',
    'HH income < 25k: percent Reach' : 'hh_income_<25k_percent_reach',
    'HH income 25 - 39k: Composition Index UV' : 'hh_income_25-39k_comp_index_uv',
    'HH income 25 - 39k: percent Reach' : 'hh_income_25-39k_percent_reach',
    'HH income 40 - 59k: Composition Index UV' : 'hh_income_30-59k_comp_index_uv',
    'HH income 40 - 59k: percent Reach' : 'hh_income_40-59k_percent_reach',
    'HH income > 60k: Composition Index UV' : 'hh_income_>60k_comp_index_uv',
    'HH income > 60k: percent Reach' : 'hh_income_>60k_percent_reach',
    'Adults with kids: Composition Index UV' : 'adults_with_kids_comp',
    'Adults with kids: percent Reach' : 'adults_with_kids_percent_reach',
    'Food and drink: Non-alcoholic beverages: Composition Index UV' : 'non_alcoholic_drinks_comp_index_uv',
    'Food and drink: Non-alcoholic beverages: percent Reach' : 'non_alcoholic_drinks_percent_reach',
    'Tech: wearable tech: Composition Index UV' : 'wearable_tech_comp_index_uv',
    'Tech: wearable tech: percent Reach' : 'wearable_tech_percent_reach',
    'Tech: smartphones: Composition Index UV' : 'smartphones_comp_index_uv',
    'Tech: smartphones: percent Reach' : 'smartphones_percent_reach',
    'Shopping: holiday shopping: Composition Index UV' : 'holiday_shopping_comp_index_uv',
    'Shopping: holiday shopping: percent Reach' : 'holiday_shopping_percent_reach',
    'Shopping: sales and promos: Composition Index UV' : 'sales_and_promos_shopping_comp_index_uv',
    'Shopping: sales and promos: percent Reach' : 'sales_and_promos_shopping_percent_reach',
    'Mens fashion: casual: Composition Index UV' : 'mens_casual_fashion_comp_index_uv',
    'Mens fashion: casual: percent Reach' : 'mens_casual_percent_reach',
    'Womens fashion: casual: Composition Index UV' : 'womens_casual_fashion_comp_index_uv',
    'Womens fashion: casual: percent Reach' : 'womens_casual_fashion_percent_reach',
    'Automotive: car culture: Composition Index UV' : 'car_culture_comp_index_uv',
    'Automotive: car culture:percent Reach' : 'car_culture_percent_reach',
    'Personal finance: credit cards:Composition Index UV' : 'credit_cards_comp_index_uv',
    'Personal finance: credit cards:percent Reach' : 'credit_cards_percent_reach', 
    'Personal finance: mortgages: Composition Index UV' : 'mortgages_comp_index_uv',
    'Personal finance: mortgages: percent Reach' : 'mortgages_percent_reach',
    'Geo' : 'geo'
}, inplace=True)

#verify temporary columns
try:
    us.drop(columns={'media_id'}, inplace=True)
    print(bcolors.WARNING +'\nUndesired columns found and removed from US non endemic indexing' + bcolors.ENDC)
except: 
    print(bcolors.OKGREEN + '\nUS non endemic indexing data is clean' + bcolors.ENDC)

#verify dataset integrity
try: 
    print(bcolors.OKBLUE + "\nData cleaned successfully and ready for upload" + bcolors.ENDC)
    print("\nDataset examples: \n " ,us.head(4) , "\n" , uk.head(4) , "\n")
except ValueError as e: 
    print(bcolors.FAIL + "\nValueError detected " , e + bcolors.ENDC)

#initate file creation and concatination

try:
    print("\nInitating file creation...")
    us.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\gaming_index\us_site_key_measures_feb.csv", index=False)
    uk.to_csv(r"C:\Users\chris\Documents\Work\Comscore\raw_data\output_data\gaming_index\uk_site_key_measures_feb.csv", index=False)
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
        df.to_sql('combined_non_endemic_index',conn, if_exists='replace')
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
    query = pd.read_sql('select * from combined_non_endemic_index', connection)
    results = query
    return results
print(get_records('combined_non_endemic_index'))
