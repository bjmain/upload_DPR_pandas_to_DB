#!/usr/bin/python3
# import the module
import os
import glob
import pandas as pd
import csv
from sqlalchemy import create_engine
import psycopg2
import config   #you need to create this config.py file and update the variables with your database, username and password
import subprocess
import sys

#Note: you need to indicate which directory (e.g. path/to/pur1997) in argv[1]

# Get a database connection
conn_string = "host="+config.HOST+" dbname="+config.DB+" user="+config.username+" password="+config.password

# Get a database connection
conn = psycopg2.connect(conn_string)

# Create a cursor object. Allows us to execute the SQL query
cursor = conn.cursor()

def load_data(schema, table):
    sql_command = "SELECT * FROM {}.{};".format(str(schema), str(table))
    # Load the data
    data = pd.read_sql(sql_command, conn)
    return (data)

# Download data that is already uploaded (find where you left off)
chem_df = load_data(config.dpr_schema, config.use_data)

# make lists for filtering the data
already_in = list(chem_df['use_no'])
chem_list = list(pd.read_csv("/home/bmain/pesticide/chem_com.csv")["chem_code"])
prod = list(pd.read_csv("/home/bmain/pesticide/product_prodno.csv")["prodno"])


# set up new DB connection for alchemy
engine = create_engine('postgresql://{user}:{pw}@{site}:5432/{db}'.format(user=config.username, pw=config.password, site=config.HOST, db=config.DB))

# make error file
error = open("error_rows_prodo.csv", 'w')  # these typically occur when there is misformated data
weird_comtrs = open("comtrs_error_rows.csv", "w")

def upload_by_row(pur_directory):
    
    files = glob.glob("%s/udc*" % (pur_directory))
    for udc in files:
        df = pd.read_csv(udc,low_memory=False, dtype={'range': str, 'section':str, 'township':str, 'comtrs':str})
        
        print(udc, len(df))
        df = df[~df['use_no'].isin(already_in)] # remove rows that are already in
        print(udc, len(df))
        
        # collapse redundant rows and keep one with prod_no info
        good_columns = list(df.columns)
        for col in [ 'chem_code', 'prodchem_pct', 'lbs_chm_used', 'lbs_prd_used']:
            good_columns.remove(col)
        df = df.drop_duplicates(good_columns,keep= 'first')
        print("dup_drop:",len(df))
        
        #df = df[df['chem_code'].isin(chem_list)] # keep rows that have valid chem code
        df = df[df['prodno'].isin(prod)] # keep rows that have valid prodno
        print("good_prodno:",len(df))
        
        #df['year'] = pd.DatetimeIndex(df['applic_dt']).year  #we are using the file name over the time because sometimes the time data is wrong.
        df['year'] = pur_directory.split("/")[-1].strip("pur")
        if 'comtrs' not in df.columns:
            df['comtrs']= df['county_cd'].astype(str)+df['base_ln_mer'].astype(str)+df['township'].astype(str)+df['tship_dir'].astype(str)+df['range'].astype(str)+df['range_dir'].astype(str)+df['section'].astype(str)

        df['comtrs_check']= df['county_cd'].astype(str)+df['base_ln_mer'].astype(str)+df['township'].astype(str)+df['tship_dir'].astype(str)+df['range'].astype(str)+df['range_dir'].astype(str)+df['section'].astype(str)
        
        for IND, row in df.iterrows():
            use_event = pd.DataFrame(df, index = [IND])
            # Check to make sure the comtrs string matches the comtrs columns
            comtrs_str = use_event['comtrs'].values.tolist()[0]
            comtrs_str_check = use_event['comtrs_check'].values.tolist()[0]
            if comtrs_str != comtrs_str_check:
                use_event.to_csv('comtrs_error_rows.csv', mode='a', header=False)    
            #remove tmp column for checking comtrs
            use_event = use_event.drop(columns=['comtrs_check'])

            #use_event.to_sql('use_data_chemical', con=engine, schema="dpr_pur", if_exists='append',index = False)

            try:
                use_event.to_sql('use_data_chemical', con=engine, schema="dpr_pur", if_exists='append',index = False)
                print("good",IND) 
           # save rows with errors to follow up with later
            except:
                print("error",IND) 
                use_event.to_csv('error_rows_prodno.csv', mode='a', header=False)    
        
        print(udc)

# You need to point to the unzipped pur files with udc files for each county inside.
upload_by_row(sys.argv[1])
