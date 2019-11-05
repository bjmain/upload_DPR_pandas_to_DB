#!/usr/bin/python3
# import the module
import os
import glob
import pandas as pd
import csv
from sqlalchemy import create_engine
import psycopg2

# Get a database connection
conn_string = "host='db.****.org' dbname='****' user='****' password='****'"
conn = psycopg2.connect(conn_string)

# Create a cursor object. Allows us to execute the SQL query
cursor = conn.cursor()

def load_data(schema, table):
    data_year = os.getcwd().split("/")[-1].strip("pur") # get year based on directory
    sql_command = "SELECT * FROM {}.{} WHERE year = {};".format(str(schema), str(table), str(data_year))
    # Load the data
    data = pd.read_sql(sql_command, conn)
    return (data)

# Download data that is already uploaded (find where you left off)
chem_df = load_data("dbname","dir.data")


# make lists for filtering the data
already_in = list(chem_df['use_no'])
chem_list = list(pd.read_csv("chem_com.csv")["chem_code"])
prod = list(pd.read_csv("product_prodno.csv")["prodno"])


# set up new DB connection for alchemy
engine = create_engine('postgresql://USER:PW@db.***.org:5432/dbname')

# make error file
error = open("error_rows_prodo.csv", 'w')

files = glob.glob("udc*")
for udc in files:
    df = pd.read_csv(udc,low_memory=False)
    
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
    
    #df['year'] = pd.DatetimeIndex(df['applic_dt']).year
    df['year'] = os.getcwd().split("/")[-1].strip("pur")
    
    for IND, row in df.iterrows():
            use_event = pd.DataFrame(df, index = [IND])

            try:
                use_event.to_sql('use_data_chemical', con=engine, schema="dpr_pur", if_exists='append',index = False)
                print(row) 
            # save rows with errors to follow up with later
            except:
                use_event.to_csv('error_rows_prodno.csv', mode='a', header=False)    
    
    print(udc)


