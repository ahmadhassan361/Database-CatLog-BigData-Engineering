import pyodbc
import pandas as pd
from utils import append_in_file
#global variables

def run_query(query,server):
    main_database = 'master'
    cnxn = pyodbc.connect(r'Driver=SQL Server;Server={};Database={};Trusted_Connection=yes;'.format(server,main_database))
    cursor = cnxn.cursor()
    cursor.execute(query)
    return_list = []
    while 1:
        row = cursor.fetchone()
        if not row:
            break
        return_list.append(row)
    cnxn.close()
    return return_list

def fetch_databases_and_details(server):
    #Temp Lists
    databases_list = []
    catlog_list = []
    exclude_database = ['master','tempdb','model','msdb']


    # Fetch Databases
    databases = run_query("select name FROM sys.databases;",server)
    for i in databases:
        databases_list.append(i.name)

    for i in exclude_database:
        databases_list.remove(i)

    # Fetch Tables of Each Database
    for i in databases_list:
        tables_list = []
        tables = run_query("SELECT TABLE_NAME FROM {}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'".format(i),server)
        tables = [x.TABLE_NAME for x in tables ] # Clean Table Name OBJ to String

        for j in tables:
            columns = run_query("select COLUMN_NAME from {}.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{}'".format(i,j),server)
            columns = [x.COLUMN_NAME for x in columns]

            tables_list.append(
                {
                    'table_name':j,
                    'columns':columns
                }
            )


        export = {
            'database_name':i,
            'tables':tables_list,
        }
        catlog_list.append(export)
    return catlog_list

def store_sql_data_to_file(server):
    cat_log = fetch_databases_and_details(server)
    


    for i in range(0,len(cat_log)):
        
        cat_obj = cat_log[i]
        for j in range(0,len(cat_obj['tables'])):
            for k in cat_obj['tables'][j]['columns']:
                append_in_file(['MS SQL',server,cat_obj['database_name'],cat_obj['tables'][j]['table_name'],k])
                

def run_fetch_sql_server():
    servers = []
    with open ("./servers/sql.txt", "r") as myfile:
        data = myfile.read().splitlines()
        servers = data
    for serve in servers:
        store_sql_data_to_file(serve)



