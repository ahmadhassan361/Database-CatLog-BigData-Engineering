import cx_Oracle
import pandas as pd
from utils import append_in_file
def connection(host,port,service,username,password):
    conn = cx_Oracle.connect(r'{}/{}@{}:{}/{}'.format(username,password,host,port,service)) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    return conn


def run_query(query,conn):
    c = conn.cursor()
    try:
        c.execute(query) # use triple quotes if you want to spread your query across multiple lines
        temp_list = []
        for row in c:
            temp_list.append(row[0])
    
        return temp_list
    except:
        pass

    



def run_oracle_script():

    df = pd.read_csv('./servers/oracle_db.csv')
    df.columns = ['host','port','service','username','password']
    main_catlog_list = []
    
    for index in df.index:
        conn = connection(host=df['host'][index],port=int(df['port'][index]),service=df['service'][index],username=df['username'][index],password=df['password'][index])

        database_name = run_query(query='select name  from v$database',conn=conn)
        tables_list = run_query(query='SELECT table_name FROM dba_tables',conn=conn)
        tables_temp_list = []
        for table in tables_list:
            if "$" in table:
                continue
            columns = run_query(query="SELECT COLUMN_NAME FROM DBA_TAB_COLS WHERE table_name='{}'".format(table),conn=conn)
            data = {
                'table':table,
                'columns':columns
            }
            tables_temp_list.append(data)
        main_catlog_list.append(
            {
                'server':df['host'][index],
                'database':database_name[0],
                'tables':tables_temp_list
            }
        )
        conn.close()

        
    for i in main_catlog_list:
        for j in i['tables']:
            if j['columns'] is not None:
                for k in j['columns']:
                    append_in_file(['Oracle',i['server'],i['database'],j['table'],k])
            else:
                append_in_file(['Oracle',i['server'],i['database'],j['table'],'No Column'])



run_oracle_script()