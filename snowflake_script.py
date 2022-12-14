import snowflake.connector
from utils import append_in_file

def run_snowflake_script():
    ctx = snowflake.connector.connect(
        user='ahmadhassan361',
        password='Jarvis_22',
        account='ynpuanh-te32705'
        )
    cs = ctx.cursor()
    try:
        
        cs.execute("show warehouses;")
        warehouses = cs.fetchall()
        for war in warehouses:
            cs.execute("USE WAREHOUSE {};".format(war[0]))
            cs.execute("show databases;")
            databases_list = cs.fetchall()
            
            for i in databases_list:
                
                cs.execute("select table_schema,table_name from {}.information_schema.tables".format(i[1]))
                tables_row = cs.fetchall()
                for j in tables_row:
                    
                    cs.execute("show columns in table {}.{}.{};".format(i[1],j[0],j[1]))
                    columns_list = cs.fetchall()
                    for k in columns_list:
                        append_in_file(['Snowflake ({})'.format(war[0]),'{}.{}'.format(i[1],j[0]),j[1],k[2]])
                    
        
    finally:
        cs.close()
    ctx.close()