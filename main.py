import itertools
import threading
import time
import sys

done = False
#here is the animation
def animate():
    for c in itertools.cycle(['|', '/', '-', '\\']):
        if done:
            break
        sys.stdout.write('\r' + c)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rDone!     ')

t = threading.Thread(target=animate)
t.start()



# Runing Databases Scripts 
print("-----Script Start-----")

from utils import append_in_file
print('--Clear File')
append_in_file(['ServerName','Database','Table','Column'],isAppend=False)

print("--Build Ms SQL Catlog")
from ms_sql_script import run_fetch_sql_server
run_fetch_sql_server()

print("--Build Oracle Catlog")
from oracle_script import run_oracle_script
run_oracle_script()

print("--Build Oracle Catlog")
from snowflake_script import run_snowflake_script
run_snowflake_script()

done = True