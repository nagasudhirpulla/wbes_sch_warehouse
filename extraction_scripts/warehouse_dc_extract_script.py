import datetime as dt
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
import os
import numpy as np

def getConf(confKey):
    db_name = os.getenv('SCHEDULE_WAREHOUSE_DB_NAME', 'db_name')
    db_username = os.getenv('METER_WAREHOUSE_DB_USERNAME', 'username')
    db_password = os.getenv('METER_WAREHOUSE_DB_PASSWORD', 'password')
    db_host = os.getenv('METER_WAREHOUSE_DB_HOST', 'hostip')
    db_port = os.getenv('METER_WAREHOUSE_DB_PORT', 'db_port')
    if confKey == "dbConfig":
        return dict(
            db_name=db_name,
            db_username=db_username,
            db_password=db_password,
            db_host=db_host,
            db_port=db_port
        )

from_date = dt.datetime(2019, 1, 1)
to_date = dt.datetime(2019, 11, 30)

dbConfig = getConf("dbConfig")
conn = psycopg2.connect(host=dbConfig['db_host'], dbname=dbConfig['db_name'],
                                     user=dbConfig['db_username'], password=dbConfig['db_password'])
sql = "SELECT sch_utility, sch_date, sch_block, sch_type, sch_val FROM public.schedules where sch_type in ('onBarForSch', 'offBar') and sch_date BETWEEN '2019-01-01' and '2019-11-30';"
dat = sqlio.read_sql_query(sql, conn)
conn = None

df2 = dat.pivot_table(
        values='sch_val', 
        index=['sch_date', 'sch_block'], 
        columns=['sch_utility', 'sch_type'], 
        aggfunc=np.sum)
df2.to_csv('dc_data_rearranged.csv', index=True)