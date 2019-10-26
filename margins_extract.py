import psycopg2
from app_conf import getConf
import pandas.io.sql as sqlio
import numpy as np
from functools import reduce

def subs(series):
    return reduce(lambda x, y: x + y, series)

dbConfig = getConf("dbConfig")
conn = psycopg2.connect(host=dbConfig['db_host'], dbname=dbConfig['db_name'],
                                     user=dbConfig['db_username'], password=dbConfig['db_password'])
sqlTxt = '''select sch_utility, sch_date, sch_block, sch_type, sch_val from public.schedules 
        	where sch_date between '2018-12-21' and '2018-12-31' and sch_type in 
            ('onBarForSch', 'Total')'''
dat = sqlio.read_sql_query(sqlTxt, conn)
dat.to_excel('data.xlsx')            
df2 = dat.pivot_table(
        values='sch_val', 
        index=['sch_date', 'sch_block'], 
        columns=['sch_utility', 'sch_type'], 
        aggfunc=np.sum)
df2.to_excel('data1.xlsx')