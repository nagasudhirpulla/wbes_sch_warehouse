# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 10:30:27 2019

@author: Nagasudhir
"""

import datetime as dt
from wbes_full_sch_utils import getAllIsgsSchRowsForDate
from sch_db_adapter import SchDbAdapter

from_date = dt.datetime(2018, 12, 21)
to_date = dt.datetime(2018, 12, 31)

schAdapter = SchDbAdapter()
schAdapter.connectToDb()

for dayIter in range((to_date-from_date).days+1):
    targetDt = from_date + dt.timedelta(days=dayIter)
    schRows = getAllIsgsSchRowsForDate(targetDt, revNum=None)
    schAdapter.pushSchRows(schRows)
    print('{0} sch push done'.format(
        dt.datetime.strftime(targetDt, '%d-%m-%Y')))
schAdapter.disconnectDb()
# print(getMaxRevForDate(targetDt))
