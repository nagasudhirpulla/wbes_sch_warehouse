# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 10:30:27 2019

@author: Nagasudhir
"""

import datetime as dt
from wbes_full_sch_utils import getAllBuyerSchRowsForDate
from sch_db_adapter import SchDbAdapter

from_date = dt.datetime(2018, 6, 1)
to_date = dt.datetime(2019, 10, 31)

schAdapter = SchDbAdapter()
schAdapter.connectToDb()

for dayIter in range((to_date-from_date).days+1):
    targetDt = from_date + dt.timedelta(days=dayIter)
    schRows = getAllBuyerSchRowsForDate(targetDt, revNum=None)
    schAdapter.pushSchRows(schRows)
    print('{0} buyer sch push done at {1}'.format(
        dt.datetime.strftime(targetDt, '%d-%m-%Y'), dt.datetime.strftime(dt.datetime.now(), '%d-%m-%Y %H:%M')))
schAdapter.disconnectDb()
