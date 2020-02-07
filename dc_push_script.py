# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 10:30:27 2019

@author: Nagasudhir
"""

import datetime as dt
from dc_from_oracle_to_pg import getDcDbRowsForDates
from sch_db_adapter import SchDbAdapter

from_date = dt.datetime(2020, 1, 1)
to_date = dt.datetime(2020, 1, 31)

schAdapter = SchDbAdapter()
schAdapter.connectToDb()

for dayIter in range((to_date-from_date).days+1):
    targetDt = from_date + dt.timedelta(days=dayIter)
    schRows = getDcDbRowsForDates(targetDt, targetDt)
    schAdapter.pushSchRows(schRows)
    print('{0} dc push done'.format(
        dt.datetime.strftime(targetDt, '%d-%m-%Y')))
schAdapter.disconnectDb()
# print(getMaxRevForDate(targetDt))
