# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 10:30:27 2019

@author: Nagasudhir
"""

import datetime as dt
from wbes_full_sch_utils import getAllIsgsSchRowsForDate
from sch_db_adapter import SchDbAdapter

targetDt = dt.datetime.now()
schRows = getAllIsgsSchRowsForDate(targetDt, revNum=None)
schAdapter = SchDbAdapter()
schAdapter.connectToDb()
schAdapter.pushSchRows(schRows)
schAdapter.disconnectDb()
# print(getMaxRevForDate(targetDt))
