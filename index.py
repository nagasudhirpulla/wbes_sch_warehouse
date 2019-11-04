# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 10:30:27 2019

@author: Nagasudhir
"""

import datetime as dt
from data_update_script import updateSchedules

nowDay = dt.datetime.now()
startDay = dt.datetime(2019, 11, 4)
targetDates = []
for dayIter in range((nowDay-startDay).days+1):
    targetDt = startDay + dt.timedelta(days=dayIter)
    targetDates.append(targetDt)
updateSchedules(targetDates)