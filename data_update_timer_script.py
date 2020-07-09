# -*- coding: utf-8 -*-
"""
Created on Mon Nov 4 11:12:20 2019

@author: Nagasudhir

Generally data can be updated 
1st time - weekly once for previous week
2nd time - Monthly once at M+2nd day 
3rd time - Monthly once at M+21st day 
4th time - Monthly once at M+40th day 

For a day d, this script will update data for d, d-1, d-2, d-1Month, d-2Months
"""

import datetime as dt
from data_update_script import updateSchedules

nowDay = dt.datetime.now()
dMinus1Day = nowDay - dt.timedelta(days=1)
dMinus2Day = nowDay - dt.timedelta(days=2)
dMinus1Month = nowDay - dt.timedelta(days=30)
# dMinus2Month = nowDay - dt.timedelta(days=60)
targetDates = [nowDay, dMinus1Day, dMinus2Day, dMinus1Month]

updateSchedules(targetDates)
