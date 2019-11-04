# -*- coding: utf-8 -*-
"""
Created on Mon Nov 4 11:12:20 2019

@author: Nagasudhir
"""

import datetime as dt
from wbes_full_sch_utils import getAllIsgsSchRowsForDate, getAllBuyerSchRowsForDate
from dc_from_oracle_to_pg import getDcDbRowsForDates
from sch_db_adapter import SchDbAdapter


def updateSchedules(targetDates):
    # push full schedules
    schAdapter = SchDbAdapter()
    schAdapter.connectToDb()
    for targetDt in targetDates:
        # push seller full schedules
        schRows = getAllIsgsSchRowsForDate(targetDt, revNum=None)
        schAdapter.pushSchRows(schRows)
        print('{0} seller sch push done at {1}'.format(
            dt.datetime.strftime(targetDt, '%d-%m-%Y'), dt.datetime.strftime(dt.datetime.now(), '%d-%m-%Y %H:%M')))
        # push buyer full schedules
        schRows = getAllBuyerSchRowsForDate(targetDt, revNum=None)
        schAdapter.pushSchRows(schRows)
        print('{0} buyer sch push done at {1}'.format(
            dt.datetime.strftime(targetDt, '%d-%m-%Y'), dt.datetime.strftime(dt.datetime.now(), '%d-%m-%Y %H:%M')))
        # push dc schedules
        schRows = getDcDbRowsForDates(targetDt, targetDt)
        schAdapter.pushSchRows(schRows)
        print('{0} dc push done at {1}'.format(dt.datetime.strftime(
            targetDt, '%d-%m-%Y'), dt.datetime.strftime(dt.datetime.now(), '%d-%m-%Y %H:%M')))
    schAdapter.disconnectDb()
