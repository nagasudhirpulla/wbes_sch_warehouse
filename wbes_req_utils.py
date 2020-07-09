# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 11:50:57 2019

@author: Nagasudhir
"""
import datetime as dt
import requests
import re
import json
from wbes_utils import getMaxRevForDate, getDefaultReqHeaders

# convert the response dataArray to the schedule entry rows
# dataArray headers are in first 3 rows.
# first 2 columns of first header are 'Time Block' and 'Time Desc', next columns are all generator columns
# last column is grand total and need not be considered
# second header columns contain the beneficiary names in format <ben_name>(<rev_num>)
# third header columns contain the on bar off bar total differentiators
# data starts from index 3 to index 98
# the dimension of the data array is to be 103x1005
def convertDataArrayToSchRows(dataArray, targetDt):
    dataRows = []
    # check for the dimension of dataArray
    if len(dataArray) != 103:
        return []
    if len(dataArray[0]) < 3:
        return []
    colIter = 2
    while colIter < len(dataArray[0])-2:
        genName = dataArray[0][colIter+1]
        buyerName = dataArray[1][colIter+1]
        buyerName = buyerName[0:buyerName.rfind('(')]
        for blk in range(1, 97):
            rowNum = blk + 2
            onBarVal = dataArray[rowNum][colIter]
            offBarVal = dataArray[rowNum][colIter+1]
            totalReqVal = dataArray[rowNum][colIter+2]
            dataRows.append({'seller_name': genName,'buyer_name': buyerName, 'block': blk, 'sch_type':'onBarReq', 'val':float(onBarVal), 'sch_date': dt.datetime.strftime(targetDt, '%Y-%m-%d')})
            dataRows.append({'seller_name': genName,'buyer_name': buyerName, 'block': blk, 'sch_type':'offBarReq', 'val':float(offBarVal), 'sch_date': dt.datetime.strftime(targetDt, '%Y-%m-%d')})
            dataRows.append({'seller_name': genName,'buyer_name': buyerName, 'block': blk, 'sch_type':'totalReq', 'val':float(totalReqVal), 'sch_date': dt.datetime.strftime(targetDt, '%Y-%m-%d')})
        colIter = colIter + 3
    return dataRows

# download all isgs sch compoenets for a date from wbes reports
# https://wbes.wrldc.in/ReportFullSchedule/GetFullInjSummary?scheduleDate=16-10-2019&sellerId=ALL&revisionNumber=197&regionId=2&byDetails=1&isDrawer=0&isBuyer=0
def getAllIsgsReqRowsForDate(targetDt, revNum=None):
    rev = revNum
    # get max rev of day if rev number not specified
    if revNum == None:
        rev = getMaxRevForDate(targetDt)
    headers = getDefaultReqHeaders()
    schUrl = "https://wbes.wrldc.in/Report/GetRldcData?isBuyer=false&utilId=ALL&regionId=2&scheduleDate={0}&revisionNumber={1}&byOnBar=1".format(
        dt.datetime.strftime(targetDt, '%d-%m-%Y'), rev)
    r = requests.get(schUrl, headers=headers)
    resJson = r.json()
    dataRows = convertDataArrayToSchRows(resJson['jaggedarray'], targetDt)
    return dataRows

