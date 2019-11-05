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
# dataArray headers are in first 2 rows.
# first 2 columns of first header are 'Time Block' and 'Time Desc', next columns are all generator columns
# last column is grand total and need not be considered
# second header columns contain the schedule component description
# data starts from index 2 to index 97
# the dimension of the data array is to be 102x343
def convertDataArrayToSchRows(dataArray, targetDt):
    dataRows = []
    # check for the dimension of dataArray
    if len(dataArray) != 102:
        return []
    if len(dataArray[0]) < 3:
        return []
    for blk in range(1, 97):
        rowNum = blk + 1
        for colIter in range(2, len(dataArray[0])-1):
            genName = dataArray[0][colIter]
            schType = dataArray[1][colIter]
            schVal = dataArray[rowNum][colIter]
            dataRows.append({'util_name': genName, 'block': blk, 'sch_type':schType, 'val':float(schVal), 'sch_date': dt.datetime.strftime(targetDt, '%Y-%m-%d')})
    return dataRows

# download all isgs sch compoenets for a date from wbes reports
# http://scheduling.wrldc.in/wbes/ReportFullSchedule/GetFullInjSummary?scheduleDate=16-10-2019&sellerId=ALL&revisionNumber=197&regionId=2&byDetails=1&isDrawer=0&isBuyer=0
def getAllIsgsSchRowsForDate(targetDt, revNum=None):
    rev = revNum
    # get max rev of day if rev number not specified
    if revNum == None:
        rev = getMaxRevForDate(targetDt)
    headers = getDefaultReqHeaders()
    schUrl = "http://scheduling.wrldc.in/wbes/ReportFullSchedule/GetFullInjSummary?scheduleDate={0}&sellerId=ALL&revisionNumber={1}&regionId=2&byDetails=1&isDrawer=0&isBuyer=0".format(
        dt.datetime.strftime(targetDt, '%d-%m-%Y'), rev)
    r = requests.get(schUrl, headers=headers)
    resText = r.text
    # extract data array from the response
    jsonText = re.search('var data = JSON\.parse\((.*)\);', resText).group(1)
    # jsonText = jsonText.replace('\\', '')
    dataArray = json.loads(eval(jsonText))
    dataRows = convertDataArrayToSchRows(dataArray, targetDt)
    return dataRows

# http://scheduling.wrldc.in/wbes/ReportNetSchedule/GetNetScheduleSummary?regionId=2&scheduleDate=01-11-2019&sellerId=ALL&revisionNumber=129&byDetails=1&isBuyer=1
def getAllBuyerSchRowsForDate(targetDt, revNum=None):
    rev = revNum
    # get max rev of day if rev number not specified
    if revNum == None:
        rev = getMaxRevForDate(targetDt)
    headers = getDefaultReqHeaders()
    schUrl = "http://scheduling.wrldc.in/wbes/ReportNetSchedule/GetNetScheduleSummary?regionId=2&scheduleDate={0}&sellerId=ALL&revisionNumber={1}&byDetails=1&isBuyer=1".format(
        dt.datetime.strftime(targetDt, '%d-%m-%Y'), rev)
    r = requests.get(schUrl, headers=headers)
    resText = r.text
    # extract data array from the response
    jsonText = re.search('var data = JSON.parse\((.*)\);', resText).group(1)
    jsonText = jsonText.replace("\\", "")
    dataArray = json.loads(jsonText[1:-1])
    dataRows = convertDataArrayToSchRows(dataArray, targetDt)
    return dataRows