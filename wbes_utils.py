import datetime as dt
import requests
import re

# get default headers for requests
def getDefaultReqHeaders():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3887.7 Safari/537.36',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }

# get max rev for selected date
# https://wbes.wrldc.in/Report/GetCurrentDayFullScheduleMaxRev?regionid=2&ScheduleDate=17-10-2019
def getMaxRevForDate(revDt):
    headers = getDefaultReqHeaders()
    revUrl = "https://wbes.wrldc.in/Report/GetCurrentDayFullScheduleMaxRev?regionid=2&ScheduleDate={0}".format(
        dt.datetime.strftime(revDt, "%d-%m-%Y"))
    r = requests.get(revUrl, headers=headers)
    maxRevObj = r.json()
    return maxRevObj['MaxRevision']