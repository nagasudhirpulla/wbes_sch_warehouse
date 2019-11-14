import pandas as pd
import datetime as dt
from sch_db_adapter import SchDbAdapter
import glob


def getStationAliasDict():
    stationAliasDict = {'MAUDA2': 'MOUDA_II', 'MAUDA': 'MOUDA_II'}
    return stationAliasDict


def getSchRowsFromAgcDf(agcDf):
    stationAliasDict = getStationAliasDict()
    dataRows = []
    # iterate through each row
    for rowIter in range(agcDf.shape[0]):
        # we have single day data in each row
        # get date
        schDate = dt.datetime.strptime(agcDf.iloc[rowIter, 0], '%m/%d/%Y')
        schType = 'AGC'
        genName = stationAliasDict[agcDf.iloc[rowIter, 1]]
        for blk in range(1, 97):
            schVal = agcDf.iloc[rowIter, 2+blk]
            dataRows.append({'util_name': genName, 'block': blk, 'sch_type': schType,
                             'val': 4.0*float(schVal), 'sch_date': schDate})
    return dataRows


def getSchRowsFromAgcCsv(csvPath):
    agcDf = pd.read_csv(csvPath)
    schRows = getSchRowsFromAgcDf(agcDf)
    return schRows


def pushSchRowsFromAgcFolder(agcFolderPath):
    schAdapter = SchDbAdapter()
    schAdapter.connectToDb()
    for fname in glob.glob(agcFolderPath):
        schRows = getSchRowsFromAgcCsv(fname)
        schAdapter.pushSchRows(schRows)
        nowTimeStr = dt.datetime.strftime(dt.datetime.now(), '%d-%m-%Y %H:%M')
        print("{0} - agc files push done for filename {1}".format(nowTimeStr, fname))
    schAdapter.disconnectDb()
