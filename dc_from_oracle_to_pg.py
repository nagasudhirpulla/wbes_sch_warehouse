import cx_Oracle
import datetime as dt
import os
import pandas as pd


def getWbesReadOnlyConnStr():
    source_db_username = os.getenv(
        'WBES_READONLY_USERNAME', 'source_db_username')
    source_db_password = os.getenv(
        'WBES_READONLY_PASSWORD', 'source_db_password')
    source_db_host = os.getenv('WBES_READONLY_HOST', 'source_db_host')
    source_db_port = os.getenv('WBES_READONLY_PORT', 'source_db_port')
    source_db_service = os.getenv(
        'WBES_READONLY_SERVICE', 'WBES_READONLY_SERVICE')
    oracle_connection_string = '{source_db_username}/{source_db_password}@{source_db_host}:{source_db_port}/{source_db_service}'.format(
        source_db_username=source_db_username,
        source_db_password=source_db_password,
        source_db_host=source_db_host,
        source_db_port=source_db_port,
        source_db_service=source_db_service
    )
    return oracle_connection_string


def getDcDfBetweenDates(from_dt, to_dt):
    dbName = "WBES_OLD" if (
        dt.datetime.now() - from_dt).days > 6 else "WBES_NR7"
    sqlTxt = '''select table1.*, table2.acronym, table2.util_type_id, table2.isgs_type_id from 
    (SELECT util_id, declared_for_date, DECLARED_ON_BAR, SELLER_ONBAR_IP, ON_BAR_INSTALLED_CAPACITY, CLOSED_CYCLE_ON_BAR, OPEN_CYCLE_ON_BAR, DECLARED_OFF_BAR, ramp_up, ramp_down FROM {0}.declaration where (util_id, declared_for_date, revision_no) in 
    (
        select util_id, declared_for_date, max(revision_no) from {0}.declaration 
        WHERE declared_for_date between :from_date_key and :to_date_key and is_scheduled=1
        and util_id in (select util_id from WBES_NR7.utility where is_active = 1 and region_id=2)
        GROUP BY util_id, declared_for_date
    )) table1
    left join WBES_NR7.utility table2 on table1.util_id = table2.util_id
    order by declared_for_date, acronym'''.format(dbName)
    # from_date_key = from_dt.strftime('%Y-%m-%d')
    from_date_key = from_dt.date()
    # to_date_key = to_dt.strftime('%Y%m%d')
    to_date_key = to_dt.date()

    oracle_connection_string = getWbesReadOnlyConnStr()
    con = cx_Oracle.connect(oracle_connection_string)
    cur = con.cursor()
    res = None
    try:
        cur.prepare(sqlTxt)
        cur.execute(None, {'from_date_key': from_date_key,
                           'to_date_key': to_date_key})
        colNames = [x[0] for x in cur.description]
        curRows = cur.fetchall()
        res = pd.DataFrame(curRows, columns=colNames)
    finally:
        if cur is not None:
            cur.close()
        return res


"""
For thermal isgs, util_id=2, isgs_type_id=1. For other isgs, util_id=2, isgs_type_id!=1. For gas, util_id=13.
"""


def convertDcDfToDbRows(dcDf):
    dbRows = []
    for dcRowIter in range(len(dcDf)):
        dcRow = dcDf.iloc[dcRowIter]
        utilName = dcRow['ACRONYM'].replace(" ", "_")
        utilTypeId = dcRow['UTIL_TYPE_ID']
        isgsTypeId = dcRow['ISGS_TYPE_ID']
        dcDate = dcRow['DECLARED_FOR_DATE']
        dcDateStr = dt.datetime.strftime(dcDate, '%Y-%m-%d')
        onBarDcForSch = [float(x) for x in dcRow['DECLARED_ON_BAR'].split(',')]
        rampUps = [float(x) for x in dcRow['RAMP_UP'].split(',')]
        rampDowns = [float(x) for x in dcRow['RAMP_DOWN'].split(',')]
        # check for None values
        if utilName == None or dcDate == None or len(onBarDcForSch) != 96 or len(rampUps) != 96 or len(rampDowns) != 96:
            continue
        for blk in range(96):
            dbRows.append({'util_name': utilName, 'sch_type': 'onBarForSch',
                           'sch_date': dcDateStr, 'block': blk+1, 'val': onBarDcForSch[blk]})
            dbRows.append({'util_name': utilName, 'sch_type': 'rampUp',
                           'sch_date': dcDateStr, 'block': blk+1, 'val': rampUps[blk]})
            dbRows.append({'util_name': utilName, 'sch_type': 'rampDn',
                           'sch_date': dcDateStr, 'block': blk+1, 'val': rampDowns[blk]})
        # get off bar dc values for generators with isgs type 1 or None
        if isgsTypeId == 1 or isgsTypeId == None:
            offBarVals = [float(x)
                          for x in dcRow['DECLARED_OFF_BAR'].split(',')]
            for blk in range(96):
                dbRows.append({'util_name': utilName, 'sch_type': 'offBar',
                               'sch_date': dcDateStr, 'block': blk+1, 'val': offBarVals[blk]})
        # check if the generator is gas and get open cycle and closed cycle onbar values
        if utilTypeId == 13:
            closedOnBarVals = [
                float(x) for x in dcRow['CLOSED_CYCLE_ON_BAR'].split(',')]
            openOnBarVals = [float(x)
                             for x in dcRow['OPEN_CYCLE_ON_BAR'].split(',')]
            for blk in range(96):
                dbRows.append({'util_name': utilName, 'sch_type': 'ccOnBar',
                               'sch_date': dcDateStr, 'block': blk+1, 'val': closedOnBarVals[blk]})
                dbRows.append({'util_name': utilName, 'sch_type': 'ocOnBar',
                               'sch_date': dcDateStr, 'block': blk+1, 'val': openOnBarVals[blk]})
        # check if the generator is isgs thermal and get seller dc and onbar installed capacity values
        if utilTypeId == 2 and isgsTypeId == 1:
            sellerOnBarVals = [float(x)
                               for x in dcRow['SELLER_ONBAR_IP'].split(',')]
            installedOnBarVals = [
                float(x) for x in dcRow['ON_BAR_INSTALLED_CAPACITY'].split(',')]
            for blk in range(96):
                dbRows.append({'util_name': utilName, 'sch_type': 'sellOnBar',
                               'sch_date': dcDateStr, 'block': blk+1, 'val': sellerOnBarVals[blk]})
                dbRows.append({'util_name': utilName, 'sch_type': 'icOnBar',
                               'sch_date': dcDateStr, 'block': blk+1, 'val': installedOnBarVals[blk]})
    return dbRows


def getDcDbRowsForDates(from_dt, to_dt):
    dcDf = getDcDfBetweenDates(from_dt, to_dt)
    dbRows = convertDcDfToDbRows(dcDf)
    return dbRows
