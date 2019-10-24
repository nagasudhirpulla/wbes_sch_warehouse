import cx_Oracle
import datetime as dt
import os

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


def getDcRowsBetweenDates(from_dt, to_dt):
    sqlTxt = '''select table1.*, table2.acronym, table2.util_type_id, table2.isgs_type_id from 
    (SELECT util_id, declared_for_date, DECLARED_ON_BAR, SELLER_ONBAR_IP, ON_BAR_INSTALLED_CAPACITY, CLOSED_CYCLE_ON_BAR, OPEN_CYCLE_ON_BAR, DECLARED_OFF_BAR, ramp_up, ramp_down FROM WBES_OLD.declaration where (util_id, declared_for_date, revision_no) in 
    (
        select util_id, declared_for_date, max(revision_no) from WBES_OLD.declaration 
        WHERE declared_for_date between :from_date_key and :to_date_key and is_scheduled=1
        and util_id in (select util_id from WBES_NR7.utility where is_active = 1 and region_id=2)
        GROUP BY util_id, declared_for_date
    )) table1
    left join WBES_NR7.utility table2 on table1.util_id = table2.util_id
    order by declared_for_date, acronym'''
    # from_date_key = from_dt.strftime('%Y-%m-%d')
    from_date_key = from_dt.date()
    # to_date_key = to_dt.strftime('%Y%m%d')
    to_date_key = to_dt.date()

    oracle_connection_string = getWbesReadOnlyConnStr()
    con = cx_Oracle.connect(oracle_connection_string)
    cur = con.cursor()
    cur.prepare(sqlTxt)
    cur.execute(None, {'from_date_key': from_date_key,
                       'to_date_key': to_date_key})
    res = cur.fetchall()
    cur.close()
    return res


from_dt = dt.datetime(2019, 7, 2)
to_dt = dt.datetime(2019, 7, 2)
x = getDcRowsBetweenDates(from_dt, to_dt)
