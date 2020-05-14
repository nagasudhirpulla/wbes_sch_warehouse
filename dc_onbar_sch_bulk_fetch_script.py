# %%
from wbes_full_sch_utils import getAllIsgsSchRowsForDate
from dc_from_oracle_to_pg import getDcDbRowsForDates
import datetime as dt
import pandas as pd


def getSchOnbarDcDfForDate(targetDt):
    x = getAllIsgsSchRowsForDate(targetDt)
    x = [p for p in x if p['sch_type'] == 'Total']
    df = pd.DataFrame(data=x)
    df = df[['util_name', 'val']]
    df = df.groupby(['util_name']).mean()
    # df['sch_date'] = targetDt.date()
    df['val'] = df['val']*0.024
    df['sch_type'] = 'sch'
    df = df.reset_index()

    schRows = getDcDbRowsForDates(targetDt, targetDt)
    onBarRows = [p for p in schRows if p['sch_type'] == 'onBarForSch']
    onBarDf = pd.DataFrame(data=onBarRows)
    onBarDf = onBarDf[['util_name', 'val']]
    onBarDf = onBarDf.groupby(['util_name']).mean()
    # onBarDf['sch_date'] = targetDt.date()
    onBarDf['val'] = onBarDf['val']*0.024
    onBarDf['sch_type'] = 'onbar'
    onBarDf = onBarDf.reset_index()

    totalDcRows = [p for p in schRows if p['sch_type'] == 'offBar']
    totalDcDf = pd.DataFrame(data=totalDcRows)
    totalDcDf = totalDcDf[['util_name', 'val']]
    totalDcDf = totalDcDf.groupby(['util_name']).mean()
    # totalDcDf['sch_date'] = targetDt.date()
    totalDcDf['val'] = totalDcDf['val']*0.024
    totalDcDf['sch_type'] = 'dc'
    totalDcDf = totalDcDf.reset_index()

    result = pd.concat([df, onBarDf, totalDcDf], axis=0,
                       join='outer', ignore_index=True)
    result['sch_date'] = targetDt.date()

    return result
# %%


endDay = dt.datetime(2019, 5, 13)
startDay = dt.datetime(2019, 3, 1)
targetDates = []
for dayIter in range((endDay-startDay).days+1):
    targetDt = startDay + dt.timedelta(days=dayIter)
    targetDates.append(targetDt)

resDf = pd.DataFrame()
for targetDt in targetDates:
    df = getSchOnbarDcDfForDate(targetDt)
    if len(df.columns) == 0:
        continue

    if len(resDf.columns) == 0:
        resDf = df
    else:
        resDf = pd.concat([resDf, df], axis=0, join='outer', ignore_index=True)
    print('{0} - process done for {1}'.format(dt.datetime.now(), targetDt))

df2 = resDf.pivot_table(
    index=['sch_date'],
    columns=['util_name', 'sch_type'],
    values='val'
)
# %%
genNames = list(set(df2.columns.get_level_values(0)))

for rowIter in range(df2.shape[0]):
    for gen in genNames:
        try:
            df2.iloc[rowIter][gen, 'dc'] = df2.iloc[rowIter][gen,
                                                             'dc'] + df2.iloc[rowIter][gen, 'onbar']
        except:
            x = 1

df2.to_csv('sch_dc_onbar_{0}.csv'.format(dt.datetime.timestamp(dt.datetime.now())))
