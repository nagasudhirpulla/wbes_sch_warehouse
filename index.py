# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 10:30:27 2019

@author: Nagasudhir
"""

import datetime as dt
from data_update_script import updateSchedules

# nowDay = dt.datetime.now()
nowDay = dt.datetime(2020, 5, 1)
startDay = dt.datetime(2020, 4, 1)
targetDates = []
for dayIter in range((nowDay-startDay).days+1):
    targetDt = startDay + dt.timedelta(days=dayIter)
    targetDates.append(targetDt)
updateSchedules(targetDates)

# import datetime as dt
# import pandas as pd
# import numpy as np
# from wbes_req_utils import getAllIsgsReqRowsForDate

# startDay = dt.datetime(2019, 1, 1)
# nowDay = dt.datetime.now()
# dataRows = []
# for dayIter in range((nowDay-startDay).days+1):
#     targetDt = startDay + dt.timedelta(days=dayIter)
#     dataRows = dataRows + getAllIsgsReqRowsForDate(targetDt)
# reqDf = pd.DataFrame(data=dataRows)
# df2 = reqDf.pivot_table(
#         values='val', 
#         index=['sch_date', 'block'], 
#         columns=['seller_name', 'buyer_name', 'sch_type'], 
#         aggfunc=np.sum)
# df2.to_csv('req_data_rearranged.csv', index=True)