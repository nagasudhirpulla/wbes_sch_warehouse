import psycopg2
from app_conf import getConf

class SchDbAdapter:
    conn = None

# object attributes = {'util_name', 'block', 'sch_type', 'val', 'sch_date'}
# schedules table columns = sch_utility, sch_date, sch_block, sch_type, sch_val
    def pushSchRows(self, schRows):
        cur = self.conn.cursor()
        # we will commit in multiples of 150 rows
        rowIter = 0
        insIncr = 500
        numRows = len(schRows)
        while rowIter < numRows:
            # set iteration values
            iteratorEndVal = rowIter+insIncr
            if iteratorEndVal >= numRows:
                iteratorEndVal = numRows

            # Create row tuples
            dataInsertionTuples = []
            for insRowIter in range(rowIter, iteratorEndVal):
                dataRow = schRows[insRowIter]

                dataInsertionTuple = (dataRow['util_name'], dataRow['sch_date'],
                                      dataRow['block'], dataRow['sch_type'],
                                      dataRow['val'])
                dataInsertionTuples.append(dataInsertionTuple)

            # prepare sql for insertion and execute
            dataText = ','.join(cur.mogrify('(%s,%s,%s,%s,%s)', row).decode(
                "utf-8") for row in dataInsertionTuples)
            sqlTxt = 'INSERT INTO public.schedules(\
        	sch_utility, sch_date, sch_block, sch_type, sch_val)\
        	VALUES {0} on conflict (sch_utility, sch_date, sch_block, sch_type) \
            do update set sch_val = excluded.sch_val'.format(dataText)
            cur.execute(sqlTxt)
            self.conn.commit()

            rowIter = iteratorEndVal

        # close cursor and connection
        cur.close()
        return True

    def connectToDb(self):
        dbConfig = getConf("dbConfig")
        self.conn = psycopg2.connect(host=dbConfig['db_host'], dbname=dbConfig['db_name'],
                                     user=dbConfig['db_username'], password=dbConfig['db_password'])

    def disconnectDb(self):
        self.conn.close()
