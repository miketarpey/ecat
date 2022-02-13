from datetime import datetime
import cx_Oracle
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

format = '%(asctime)s %(message)s'
datefmt='%d %b %y %H:%M:%S'
logging.basicConfig(level=logging.INFO, format=format, datefmt=datefmt)


class STATUS():
    ''' Class to encapsulate e-Catalogue status values '''

    def __init__(self):

        self.status = {10257: 'NotForCatalog', 10258: 'PendingDataEntry',
                       10259: 'PendingApproval', 10260: 'Approved',
                       10261: 'Rejected', 10262: 'Withdrawn',
                       10263: 'PendingProductLaunch', 10264: 'Obsolete',
                       1000218: 'NotThisCompany'}

    def get(self, key):
        return self.status.get(key)

    def get_dataframe(self):
        self.df = pd.DataFrame([self.status]).T.reset_index()
        self.df.columns = ['status', 'status_description']

        return self.df


class product_code():
    ''' Class to encapsulate the productcode and p_productcode tables in ecat database '''


    def __init__(self, published=False, keys=None, connection=None):
        ''' '''
        if published:
            self.table = 'p_productcode'
        else:
            self.table = 'productcode'

        if keys is None:
            logger.info(f'{self.table}: You MUST pass a list of keys')
            return None
        else:
            sql = f'''select * from {self.table}
                      where productcode_id||baxter_productcode in {keys}'''
            self.connection = connection
            self.df = pd.read_sql(sql, connection)

            total_rows, total_cols = self.df.shape
            logger.info(f'{self.table}: {total_rows} rows, {total_cols} columns.')

    def get_dataframe(self):
        return self.df


class reimport_log():
    ''' Class to encapsulate the reimport_log table in ecat database '''


    def __init__(self, table='test_bp_reimport_log', connection=None):
        ''' '''

        self.table = table
        self.connection = connection


    def get_last_update(self):
        ''' Get last_update from table, return datetime object
        '''

        parse_format = '%d-%b-%y %I.%M.%S.000000 %p'

        try:
            with self.connection.cursor() as c:
                sql = f'select to_char(max(date_reimport_ts)) from {self.table}'
                last_updated = c.execute(sql).fetchone()[0]
                last_updated = datetime.strptime(last_updated, parse_format)
        except cx_Oracle.DatabaseError as e:
            self.connection.rollback()
            logger.info(e)
            return None

        return last_updated


    def insert(self, file_updated):
        ''' insert reimport log record'''

        try:
            with self.connection.cursor() as c:
                fields = '(date_reimport_ts, updated_p_pc, updated_pc)'
                updated = file_updated.strftime('%d-%b-%y %I.%M.%S.000000 %p')
                values = updated, "0", "0"
                statement = f'insert into {self.table} {fields} VALUES{values}'
                logger.debug(statement)
                c.execute(statement)
                self.connection.commit()
        except cx_Oracle.DatabaseError as e:
            self.connection.rollback()
            logger.info(e)
            return None

        logger.info(f'{self.table}: Inserted row: date_reimport_ts={updated}')


class reimport():
    ''' Class to encapsulate the reimport table in ecat database '''


    def __init__(self, table='temp_bp_class_reimport_data', connection=None):
        ''' '''
        self.table = table
        self.connection = connection


    def get_columns(self):

        sql = f'select * from {self.table} where 1=2'
        df = pd.read_sql(sql, self.connection).fillna(np.nan)

        return df.columns


    def upload(self, df=None):
        '''
        Upload pandas dataframe containing converted/validated reimport data
        to TEMP_BP_CLASS_REIMPORT_DATA table.

        Parameters
        ----------
        df
            pandas data frame

        Returns
        -------
            None
        '''
        try:
            logger.info(f'{self.table}: Clearing(truncate).')

            cursor = self.connection.cursor()
            sql = f'truncate table {self.table}'
            cursor.execute(sql)
            self.connection.commit()

            column_positions = ', '.join([f':{col}' for col in range(1, df.shape[1]+1)])
            statement = f'insert into {self.table} values({column_positions})'
            logger.debug(statement)

            row_values = self._prepare_rowvalues_for_db(df)
            cursor.executemany(statement, row_values, batcherrors=True)

            for error in cursor.getbatcherrors():
                logger.info(f'Error, {error.message}, at row offset, {error.offset}')
                lastGoodRow = row_values[error.offset-1:error.offset]
                failedRow = row_values[error.offset:error.offset+1]
                logger.info("lastGoodRow: " + str(error.offset-1))
                logger.info(lastGoodRow)
                logger.info("failedRow: " + str(error.offset))
                logger.info(failedRow)
                raise

        except cx_Oracle.DatabaseError as e:
            self.connection.rollback()
            logger.info(statement)
            logger.info(e)
            raise
            pass

        finally:
            self.connection.commit()
            cursor.close()
            pass

        logger.info(f'{self.table}: Inserted {df.shape[0]} rows.')


    def _prepare_rowvalues_for_db(self, df):
        '''
        '''
        dx = df.copy(deep=True)

        db_datefmt = '%d-%b-%Y %I.%M.%S.00000000'
        dx.DATE_APPROVED = dx.DATE_APPROVED.dt.strftime(db_datefmt)
        dx.DATE_LASTMODIFIED = dx.DATE_LASTMODIFIED.dt.strftime(db_datefmt)

        row_values = dx.replace(to_replace={np.NaN: None})
        row_values = row_values.values.tolist()

        logger.info(f'{self.table}: Date columns prepared for DB update')

        return row_values