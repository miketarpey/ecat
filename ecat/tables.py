from datetime import datetime
from ecat.constants import COMMON_COLS
import cx_Oracle
import logging
import numpy as np
import pandas as pd
from typing import Union

logger = logging.getLogger(__name__)

format = '%(asctime)s %(message)s'
datefmt='%d %b %y %H:%M:%S'
logging.basicConfig(level=logging.INFO, format=format, datefmt=datefmt)


class product_code():
    ''' Class to encapsulate the productcode and p_productcode tables in ecat database '''

    def __init__(self, published: bool=False, keys: list=None,
            connection: Union[cx_Oracle.Connection]=None) -> None:
        ''' product_code / p_productcode constructor

        Parameters
        ----------
        published
            Default False. Retrieve product_code table data
            If True, retrieve p_productcode table data
        keys
            A list of keys ()

        Returns
        -------
        None

        '''
        if published:
            self.table = 'p_productcode'
        else:
            self.table = 'productcode'

        if keys is None:
            logger.info(f'{self.table}: You MUST pass a list of keys')
            return
        else:
            sql = f'''select * from {self.table}
                      where productcode_id||baxter_productcode in {keys}'''
            self.connection = connection
            self.df = pd.read_sql(sql, connection)
            self.df = self.df.fillna(np.NaN)
            self.df = self.df.sort_values('PRODUCTCODE_ID')
            self.df = self.df.reset_index(drop=True)

            self.set_common_cols()

            total_rows, total_cols = self.df.shape
            logger.info(f'{self.table}: {total_rows} rows, {total_cols} columns.')

    def set_common_cols(self) -> None:
        ''' Define commmon fields between classroom CSV, product and p_product'''
        common_cols = COMMON_COLS()
        self.common_cols = common_cols.get()

    def get_dataframe(self, common_fields_only:bool=True)-> pd.DataFrame:

        if common_fields_only:
            logger.info(f'{self.table}: <<Common>> columns only')
            dx = self.df[self.common_cols]
        else:
            dx = self.df

        total_rows, total_cols = dx.shape
        logger.info(f'{self.table}: {total_rows} rows, {total_cols} columns.')

        return dx


class reimport_log():
    ''' Class to encapsulate the reimport_log table in ecat database '''


    def __init__(self, table:str='test_bp_reimport_log',
                 connection: Union[cx_Oracle.Connection]=None) -> None:
        ''' '''

        self.table = table
        self.connection = connection

    def get_last_update(self) -> datetime:
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


    def insert(self, file_updated: datetime) -> None:
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
            return

        logger.info(f'{self.table}: Inserted row: date_reimport_ts={updated}')


class reimport():
    ''' Class to encapsulate the reimport table in ecat database '''


    def __init__(self, table: str='temp_bp_class_reimport_data',
                 connection: Union[cx_Oracle.Connection]=None) -> None:
        ''' '''
        self.table = table
        self.connection = connection

    def get_columns(self) -> list:

        sql = f'select * from {self.table} where 1=2'
        df = pd.read_sql(sql, self.connection).fillna(np.nan)

        return df.columns

    def upload(self, df: pd.DataFrame=None) -> None:
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
            with self.connection.cursor() as cursor:
                sql = f'truncate table {self.table}'
                cursor.execute(sql)
                self.connection.commit()
                logger.debug(f'{self.table}: {sql}.')

                col_positions = ', '.join([f':{col}' for col in range(1, df.shape[1]+1)])
                statement = f'insert into {self.table} values({col_positions})'
                logger.debug(statement)

                row_values = self._prepare_rowvalues_for_db(df)
                cursor.executemany(statement, row_values, batcherrors=True)
                self.connection.commit()

                for error in cursor.getbatcherrors():
                    logger.info(f'Error @row {error.offset}: {error.message}')
                    lastGoodRow = row_values[error.offset-1:error.offset]
                    failedRow = row_values[error.offset:error.offset+1]
                    logger.info("lastGoodRow: " + str(error.offset-1))
                    logger.info(lastGoodRow)
                    logger.info("failedRow: " + str(error.offset))
                    logger.info(failedRow)

        except cx_Oracle.DatabaseError as e:
            self.connection.rollback()
            logger.info(statement)
            logger.info(e)

        logger.info(f'{self.table}: Inserted {df.shape[0]} rows.')


    def _prepare_rowvalues_for_db(self, df:pd.DataFrame) -> list:
        '''
        '''
        dx = df.copy(deep=True)

        db_datefmt = '%d-%b-%Y %I.%M.%S.00000000'
        dx.DATE_APPROVED = dx.DATE_APPROVED.dt.strftime(db_datefmt)
        dx.DATE_LASTMODIFIED = dx.DATE_LASTMODIFIED.dt.strftime(db_datefmt)

        row_values = dx.replace(to_replace={np.NaN: None})
        row_values = row_values.values.tolist()

        logger.debug(f'{self.table}: Date columns prepared for DB update')

        return row_values
