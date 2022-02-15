import re
import pandas as pd
import numpy as np
import logging
from ecat.xl import write_excel
from pathlib import Path
from ecat.constants import COMMON_COLS
from datetime import datetime

logger = logging.getLogger(__name__)


class artikel():
    ''' Class to encapsulate the artikel/item (CSV) data

    Example
    -------
    filename = Path('inputs') / 'export_artikel_20220204200253.csv'
    csv_data = artikel(filename)

    '''

    def __init__(self, filename:Path , delimiter:str='\t',
                 encoding: str='utf-8') -> None:
        '''
        Parameters
        ----------
        filename
            CSV file name
        delimiter
            Default '\t' (TAB)
        encoding
            Default 'utf-8'

        Returns
        -------
        None

        '''

        self.filename = filename
        df = pd.read_csv(self.filename, encoding=encoding,
                         delimiter=delimiter, na_values='(null)')

        df['DATE_APPROVED'] = pd.to_datetime(df['DATE_APPROVED'])
        df['DATE_LASTMODIFIED'] = pd.to_datetime(df['DATE_LASTMODIFIED'])
        df['ARTICLE_STATUS'] = df['ARTICLE_STATUS'].fillna(0).astype(int)
        df['GHX_STATUS'] = df['GHX_STATUS'].fillna(0).astype(int)
        df['CSS_STATUS'] = df['CSS_STATUS'].fillna(0).astype(int)
        df['THERAPIEGRUPPE'] = df['THERAPIEGRUPPE'].fillna(0).astype(int)

        self.set_common_cols()

        self.df = df
        total_rows, total_cols = self.df.shape
        logger.info(f'{self.filename}: Imported {total_rows} rows, {total_cols} columns.')


    def get_filename_date(self) -> datetime:
        ''' Extract date value from filename  '''

        match = re.search('(\d+)', self.filename.as_posix())
        if not match:
            logger.info(f'{self.filename}: Invalid filename')
            return None
        else:
            parse_format = '%Y%m%d%H%M%S'
            new_date = datetime.strptime(match[1], parse_format)
            return new_date


    def filter_data(self, filter_date: datetime=None) -> pd.DataFrame:
        ''' Filter item data based on DATE_LASTMODIFIED '''

        # Note: Records where user LAST_USER = 'JDE_Upload_prd' are
        # filtered out. Only actual 'user' updates need to be considered.
        query = "LAST_USER.str.lower() != 'jde_upload_prd'"
        self.df = self.df.query(query)

        total_rows, total_cols = self.df.shape
        logger.info(f'{self.filename}: Filtered with query: {query}')
        logger.info(f'{self.filename}: Filtered {total_rows} rows, {total_cols} columns.')

        query = f"DATE_LASTMODIFIED>='{filter_date}'"
        self.df = self.df.query(query)

        # Make sure that productcode_id is numeric/integer
        self.df.PRODUCTCODE_ID = pd.to_numeric(self.df.PRODUCTCODE_ID, errors='ignore')

        self.df = self.df.sort_values('PRODUCTCODE_ID').reset_index(drop=True)

        total_rows, total_cols = self.df.shape
        logger.info(f'{self.filename}: Filtered with query: {query}')
        logger.info(f'{self.filename}: Filtered {total_rows} rows, {total_cols} columns.')

        return self.df


    def get_dataframe(self, common_fields_only:bool=True)-> pd.DataFrame:

        if common_fields_only:
            logger.info(f'{self.filename}: <<Common>> columns only')
            dx = self.df[self.common_cols]
        else:
            dx = self.df

        total_rows, total_cols = dx.shape
        logger.info(f'{self.filename}: {total_rows} rows, {total_cols} columns.')

        return dx


    def invalid_data(self) -> bool:
        ''' Determine whether PRODUCTCODE_ID is numeric or not null '''

        invalid_data = False

        df_isna = self.df.query("PRODUCTCODE_ID.isna()")
        total_isna = df_isna.shape[0]
        if total_isna > 0:
            invalid_data = True
            logger.info(f'ERROR: Null product_id -> {total_isna} rows')
            filename='outputs/ECAT_null_products.xlsx'
            write_excel(df_isna, filename=filename)

        df_not_numeric = self.df.loc[~self.df['PRODUCTCODE_ID'].astype(str).str.isnumeric()]
        total_not_numeric = df_not_numeric.shape[0]
        if total_not_numeric > 0:
            invalid_data = True
            logger.info(f'ERROR: Non-numeric product_id -> {total_not_numeric} rows')
            filename='outputs/ECAT_Non_numeric_products.xlsx'
            write_excel(df_not_numeric, filename=filename)

        if invalid_data:
            return True

        # FIX:: PRODUCTCODE_ID needs to be manually set to integer (?, why?)
        df.PRODUCTCODE_ID = pd.to_numeric(df.PRODUCTCODE_ID)

        return False


    def get_keys(self) -> list:
        ''' Return list of PRODUCTCODE_ID + BAXTER_PRODUCTCODE '''

        concated_keys = self.df.PRODUCTCODE_ID.astype(str) +\
                        self.df.BAXTER_PRODUCTCODE.astype(str)
        keys = '(' + ', '.join(list("'" + concated_keys + "'" )) + ')'

        return keys


    def set_common_cols(self) -> None:
        ''' Define commmon fields between classroom CSV, product and p_product'''
        common_cols = COMMON_COLS()
        self.common_cols = common_cols.get()

