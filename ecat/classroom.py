from datetime import datetime
import logging
import re
from pathlib import Path
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

format = '%(asctime)s %(message)s'
datefmt='%d %b %y %H:%M:%S'
logging.basicConfig(level=logging.INFO, format=format, datefmt=datefmt)


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

        self.df = df
        total_rows, total_cols = self.df.shape
        logger.info(f'{self.filename}: {total_rows} rows, {total_cols} columns.')


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

        query = f"DATE_LASTMODIFIED>='{filter_date}'"
        self.df = self.df.query(query)

        total_rows, total_cols = self.df.shape
        logger.info(f'{self.filename}: {query}, {total_rows} rows, {total_cols} columns.')

        return self.df


    def is_missing_data(self) -> bool:
        ''' Determine whether PRODUCTCODE_ID is numeric or not null '''

        missing = False
        total_isna = self.df.query("PRODUCTCODE_ID.isna()").shape[0]
        total_not_numeric = self.df.query("~PRODUCTCODE_ID.str.isnumeric()").shape[0]

        if total_isna > 0 or total_not_numeric:
            logger.info(f'<< ERROR >>')
            logger.info(f'total_null_product_id = {total_isna}')
            logger.info(f'total_not_numeric_product_id = {total_not_numeric}')

        return missing


    def get_keys(self) -> list:
        ''' Return list of PRODUCTCODE_ID + BAXTER_PRODUCTCODE '''

        concated_keys = self.df.PRODUCTCODE_ID + self.df.BAXTER_PRODUCTCODE
        keys = '(' + ', '.join(list("'" + concated_keys + "'" )) + ')'

        return keys
