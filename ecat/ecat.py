'''
Updating the e-Catalogue database with class.room database item data

1. Import CSV file, validate and transform to a pandas dataframe.

2. Make a connection to test/production eCatalogue database

3. Read reimport_log table to determine last time reimport table
   was updated. Use this date to filter out records in CSV file that
   have been 'updated' since this date.

4. Filter the CSV file data using the last_updated date.

5. Make a connection to the reimport table, check that the columns
   match what we have found in the CSV data file.

6. If no 'missing data' in the CSV, upload the CSV to the reimport table.
'''
from ecat.tables import reimport_log, reimport, product_code
from ecat.classroom import artikel
from ecat.db import Connections
import pandas as pd
from typing import Union

from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

format = '%(asctime)s %(message)s'
datefmt='%d %b %y %H:%M:%S'
logging.basicConfig(level=logging.INFO, format=format, datefmt=datefmt)


def csv_to_reimport(filename: str=None, database: str='eCatalogDEV',
        last_update: Union[None, str]=None, update: bool=False) -> None:
    ''' Importing artikel/item data from class.room database

    Parameters
    ----------
    filename
        name of CSV extract file containing articles/item data from class.room
    database
        name of e-Catalogue database. Valid values are:
        eCatalogDEV, eCatalogPRD
    last_update
        Default None. If None, use the last_update from reimport log table.
        Can be specified to manually override reimport log table value or
        used for testing.
    update
        Default False. If True, upload/merge CSV data with reimport table.
        Update 'last updated' on reimport log table with filename date.

    Returns
    -------
    None

    Example
    -------
    from ecat.ecat import csv_to_reimport

    filename = Path('inputs') / 'export_artikel_20220204200253.csv'

    csv_to_reimport(filename=filename, database='eCatalogDEV',
                    last_update='20211102', update=True)
    '''
    connections = Connections()
    con = connections.get_connection(database)
    if con is None:
        return

    csv_data = artikel(filename)

    if last_update is None:
        log_table = reimport_log(connection=con)
        last_update = log_table.get_last_update()
    else:
        last_update = datetime.strptime(last_update, '%Y%m%d')
        logger.info('<< ::TEST:: RE-IMPORT DATE - MANUAL OVERRIDE >>')

    csv_file_date = csv_data.get_filename_date()
    if csv_file_date < last_update:
        msg = f'CSV file date {csv_file_date} < last DB update {last_update}'
        logger.info(msg)
        logger.info(f'NO UPDATE TO eCatalogue database.')
        return

    df = csv_data.filter_data(filter_date=last_update)
    if csv_data.is_missing_data():
        return

    # FIX:: PRODUCTCODE_ID needs to be manually set to integer (?, why?)
    df.PRODUCTCODE_ID = pd.to_numeric(df.PRODUCTCODE_ID)

    reimport_table = reimport(connection=con)
    reimport_columns = reimport_table.get_columns()
    if list(df.columns) != list(reimport_columns):
        msg = f'Error: CSV cols {len(df.columns)} <> Re-import cols {len(reimport_columns)}'
        logger.info(msg)
        return

    if not update:
          logger.info('<< ::TEST:: NO UPDATES MADE >>')
    else:
        reimport_table.upload(df)

        log_table = reimport_log(connection=con)
        log_table.insert(last_update)
