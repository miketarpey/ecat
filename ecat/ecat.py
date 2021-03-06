import pandas as pd
import logging
from typing import Union
from pathlib import Path
from ecat.tables import reimport_log, reimport, product_code
from ecat.db import Connections
from ecat.classroom import artikel
from ecat.analysis import generate_analysis, compare_data
from ecat.sql import get_template_config, render_sql, series_to_str
from ecat.version import __version__
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")


logger = logging.getLogger(__name__)

format = '%(asctime)s %(message)s'
datefmt='%d %b %y %H:%M:%S'
logging.basicConfig(level=logging.INFO, format=format, datefmt=datefmt)

logger.info(f'ecat version {__version__}')


def classroom_upload(filename: Path, database: str='eCatalogDEV',
        last_update: Union[None, str]=None, update: bool=False) -> None:
    ''' Upload classroom item data to the Baxter eCatalogue database.

    The function attempts to capture the process of updating the e-Catalogue
    database with class.room database item data:

    - For given CSV file import, validate & transform to a pandas dataframe.

    - Make a connection to test/production eCatalogue database

    - Read reimport_log table to determine last time reimport table
    - was updated. Use this date to filter out records in CSV file that
    - have been 'updated' since this date.

    - Filter the CSV data using the last_updated date.

    - Make a connection to the reimport table, check that the columns
    - match what is already identified in the CSV data file.

    - If no 'missing data' in the CSV, upload the CSV to the reimport table.


    Parameters
    ----------
    filename
        name of CSV extract file containing articles/item data from class.room
    database
        name of e-Catalogue database.
        Valid values are: eCatalogDEV, eCatalogPRD
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
    from ecat.ecat import classroom_upload

    filename = Path('inputs') / 'export_artikel_20220204200253.csv'

    classroom_upload(filename=filename, database='eCatalogDEV',
                    last_update='20211102', update=True)
    '''
    connections = Connections()
    con = connections.get_connection(database)
    if con is None:
        return

    if last_update is None:
        log_table = reimport_log(connection=con)
        last_updated = log_table.get_last_update()
    else:
        last_updated = datetime.strptime(last_update, '%Y%m%d')
        logger.info('')
        logger.info('<< ::TEST:: RE-IMPORT DATE - MANUAL OVERRIDE >>')

    logger.info('')
    logger.info('1. Import classroom data, filter')
    classroom_data = artikel(filename)
    csv_file_date = classroom_data.get_filename_date()
    if csv_file_date < last_updated:
        msg = f'CSV file date {csv_file_date} < last DB update {last_updated}'
        logger.info(msg)
        logger.info(f'NO UPDATE TO eCatalogue database.')
        return

    df = classroom_data.filter_data(filter_date=last_updated)
    if classroom_data.invalid_data():
        return

    logger.info('')
    logger.info('2. Get Reimport table meta-data')
    reimport_table = reimport(connection=con)
    reimport_columns = reimport_table.get_columns()
    if list(df.columns) != list(reimport_columns):
        msg = f'Error: CSV cols {len(df.columns)} <> Re-import cols {len(reimport_columns)}'
        logger.info(msg)
        return

    if not update:
        logger.info('<< ::TEST:: NO UPDATES MADE >>')
    else:
        logger.info('')
        logger.info('3. Upload classroom item data')
        reimport_table.upload(df)

        logger.info('')
        logger.info('4. Update reimport_log with last update')
        log_table = reimport_log(connection=con)
        log_table.insert(last_updated)


def classroom_analyse(filename: Path, database: str='eCatalogDEV',
        last_update: Union[None, str]=None) -> None:
    '''  Analyse classroom item data before updating Baxter eCatalogue database.

    This function analyses/compares classroom item data.

    - Get classroom CSV data, filter by date and identify all item 'keys'
      to allow retrieval of corresponding product and p_product data.

    - Analyse/compare classroom items with corresponding items in eCAT DB.
      Generate Excel workbook showing item, status and whether or not the
      classroom item exists in the eCAT product and p_product tables.

    - Generate two further 'difference' Excel workbooks showing:
       a) Items common to classroom and products
       b) Items common to classroom and p_products


    Parameters
    ----------
    filename
        name of CSV extract file containing articles/item data from class.room
    database
        name of e-Catalogue database.
        Valid values are: eCatalogDEV, eCatalogPRD
    last_update
        Default None. If None, use the last_update from reimport log table.
        Can be specified to manually override reimport log table value or
        used for testing.


    Returns
    -------
    None

    '''
    connections = Connections()
    con = connections.get_connection(database)
    if con is None:
        return

    logger.info('')
    classroom_data = artikel(filename)

    if last_update is None:
        log_table = reimport_log(connection=con)
        last_updated = log_table.get_last_update()
    else:
        last_updated = datetime.strptime(last_update, '%Y%m%d')
        logger.info('')
        logger.info('<< ::TEST:: RE-IMPORT DATE - MANUAL OVERRIDE >>')

    logger.info('')
    logger.info('1. Import classroom data, filter')
    df_classroom = (classroom_data.filter_data(filter_date=last_updated)
                                  .sort_values('PRODUCTCODE_ID'))

    if classroom_data.invalid_data():
        return

    classroom_keys = classroom_data.get_keys()

    logger.info('')
    logger.info('2. Using classroom item keys, get productcode, p_productcode')
    product = product_code(keys=classroom_keys, published=False, connection=con)
    df_product = product.get_dataframe(common_fields_only=True)

    p_product = product_code(keys=classroom_keys, published=True, connection=con)
    df_p_product = p_product.get_dataframe(common_fields_only=True)

    logger.info('')
    logger.info('3. Analyse classroom items with eCAT DB product data')
    df_analysis = generate_analysis(df_classroom, df_product, df_p_product)

    logger.info('')
    logger.info('4. Compare differences between common classroom & eCAT DB items')
    df_common_classroom = classroom_data.get_dataframe(common_fields_only=True)
    classroom_items = df_common_classroom['PRODUCTCODE_ID']

    # Identify common rows between classroom and product table
    keys = df_analysis['PRODUCTCODE_ID'].loc[df_analysis['PRODUCT']].tolist()
    df_classroom_product = df_common_classroom[classroom_items.isin(keys)]
    df_classroom_product = df_classroom_product.reset_index(drop=True)

    # Identify common rows between classroom and p_product table
    keys = df_analysis['PRODUCTCODE_ID'].loc[df_analysis['P_PRODUCT']].tolist()
    df_classroom_p_product = df_common_classroom[classroom_items.isin(keys)]
    df_classroom_p_product = df_classroom_p_product.reset_index(drop=True)

    logger.info('')
    f ='outputs/ECAT_CSV_vs_PRODUCT.xlsx'
    df_compare = compare_data(df_classroom_product, df_product, df_classroom,
                              table1='csv', table2='product', filename=f)

    f ='outputs/ECAT_CSV_vs_P_PRODUCT.xlsx'
    df_compare = compare_data(df_classroom_p_product, df_p_product, df_classroom,
                              table1='csv', table2='p_product', filename=f)


def render_sqls(filename: str=None) -> None:
    ''' Generate rendered SQL's to update eCatalogue DB

    Overview
    --------

    - Read classroom/ecat analysis workbook.

    - Read template/substitution values for each of the
      FOUR business rules (to update product/p_product) tables.

    - Filter item data and extract associated item codes in
      a 'string' list - based on the business cases above.

    - Apply above values to the appropriate SQL template and
      render/create in an SQL file in the 'outputs' directory.
      (look in templates/templates_config.json for details)


    Parameters
    ----------

    filename
        Excel workbook containing list of classroom items
        and corresponding info on whether item exists in
        productcode and p_productcode tables in eCatalogue DB


    Returns
    -------
    None

    Example
    -------
    from ecat.ecat import render_sqls

    f = 'outputs/20220215_ECAT_Classroom_Item_Analysis - TEST.xlsx'
    render_sqls(filename=f)
    '''

    # Read classroom/ecat analysis summary Excel workbook
    df = pd.read_excel(filename)

    # Make sure column name spaces replaced with underscores
    df.columns = df.columns.str.replace(' ', '_')

    template_config = get_template_config()

    stage1 = template_config['stage1']
    result = df.query("ARTICLE_STATUS != 1000218 and PRODUCT")
    stage1['articles'] = series_to_str(result['PRODUCTCODE_ID'])
    render_sql(template_sql='UPDATE.sql', template_values=stage1)

    stage2 = template_config['stage2']
    result = df.query("ARTICLE_STATUS == 10260 and P_PRODUCT")
    stage2['articles'] = series_to_str(result['PRODUCTCODE_ID'])
    render_sql(template_sql='UPDATE.sql', template_values=stage2)

    stage3 = template_config['stage3']
    result = df.query("ARTICLE_STATUS == 10260 and not (P_PRODUCT)")
    stage3['articles'] = series_to_str(result['PRODUCTCODE_ID'])
    render_sql(template_sql='INSERT.sql', template_values=stage3)

    stage4 = template_config['stage4']
    result = df.query("ARTICLE_STATUS in (10257, 10262, 10263, 10264) and P_PRODUCT")
    stage4['articles'] = series_to_str(result['PRODUCTCODE_ID'])
    render_sql(template_sql = 'DELETE.sql', template_values=stage4)
