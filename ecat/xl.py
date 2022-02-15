import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

format = '%(asctime)s %(message)s'
datefmt='%d %b %y %H:%M:%S'
logging.basicConfig(level=logging.INFO, format=format, datefmt=datefmt)


def write_excel(df, filename='outputs/Book1.xlsx',
                    date_prefix=True, sheet_name='Sheet1'):
    ''' For given dataframe export/write to Excel'''

    filename = Path(filename)
    if date_prefix:
        ts = "{:%Y%m%d_}".format(datetime.now())
        filename = filename.parents[0] / f'{ts}{filename.stem}{filename.suffix}'

    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:

        df.to_excel(writer, sheet_name='Sheet1', startrow=1,
                    header=False, freeze_panes=(1, 0), index=False)

        wb = writer.book
        wrap = wb.add_format({'text_wrap': 1})
        ws = writer.sheets[sheet_name]

        header_fmt = lambda x: {'header': x, 'header_format': wrap}
        column_settings = [header_fmt(col) for col in df.columns]
        settings = {'columns': column_settings}

        (max_row, max_col) = df.shape
        ws.add_table(0, 0, max_row, max_col - 1, settings)

        ws.set_column(0, max_col - 1, 18)

    logger.info(f'{filename} ({sheet_name}) created.')
