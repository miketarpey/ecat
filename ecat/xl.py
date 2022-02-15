import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


def _calc_width(df):
    ''' Given dataframe, calculate optimum column widths

    Adapted from:
    `<http://polymathprogrammer.com/2010/01/18/calculating-column-widths-in-excel-open-xml/>`_

    Examples
    --------
    .. code-block::
        for ix, width in enumerate(_calc_width(df)['max_']):
            ws.set_column(ix, ix, width)
    '''
    maxlen_colnames = [len(x) for x in df.columns]
    max_col_names = pd.DataFrame(maxlen_colnames, columns=['max_col_names'])

    maxlen_cols = [max(df.iloc[:, col].astype(str).apply(len))
                   for col in range(len(df.columns))]
    max_cols = pd.DataFrame(maxlen_cols, columns=['max_cols'])

    widths = pd.concat([max_col_names, max_cols], axis=1, sort=False)

    f = lambda x: x + x*((256/x)/256)

    widths['max_'] = widths.max(axis=1).apply(f)

    return widths['max_']


def write_excel(df: pd.DataFrame, filename: str='outputs/Book1.xlsx',
                date_prefix: bool=True, sheet_name: str='Sheet1',
                freeze_panes: tuple=(1, 0)) -> None:
    ''' For given dataframe export/write to Excel

    Parameters
    ----------
    df
        Pandas DataFrame
    filename
        Default 'outputs/Book1.xlsx' - Excel output file name.
    date_prefix
        Default True. If True, insert date prefix to give filename.
        E.g.
        If filename = 'outputs/Book1.xlsx' then date_prefix=True
        gives 'outputs/20221008_Book1.xlsx'
    sheet_name
        Default 'Sheet1'. Excel worksheet name
    freeze_panes
        Default (1, 0). Freeze first line of worksheet.


    Returns
    -------
    None

    '''

    filename = Path(filename)

    if date_prefix:
        ts = "{:%Y%m%d_}".format(datetime.now())
        filename = filename.parents[0] / f'{ts}{filename.stem}{filename.suffix}'

    # Remove underscores from column headings (they mess up formatting headings)
    columns = df.columns.str.replace('_', ' ')

    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:

        df.to_excel(writer, sheet_name='Sheet1', startrow=1,
                    header=False, freeze_panes=freeze_panes, index=False)

        wb = writer.book
        wrap = wb.add_format({'text_wrap': 1})
        ws = writer.sheets[sheet_name]

        header_fmt = lambda x: {'header': x, 'header_format': wrap}
        column_settings = [header_fmt(col) for col in columns]
        settings = {'columns': column_settings}

        (max_row, max_col) = df.shape
        ws.add_table(0, 0, max_row, max_col - 1, settings)

        for ix, width in enumerate(_calc_width(df)):
            ws.set_column(ix, ix, width)
        # ws.set_column(0, max_col - 1, 18)

    logger.info(f'{filename} ({sheet_name}) created.')
