import re
import pandas as pd
import logging
from typing import Union, List
from jinja2 import Environment, FileSystemLoader
from copy import deepcopy

logger = logging.getLogger(__name__)


def generate_sql(template_sql:str, template_values:dict,
                 template_dir='templates/') -> str:
    ''' Generate eCatalogue sql insert/update/delete statements.

    Parameters
    ----------
    template_sql
        template SQL text file
    parameters
        dictionary of key/values to substitute values
    template_dir
        template directory

    Returns
    -------
    Rendered SQL statement

    '''
    loader = FileSystemLoader(searchpath=template_dir)
    env = Environment(loader=loader, trim_blocks=True)

    template_code = env.get_template(name=template_sql)

    # Convert 'comment' list into concatenated string delimitted with '\n--'
    template_values_copy = deepcopy(template_values)
    template_values_copy['comment'] = '\n-- '.join(template_values_copy['comment'])

    sql = template_code.render(template_values_copy)

    return sql


def series_to_str(series: pd.Series):
    ''' Convert pandas series to a string enclosed in parentheses

    Parameters
    ----------
    series
        pandas Series

    Returns
    -------
    str representation of Series object enclosed in parentheses.

    '''

    str_series = ', '.join(series.astype(str).tolist())
    str_series = '(' + str_series + ')'

    return str_series

