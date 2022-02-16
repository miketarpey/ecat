import re
import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Union, List, Dict
from jinja2 import Environment, FileSystemLoader
from copy import deepcopy

logger = logging.getLogger(__name__)


def render_sql(template_sql:str, template_values:dict,
                 template_dir='templates/') -> str:
    ''' Generate eCatalogue rendered sql

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

    # Output SQL in text file
    ts = "{:%Y%m%d_}".format(datetime.now())
    filename = Path(template_values['rendered_SQL'])
    filename = filename.parents[0] / f'{ts}{filename.stem}{filename.suffix}'
    with open(filename, 'w') as f:
        f.write(sql)

    logger.info(f'{filename} created.')

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


    Example
    -------
    series = pd.Series([1, 2, 3, 4, 5])
    series = series_to_str(series)
    series
    >'(1, 2, 3, 4, 5)'

    '''

    str_series = ', '.join(series.astype(str).tolist())
    str_series = '(' + str_series + ')'

    return str_series


def get_template_config(filename: str='templates/templates_config.json') -> Dict:
    ''' Get template setup (json) data as a dictionary

    Parameters
    ----------
    filename
        Default 'templates/templates_config.json'
        Filename containing template configuration

    Returns
    -------
    template setup by stage as a dictionary

    '''
    with open(filename) as f:
        template_config = json.load(f)

    return template_config
