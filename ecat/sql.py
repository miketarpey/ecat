import re
import pandas as pd
import logging
from typing import Union, List
from jinja2 import Environment, FileSystemLoader

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

    # I know, a hard-coded 'parameter' comment being 'manipulated
    # to remove newline and spaces. Not great coding but can't see
    # a better way of doing this.
    template_values['comment'] = re.sub('\n\s+--', '\n--',
                                        template_values['comment'])

    sql = template_code.render(template_values)

    return sql
