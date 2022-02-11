import pypyodbc as pyodbc
import cx_Oracle
import pandas as pd
from psycopg2 import connect as pg_connect
from piper.utils import get_config
from piper.io import read_sql

import logging

logger = logging.getLogger(__name__)

# connections {{{1
def connections(file_name = None,
                return_type = 'dataframe'):
    ''' get all available defined database environments

    Parameters
    ----------
    file_name
        json formatted connections file (similar to tnsnames.ora)
        if None (default) uses ../src/config.json
    return type
        return object type: 'dataframe'(default) 'dictionary'

    Returns
    -------
    dictionary or dataframe

    Examples
    --------

    .. code-block::

        df = connections(return_type='dataframe')
        dict_config = connections(return_type='dictionary')

    '''
    if file_name == None:
        default_config = get_config('config.json')
        file_name = default_config['connections']['location']

    config = get_config(file_name, info=False)
    logger.debug(config)

    if return_type == 'dictionary':
        return config

    if return_type == 'dataframe':
        df = (pd.DataFrame(config).T).fillna('')

        if 'pw' in df.columns:
           df = df.drop(columns=['pw'])

        lowercase_cols = ['schema', 'sid', 'user']
        for col in lowercase_cols:
            if col in df.columns:
                df[col] = df[col].str.lower()

    return df


# _get_postgres_con {{{1
def _get_postgres_con(connection_parms):
    ''' Return postgres connection for given system.

    Examples
    --------

    .. code-block::

        con = _get_postgres_con('beles8')

    Parameters
    ----------
    connection_parms
        connection parameters

    Returns
    -------
    postgres odbc connection
    '''
    host = connection_parms.get('host')
    db   = connection_parms.get('schema')
    user = connection_parms.get('user')
    pw   = connection_parms.get('pw')
    con = pg_connect(host=host, dbname=db, user=user, password=pw)

    logger.info(f"Connection: {host}, user:{user}")

    return con


# _get_oracle_con {{{1
def _get_oracle_con(connection_parms='JDE8EPA'):
    ''' Return Oracle connection and schema, schema_ctl.

    Examples
    --------

    .. code-block::

        con, schema, schema_ctl = _get_oracle_con('JDE8EPA')

    Parameters
    ----------
    connection_parms
        connection_parms environment key connection (values)
        stored in connections.json in default ap folder

    Returns
    -------
    Oracle connection, schema and schema (control) name strings
    '''
    host    = connection_parms.get('host')
    port    = connection_parms.get('port')
    sid     = connection_parms.get('sid')
    service = connection_parms.get('service')
    user    = connection_parms.get('user')
    pw      = connection_parms.get('pw')

    logger.debug(f"Connection: {host}, {port}, {sid}, {svc}")
    logger.info(f"Connection: {host}, {port}")

    dsn_tns = cx_Oracle.makedsn(host=host, port=port, service_name=svc)
    logger.debug(dsn_tns)

    connection = cx_Oracle.connect(user, pw, dsn_tns, encoding="UTF-8")
    logger.debug(f'TNS: {connection.dsn}')
    logger.debug(f'Version: {connection.version}')

    schema = connection_parms.get('schema').lower()
    schema_ctl = connection_parms.get('schema').lower().replace('dta', 'ctl')
    logger.debug(f'Schema: {schema}')

    return connection, schema, schema_ctl
