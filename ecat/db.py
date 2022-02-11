import pypyodbc as pyodbc
import cx_Oracle
import pandas as pd
import json
from psycopg2 import connect as pg_connect

import logging

logger = logging.getLogger(__name__)

class Connections():

    def __init__(self, file_name=None):
        ''' '''

        self.connections = self.get_config(file_name, return_type='dictionary')


    def get_config(self, file_name=None, return_type='dataframe'):
        ''' get all available defined database environments

        Parameters
        ----------
        file_name
            json formatted connections file (similar to tnsnames.ora)
            if None (default) uses 'connections.json' in current directory
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
            file_name = 'connections.json'

        with open(file_name) as f:
                config = json.load(f)

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


    def get_oracle_con(self, connection=None):
        ''' Return Oracle connection and schema, schema_ctl.

        Examples
        --------

        .. code-block::

            con, schema, schema_ctl = _get_oracle_con('JDE8EPA')

        Parameters
        ----------
        connection
            connection name

        Returns
        -------
        Oracle connection, schema and schema (control) name strings
        '''
        host = self.connections.get('host')
        port = self.connections.get('port')
        sid  = self.connections.get('sid')
        svc  = self.connections.get('service')
        user = self.connections.get('user')
        pw   = self.connections.get('pw')

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


    def get_postgres_con(self, connection=None):
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
        host = self.connections.get('host')
        db   = self.connections.get('schema')
        user = self.connections.get('user')
        pw   = self.connections.get('pw')
        con = pg_connect(host=host, dbname=db, user=user, password=pw)

        logger.info(f"Connection: {host}, user:{user}")

        return con
