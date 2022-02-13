import cx_Oracle
import json
import logging
import pandas as pd
import psycopg2
import pypyodbc as pyodbc

logger = logging.getLogger(__name__)

format = '%(asctime)s %(message)s'
datefmt='%d %b %y %H:%M:%S'
logging.basicConfig(level=logging.INFO, format=format, datefmt=datefmt)


class Connections():
    ''' Connections class to encapsulate connecting to databases '''


    def __init__(self, file_name=None):
        ''' '''

        self.connections = self.get_config(file_name, return_type='dictionary')

        try:
            client_path = self.connections['oracle'].get('client_path')
            cx_Oracle.init_oracle_client(lib_dir=client_path)
        except cx_Oracle.ProgrammingError as e:
            pass


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
            df = df[df.index != 'oracle']

            if 'pw' in df.columns:
               df = df.drop(columns=['pw'])
               df = df.drop(columns=['client_path'])

            lowercase_cols = ['schema', 'sid', 'user']
            for col in lowercase_cols:
                if col in df.columns:
                    df[col] = df[col].str.lower()

        return df


    def get_connection(self, db=None):
        ''' Return connection and schema, schema_ctl.

        Parameters
        ----------
        connection
            connection name


        Returns
        -------
        Oracle connection, schema and schema (control) name strings


        Examples
        --------

        .. code-block::

            connections = Connections()
            con = connections.get_connection('Copernicus PRD')

        '''
        try:
            connection_details = self.connections[db]
        except KeyError as e:
            logger.info(f'Invalid environment {db}')
            return

        logger.info(f'Environment: {db}')

        host   = connection_details.get('host')
        driver = connection_details.get('driver').lower()
        port   = connection_details.get('port')
        sid    = connection_details.get('sid')
        svc    = connection_details.get('service')
        schema = connection_details.get('schema')
        user   = connection_details.get('user')
        pw     = connection_details.get('pw')

        if driver == 'oracle':
            dsn_tns = cx_Oracle.makedsn(host=host, port=port, service_name=svc)
            connection = cx_Oracle.connect(user, pw, dsn_tns, encoding="UTF-8")
            logger.debug(f'TNS: {connection.dsn}')
            logger.debug(f'Version: {connection.version}')

            return connection

        if driver == 'postgres':
            try:
                connection = psycopg2.connect(user=user, password=pw, host=host,
                                              port=port, database=schema)
                logger.info(f"Connected to {host}, port {port}")
                logger.info(f"schema/database {schema}")
                logger.debug(connection.get_dsn_parameters())
            except Exception as error:
                logger.info(f"Error connecting to {db} {error}")
                return None

            logger.info(f'Connection status: {connection.status}')
            return connection
