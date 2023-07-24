import pyodbc

from typing import Union

from databases.connection_interface import DBConnectionInterface
from utils.log import Log
from utils.exceptions import SQLServerConnectorException

class SQLServerConnector (DBConnectionInterface):
    def __init__(self, driver:str, hostname: str, port: int, database: str,
            username: str, password: str, encrypt: str, *args, **kwargs):
        self._hostname = hostname
        self._port     = port
        self._database      = database
        self._username = username
        self._password = password
        self._driver = driver
        self._encrypt = encrypt

        self.is_connected = False

        self._connect()

    def _connect(self):
        try: 
            self._conn = pyodbc.connect(DRIVER=self._driver, SERVER=self._hostname,DATABASE=self._database,ENCRYPT=self._encrypt,UID=self._username, PWD=self._password)            
            Log.info(f"Connected at SQLServer {self._username}@{self._hostname}:"
                     + f"{self._port}/{self._database}")
            self.is_connected = True
            
        except Exception as err:
            Log.error(f"Error while connecting to SQLServer {self._username}"
                 + f"@{self._hostname}:{self._port}/{self._database}: {err}")
            self.is_connected = False
            raise SQLServerConnectorException(err) from err
        
    def run_query_old(self, query: str, fetch_results: bool = False):
        if self.is_connected:
            try:
                cursor = self._conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall() if fetch_results else None
                self._conn.commit()
                cursor.close()
                if rows:
                    return rows

            except Exception as err:
                Log.error(f"Error while executing SQLServer query: {err}")
                raise SQLServerConnectorException(err) from err
        else:
            Log.warn("SQLServer connection is not established. Will not execute query")

    def run_query(self, query: str, fetch_results: bool = False, is_multiple: bool = False,
            params_mult: list = None):
        if self.is_connected:
            try:
                cursor = self._conn.cursor()

                if is_multiple:
                    cursor.executemany(query, params_mult)
                else:
                    cursor.execute(query)

                rows = cursor.fetchall() if fetch_results else None
                Log.info("SQLServer Query execution OK")
                self._conn.commit()
                cursor.close()
                if rows:
                    return rows

            except Exception as err:
                Log.error(f"Error while executing SQLServer query: {err}")
                raise Exception(err) from err
        else:
            Log.warn("SQLServer connection is not established. Will not execute query")

    def get_primary_keys(self, table_name: str, schema: str = None):
        source = table_name.upper()
        query = f"""
            SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
            FROM all_constraints cons, all_cons_columns cols
            WHERE cols.table_name = '{source}'
            AND cons.constraint_type = 'P'
            AND cons.constraint_name = cols.constraint_name
            AND cons.owner = cols.owner
            ORDER BY cols.table_name, cols.position
        """
        pkey_list = self.run_query(query, fetch_results = True)
        if pkey_list:
            return [pk[1] for pk in pkey_list]
        return []

    def get_update_query(self, table_name: str, token: Union[str, list],
            target_col: Union[str, list], target_col_val: Union[str, list],
            unique_id_col: str, unique_id_val: str, schema: str = None):

        if isinstance(token, list) and isinstance(target_col, list)\
                and isinstance(target_col_val, list):
            set_str = ",".join(f"\"{target}\" = '{val}'" for target, val in zip(target_col, token))
            where_str = " AND ".join(f"\"{target}\" = '{original_val}'" for target,
                original_val in zip(target_col, target_col_val))
        else:
            set_str = f"\"{target_col}\" = '{token}'"
            where_str = f"\"{target_col}\" = '{target_col_val}'"

        query = f"""
            UPDATE {table_name} SET
            {set_str}
            WHERE {where_str} AND \"{unique_id_col}\" = '{unique_id_val}'
        """
        return query

    def get_batch(self, table_name: str, primary_key: str, column: str, offset: int,
            fetch_next: int, schema: str = None) -> list:
        source = f"{schema.upper()}.{table_name.upper()}" if schema else table_name.upper()
        query = f"""
            SELECT {primary_key}, {column}
            FROM {source}
            ORDER BY {primary_key}
            OFFSET {offset} ROWS FETCH NEXT {fetch_next} ROWS ONLY
        """
        Log.info(query)
        return self.run_query(query, fetch_results=True)
    
    def close_connection(self):
        if self.is_connected:
            self._conn.close()
            Log.info(f"SQLServer closed connection {self._username}@"
                + f"{self._hostname}:{self._port}/{self._database}")
