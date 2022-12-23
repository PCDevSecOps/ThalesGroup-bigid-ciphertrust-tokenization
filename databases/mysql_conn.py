import mysql.connector

from mysql.connector import Error
from typing import Union

from databases.connection_interface import DBConnectionInterface
from utils.log import Log
from utils.exceptions import MySQLConnectorException


class MySQLConnector(DBConnectionInterface):
    def __init__(self, hostname: str, port: int, database: str,
            username: str, password: str, *args, **kwargs):
        self._hostname = hostname
        self._port     = port
        self._database = database
        self._username = username
        self._password = password

        self.is_connected = False

        self._connect()


    def _connect(self):
        try:
            self._connection = mysql.connector.connect(host=self._hostname,
                                                       port=self._port,
                                                       database=self._database,
                                                       user=self._username,
                                                       password=self._password,
                                                       connection_timeout=5)
            if self._connection.is_connected():
                Log.info(f"Connected at MySQL {self._username}@"
                    + f"{self._hostname}:{self._port}/{self._database}")
                self.is_connected = True

        except Error as err:
            Log.error(f"Error while connecting to MySQL {self._username}"
                + f"@{self._hostname}:{self._port}/{self._database}: {err}")
            self.is_connected = False
            raise MySQLConnectorException(err) from err

    def run_query(self, query: str, fetch_results: bool = False, is_multiple: bool = False,
            params_mult: list = None):
        try:
            if self._connection.is_connected():
                cursor = self._connection.cursor(buffered=True)

                if is_multiple:
                    cursor.executemany(query, params_mult)
                else:
                    cursor.execute(query)

                rows = cursor.fetchall() if fetch_results else None
                Log.info("MySQL Query execution OK")
                self._connection.commit()
                cursor.close()
                if rows:
                    return rows

        except Error as err:
            Log.error(f"Error while executing MySQL query: {err}")
            raise MySQLConnectorException(err) from err

    def get_primary_keys(self, table_name: str, schema: str = None):
        source = f"{schema}.{table_name}" if schema else table_name
        query = f"""
            SHOW KEYS FROM {source} WHERE Key_name = 'PRIMARY'
        """
        return self.run_query(query, fetch_results = True)

    def get_update_query(self, schema: str, table_name: str, token: Union[str, list],
            target_col: Union[str, list], target_col_val: Union[str, list],
            unique_id_col: str, unique_id_val: str):

        if isinstance(token, list) and isinstance(target_col, list)\
                and isinstance(target_col_val, list):
            set_str = ",".join(f"{target} = '{val}'" for target, val in zip(target_col, token))
            where_str = " AND ".join(f"{target} = '{original_val}'" for target,
                original_val in zip(target_col, target_col_val))
        else:
            set_str = f"{target_col} = '{token}'"
            where_str = f"{target_col} = '{target_col_val}'"

        query = f"""
            UPDATE {schema}.{table_name} SET
            {set_str}
            WHERE {where_str} AND {unique_id_col} = '{unique_id_val}'
        """
        return query
    
    def get_batch(self, table_name: str, primary_key: str, column: str, offset: int,
            fetch_next: int, schema: str = None) -> list:
        source = f"{schema}.{table_name}" if schema else table_name
        query = f"""
            SELECT {primary_key}, {column}
            FROM {source}
            ORDER BY {primary_key}
            LIMIT {fetch_next} OFFSET {offset}
        """
        return self.run_query(query, fetch_results=True)


    def close_connection(self):
        if self.is_connected and self._connection.is_connected():
            self._connection.close()
            Log.info(f"MySQL closed connection {self._username}@"
                + f"{self._hostname}:{self._port}/{self._database}")

