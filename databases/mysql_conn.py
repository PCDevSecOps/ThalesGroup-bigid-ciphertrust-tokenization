import mysql.connector

from mysql.connector import Error

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

    def run_query(self, query: str):
        if self.is_connected:
            try:
                if self._connection.is_connected():
                    cursor = self._connection.cursor(buffered=True)
                    cursor.execute(query)
                    # record = cursor.fetchall() # update queries only -> no data to fetch
                    # print(record)
                    Log.info("MySQL Query execution OK")
                    self._connection.commit()
                    cursor.close()

            except Error as err:
                Log.error(f"Error while executing MySQL query: {err}")
                raise MySQLConnectorException(err) from err
        else:
            Log.warn("MySQL connection is not established. Will not execute query")

    def get_update_query(self, schema: str, table_name: str, token: str, target_col: str,
                target_col_val: str, unique_id_col: str, unique_id_val: str):
        query = f"""
            UPDATE {schema}.{table_name}
            SET {target_col} = '{token}'
            WHERE {target_col} = '{target_col_val}' AND {unique_id_col} = '{unique_id_val}'
        """
        return query

    def close_connection(self):
        if self.is_connected and self._connection.is_connected():
            self._connection.close()
            Log.info(f"MySQL closed connection {self._username}@"
                + f"{self._hostname}:{self._port}/{self._database}")


if __name__ == "__main__":
    mysql_conn = MySQLConnector("192.168.0.108", 3306, "TokenizationDemo", "test", "Thales123!")
    mysql_conn.run_query("SELECT SSN FROM sample_data WHERE FNAME = 'Huntley'")
    # mysql_conn.run_query("""UPDATE TokenizationDemo.Data100k
    #   SET CARDNUMBERNUM = 'CC-IjZf329mm0qO4593'
    #   WHERE CARDNUMBERNUM = '4456643983384593' AND CPF = '646.103.270-31'
    # """)
    mysql_conn.close_connection()
