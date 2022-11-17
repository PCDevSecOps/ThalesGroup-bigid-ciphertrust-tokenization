import oracledb
import os
import sys


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.log import Log                                            # noqa: E402
from databases.connection_interface import DBConnectionInterface     # noqa: E402
from utils.exceptions import OracleConnectorException                # noqa: E402


class OracleConnector(DBConnectionInterface):
    def __init__(self, hostname: str, port: int, sid: str,
            username: str, password: str, *args, **kwargs):
        self._hostname = hostname
        self._port     = port
        self._sid      = sid
        self._username = username
        self._password = password

        self.is_connected = False

        if self.test_connection():
            self._connect()

    def test_connection(self):
        try:
            conn = oracledb.connect(user=self._username,
                                    password=self._password,
                                    dsn=f"{self._hostname}:{self._port}/{self._sid}")
            cursor = conn.cursor()
            cursor.execute("SELECT USER FROM DUAL")
            cursor.close()
            conn.close()
            Log.info(f"Oracle test connection {self._username}@{self._hostname}"
                + f":{self._port}/{self._sid} OK")
            return True

        except Exception as err:
            Log.error(f"Error while testing connection to Oracle: {err}")
            raise OracleConnectorException(err) from err


    def _connect(self):
        try:
            self._conn = oracledb.connect(user=self._username,
                                    password=self._password,
                                    dsn=f"{self._hostname}:{self._port}/{self._sid}")
            Log.info(f"Connected at Oracle {self._username}@{self._hostname}:"
                + f"{self._port}/{self._sid}")
            self.is_connected = True

        except Exception as err:
            Log.error(f"Error while connecting to Oracle {self._username}"
                + f"@{self._hostname}:{self._port}/{self._sid}: {err}")
            self.is_connected = False
            raise OracleConnectorException(err) from err

    def run_query(self, query: str):
        if self.is_connected:
            try:
                cursor = self._conn.cursor()
                cursor.execute(query)
                for row in cursor:
                    print(row)
                self._conn.commit()
                cursor.close()

            except Exception as err:
                Log.error(f"Error while executing Oracle query: {err}")
                raise OracleConnectorException(err) from err
        else:
            Log.warn("Oracle connection is not established. Will not execute query")

    def get_update_query(self, schema: str, table_name: str, token: str, target_col: str,
                target_col_val: str, unique_id_col: str, unique_id_val: str):
        query = f"""
            UPDATE {schema}.{table_name}
            SET {target_col} = '{token}'
            WHERE {target_col} = '{target_col_val}' AND {unique_id_col} = '{unique_id_val}'
        """
        return query
    
    def close_connection(self):
        if self.is_connected:
            self._conn.close()


if __name__ == "__main__":
    o = OracleConnector("192.168.132.19", 1521, "tokendb", "token_user", "token_user")
    o.run_query("SELECT * FROM SAMPLE_DATA WHERE ID = 1")
    o.close_connection()

