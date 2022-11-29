import hashlib

from databases.mysql_conn import MySQLConnector
from databases.oracle_conn import OracleConnector
from Cryptodome.Cipher import AES
from base64 import b64decode


class DataSourceConnection:
    """
    Class responsible for returning the correct Data Source connector
    as well as the DS credentials decrypted, based on the encryption
    key given in the config.ini file.
    """
    def __init__(self, rdb_url: str, rdb_type: str, rdb_name: str):
        self._rdb_url     = rdb_url
        self._rdb_type    = rdb_type
        self._rdb_name    = rdb_name
        self._credentials = None

    def set_credentials(self, credentials: dict):
        self._credentials = credentials
    
    def get_username(self, encryption_key: str) -> str:
        if encryption_key is not None and self._credentials["username"]["encrypted"]:
            return self.decrypt(self._credentials["username"]["value"], encryption_key)
        return self._credentials["username"]["value"]
    
    def get_password(self, encryption_key: str) -> str:
        if encryption_key is not None and self._credentials["password"]["encrypted"]:
            return self.decrypt(self._credentials["password"]["value"], encryption_key)
        return self._credentials["password"]["value"]

    @staticmethod
    def decrypt(string: str, encryption_key: str) -> str:
        """
        Decrypts the AES encrypted password given the encryption key
        and the iv, given the input string is in the form <iv>$<cipher>
        """
        def unpad(x):
            return x[:-ord(x[len(x) - 1:])]
        
        private_key = hashlib.sha256()
        private_key.update(encryption_key.encode('utf_8'))
        private_key = private_key.digest()

        iv, original = string.split("$", 1)
        iv = b64decode(iv)
        cipher_text = b64decode(original)
        cipher = AES.new(private_key, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(cipher_text)).decode("utf-8")

    def get_conn_param(self) -> list:
        """
        Returns the correct Data Source Connector based on the rdb_type
        """
        if self._rdb_type == "rdb-mysql":
            return self._get_mysql_conn_params()
        elif self._rdb_type == "rdb-oracle":
            return self._get_oracle_conn_params()
        else:
            raise NotImplementedError("DataSourceConnection does not"
                + f"support {self._rdb_type} yet. Implement it!")

    def _get_mysql_conn_params(self) -> list:
        """
        MySQL URL format: <IP|hostname>:<port>
        """
        hostname, port = self._rdb_url.split(":")
        port = int(port)
        database = self._rdb_name
        return (MySQLConnector, hostname, port, database)

    def _get_oracle_conn_params(self) -> list:
        """
        Oracle URL format: <IP|hostname>:<port>/<SID>
        """
        hostname, port_sid = self._rdb_url.split(":")
        port, sid = port_sid.split("/")
        port = int(port)
        return (OracleConnector, hostname, port, sid)
    