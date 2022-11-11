from databases.mysql_conn import MySQLConnector
from databases.oracle_conn import OracleConnector


class DataSourceConnection:
    def __init__(self, rdb_url: str, rdb_type: str, rdb_name: str):
        self.rdb_url  = rdb_url
        self.rdb_type = rdb_type
        self.rdb_name = rdb_name
    
    def set_credentials(self, credentials: dict):
        self.credentials = credentials
    
    def get_username_password(self, encryption_key: str = None) -> list:
        username, password = self.credentials["username"], self.credentials["password"]
        if encryption_key is not None and self.credentials["username"]["encrypted"]:
            username = self.decrypt(self.credentials["username"]["value"], encryption_key)
        if encryption_key is not None and self.credentials["password"]["encrypted"]:
            password = self.decrypt(self.credentials["password"]["value"], encryption_key)
        return [username, password]
    
    @staticmethod
    def decrypt(string: str, encryption_key: str) -> str:
        return string
    
    def get_conn_param(self) -> list:
        if self.rdb_type == "rdb-mysql":
            return self.get_mysql_conn_params()
        elif self.rdb_type == "rdb-oracle":
            return self.get_oracle_conn_params()
        else:
            raise NotImplementedError(f"DataSourceConnection does not support {self.rdb_type} yet. Implement it!")
    
    def get_mysql_conn_params(self) -> list:
        hostname, port = self.rdb_url.split(":")
        port = int(port)
        database = self.rdb_name
        return (MySQLConnector, [hostname, port, database])
    
    def get_oracle_conn_params(self) -> list:
        hostname, port_sid = self.rdb_url.split(":")
        port, sid = port_sid.split("/")
        port = int(port)
        return (OracleConnector, [hostname, port, sid])
    