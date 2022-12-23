class DBConnectionInterface:
    def _connect(self):
        raise NotImplementedError("Implement connect method")
    
    def get_update_query(self, schema: str, table_name: str, token: str, target_col: str,
                target_col_val: str, unique_id_col: str, unique_id_val: str) -> str:
        raise NotImplementedError("Implement get_update_query method")
    
    def get_batch(self, table_name: str, primary_key: str, column: str,
            offset: int, fetch_next: int) -> list:
        raise NotImplementedError("Implement get_batch method")

    def run_query(self, query: str):
        raise NotImplementedError("Implement run_query method")

    def close_connection(self):
        raise NotImplementedError("Implement close_connection method")
