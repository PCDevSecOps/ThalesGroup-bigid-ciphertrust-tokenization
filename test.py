from databases.oracle_conn import OracleConnector
from cts.cts_request import CTSRequest


if __name__ == "__main__":
    # o = OracleConnector("192.168.0.119", 1521, "tokendb", "token_user", "token_user")
    # o.run_query("UPDATE TEST SET EMAIL = NULL WHERE Id = 2")
    # o.close_connection()

    req = CTSRequest("cts", "test", "Thales123!", "cts.pem")
    print(req.tokenize([None, None, None], "tokenization_group", "alphanum"))
