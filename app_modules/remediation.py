import math
import re

from configparser import RawConfigParser

from cts.cts_request import CTSRequest
from bigid.bigid import BigIDAPI
from databases.mysql_conn import MySQLConnector
from cts.cts_request import CTSRequest
from databases.ds_connection import DataSourceConnection
from utils.log import Log


def run_data_remediation(cts: CTSRequest, bigid: BigIDAPI, config: RawConfigParser, params: dict, tpa_id: str):
    Log.info("Starting remediation")

    # 1. Get a list of all available data sources
    # Conferir URL, proxy, tpa, ID
    all_data_sources = bigid.get_all_data_sources()

    # 2. Filter the DS to get only those with connectors implemented in the API
    implemented_connectors = DataSourceConnection.get_all_implemented_connector_types()
    reachable_data_sources = list(filter(
        lambda x: x["type"] in implemented_connectors, all_data_sources))
    batch_size = params["BatchSize"]

    # 3. For each data source, get a list of remediation objects
    for ds in reachable_data_sources:
        ds_name = ds["name"]
        source_conn = get_ds_connector(bigid, config, tpa_id, ds_name)

        # 4. Get the list of remediation objects in the data source
        remed_objs = bigid.get_remediation_objects_by_source(ds_name)
        if len(remed_objs) == 0:
            continue
        remed_objs_col = bigid.get_remediation_objects_by_source_columns(ds_name)
        # From this one, get policy hit and table size

        # Filter those that have "Thales Tokenization" in actions taken
        remed_objs_col = list(filter(
            lambda x: x["annotations"]["actionTaken"] == "Thales Tokenization", remed_objs))

        # 5. For every policy hit, search in the comments if the database
        # has been tokenized
        for col_obj in remed_objs_col:
            # Check comments here to get the tokenized columns
            obj_full_qual_name = col_obj["fully_qualified_name"]
            non_col_obj = list(filter(lambda x: x["fullyQualifiedName"] == obj_full_qual_name, remed_objs))[0]

            id_for_comment = non_col_obj["id"]
            comments = bigid.get_object_comments(id_for_comment)
            tokenized_columns = get_tokenized_cols_from_comments(comments)
            # table_size = non_col_obj["total_pii_count"]
            # table_size = col_obj["annotations"]["findings"]
            # Run query to get table size

            full_object_name      = non_col_obj["fullObjectName"]
            schema, table_name = full_object_name.split(".")

            pkeys = source_conn.get_primary_keys(source_conn, table_name)
            if len(pkeys) == 0:
                Log.warn(f"No primary keys found in {ds_name} - {obj_full_qual_name}. Skipping...")
                continue
            pkey_names = [i[1] for i in pkeys]

            for col_hit_name in col_obj["annotations"]["policyHit"]:
                if col_hit_name in tokenized_columns:
                    Log.info(f"Column {col_hit_name} is already tokenized. Skipping")
                    continue
            
                # Choose if there are viable primary keys for tokenization

                tokenize_column(cts, source_conn, schema, table_name, col_hit_name, table_size, batch_size)

                # Tag as tokenized
                # Comment that tokenization was performed on column X at time Y


    # conn = MySQLConnector("192.168.0.108", "3306", "TokenizationDemo", "test", "Thales123!")
    # cts = CTSRequest("cts", "test", "Thales123!", r"C:\Users\rafae\OneDrive\Área de Trabalho\thales_bigid_anonimization_api\cts.pem")
    # for offset, fetchnext in offset_fetchnext_iter(10, 3, 100):
    #     pkeys, data = get_batch_pkey_data(conn, "customer_generic", "cpf", "email", offset, fetchnext)
    #     tokens = cts.tokenize(data, "tokenization_group", "alphanum")
    #     update_multiple_query = """
    #         UPDATE TokenizationDemo.customer_generic
    #         SET email = '%s'
    #         WHERE cpf = '%s'
    #     """
    #     params_mult = [(pk, tk) for pk, tk in zip(pkeys, tokens)]
    #     conn.run_query(update_multiple_query, is_multiple=True, params_mult=params_mult)
    # conn.close()


def get_primary_key(source_conn, table_name: str, schema: str = None) -> list:
    return source_conn.get_primary_keys(table_name, schema)


def tokenize_column(cts: CTSRequest, source_conn, schema: str, table_name: str, col_hit_name: str,
        primary_key: str, params: dict, nlines: int, batch_size: int):
    for offset, fetchnext in offset_fetchnext_iter(nlines, batch_size):
        pkeys, data = get_batch_pkey_data(source_conn, f"{schema}.{table_name}", "cpf", "email", offset, fetchnext)
        tokens = cts.tokenize(data, "tokenization_group", "alphanum")
        update_multiple_query = """
            UPDATE TokenizationDemo.customer_generic
            SET email = '%s'
            WHERE cpf = '%s'
        """
        params_mult = [(pk, tk) for pk, tk in zip(pkeys, tokens)]
        conn.run_query(update_multiple_query, is_multiple=True, params_mult=params_mult)
    conn.close()


def get_tokenized_cols_from_comments(comments: list) -> list:
    cols = []
    re_pattern = r"Column (.*) tokenized by Thales"
    for comment_obj in comments:
        html_comment = comment_obj["comment"]
        re_search = re.search(re_pattern, html_comment)
        if re_search:
            cols.append(re_search.group(1))
    return cols
    

def get_ds_connector(bigid: BigIDAPI, config: RawConfigParser, tpa_id: str, ds_name: str):
    ds_conn_getter = bigid.get_data_source_conn_from_source_name(ds_name)
    ds_conn_getter.set_credentials(
        bigid.get_data_source_credentials(tpa_id, ds_name))
    connector_class, host, port, db = ds_conn_getter.get_conn_param()
    return connector_class(host, port, db,
        ds_conn_getter.get_username(config["BigID"]["encryption_key"]),
        ds_conn_getter.get_password(config["BigID"]["encryption_key"]))


def get_nlines(ds_conn, table_name: str) -> int:
    query = f"SELECT COUNT(*) FROM {table_name}"
    nlines = ds_conn.run_query(query, fetch_results=True)
    return nlines[0][0]


def offset_fetchnext_iter(nlines: int, batch_size: int, start_offset: int = 0) -> tuple:
    for i in range(math.ceil(nlines / batch_size)):
        offset = i * batch_size + start_offset
        fetch_next = min(batch_size, nlines - i * batch_size)
        yield (offset, fetch_next)


def get_batch_pkey_data(ds_conn, table_name: str, primary_key: str, column: str,
        offset: int, fetch_next: int) -> tuple:
    batch = ds_conn.get_batch(table_name, primary_key, column, offset, fetch_next)
    pkeys = [p[0] for p in batch]
    data = [d[0] for d in batch]
    return pkeys, data

