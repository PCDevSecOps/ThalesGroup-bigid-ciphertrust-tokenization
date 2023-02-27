import math
import re
import datetime

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
    Log.info(str(len(all_data_sources))+" Datasources found.")

    # 2. Filter the DS to get only those with connectors implemented in the API
    implemented_connectors = DataSourceConnection.get_all_implemented_connector_types()
    reachable_data_sources = list(filter(
        lambda x: x["type"] in implemented_connectors, all_data_sources))
    batch_size = int(params["BatchSize"])

    # 3. For each data source, get a list of remediation objects
    for ds in reachable_data_sources:
        ds_name = ds["name"]
        source_conn = get_ds_connector(bigid, config, tpa_id, ds_name)

        # 4. Get the list of remediation objects in the data source
        remed_objs = bigid.get_remediation_objects_by_source(ds_name)
        if len(remed_objs) == 0:
            Log.warn("No Remediation Objects were found.")
            continue
        remed_objs_col = bigid.get_remediation_objects_by_source_columns(ds_name)
        # From this one, get policy hit and table size

        # Filter those that have "Thales Tokenization" in actions taken
        remed_objs_col = list(filter(
            lambda x: x["annotations"]["actionTaken"] == "Thales Tokenization", remed_objs_col))
        Log.warn(str(len(remed_objs_col))+" Remediation objects found.")

        # 5. For every policy hit, search in the comments if the database
        # has been tokenized
        for col_obj in remed_objs_col:
            Log.info(str(col_obj))
            # Check comments here to get the tokenized columns
            obj_full_qual_name = col_obj["fully_qualified_name"]
            non_col_obj = list(filter(lambda x: x["fullyQualifiedName"] == obj_full_qual_name, remed_objs))[0]
            Log.info("Got non column objects using Fully Qualified name")
            annotation_id = non_col_obj["id"]
            Log.info(f"Object annotation ID: {annotation_id}")
            comments = bigid.get_object_comments(annotation_id)
            Log.info("Got comments from Remediation object")
            Log.info(str(comments))
            tokenized_columns = get_tokenized_cols_from_comments(comments)
            Log.info(f"List of columns that are already tokenized: {tokenized_columns}")


            # Run query to get table size
            _, schema, table_name = obj_full_qual_name.split(".")
            table_size = get_nlines(source_conn, table_name)

            full_object_name      = non_col_obj["fullObjectName"]
            schema, table_name = full_object_name.split(".")

            pkeys = source_conn.get_primary_keys(table_name, schema)
            if len(pkeys) == 0:
                Log.warn(f"No primary keys found in {ds_name} - {obj_full_qual_name}. Skipping...")
                continue

            for col_hit_name in col_obj["annotations"]["policyHit"]:
                Log.info(col_hit_name)
                if col_hit_name in tokenized_columns:
                    Log.info(f"Column {col_hit_name} is already tokenized. Skipping")
                    continue
            
                candidate_pkeys = list(filter(lambda x: x != col_hit_name, pkeys))
                if candidate_pkeys:
                    pkey = candidate_pkeys[0]
                else:
                    continue

                # Choose if there are viable primary keys for tokenization

                tkgroup, tktempl = params["CTSTokengroup"], params["CTSTokentemplate"]

                Log.info(f"Tokenizing column {col_hit_name} of {table_name}")
                
                tokenize_column(cts, source_conn, schema, table_name, col_hit_name, pkey, table_size, batch_size, tkgroup, tktempl)

                # Tag as tokenized
                #tag_column_thales_tokenized(bigid, ds_name, col_hit_name, obj_full_qual_name)
                # Comment that tokenization was performed on column X at time Y
                #comment_tokenization(bigid, col_hit_name, annotation_id)


def comment_tokenization(bigid: BigIDAPI, col_tokenized: str, annotation_id: str):
    date_today = datetime.datetime.now().strftime("%Y/%m/%d")
    final_comment = f"<p>Column {col_tokenized} tokenized by Thales at {date_today}</p>"

    bigid.add_comment(final_comment, annotation_id)


def tag_column_thales_tokenized(bigid: BigIDAPI, source_name: str, col_hit_name: str,
        obj_full_qual_name: str):
    
    tag_name = "Thales_Tokenized"
    tag_description = "Tags the columns that were tokenized by the remediation app"

    all_tags = bigid.get_bigid_tags()
    object_tags = bigid.get_object_tags(obj_full_qual_name)

    matching_tags = list(filter(lambda x: x["tagName"] == tag_name, all_tags))
    if matching_tags:
        parent_id = matching_tags[0]["tagId"]
    else:
        parent_id = bigid.create_main_tag(tag_name, tag_description)

    matching_subtags = list(filter(lambda x: x["tagValue"] == col_hit_name, object_tags))
    if not matching_subtags:
        subtag_id, _ = bigid.create_sub_tag(col_hit_name, parent_id,
            f"Thales API Tokenized Column {col_hit_name}")
        bigid.add_tag(obj_full_qual_name, source_name, parent_id, subtag_id)



def get_primary_key(source_conn, table_name: str, schema: str = None) -> list:
    return source_conn.get_primary_keys(table_name, schema)


def tokenize_column(cts: CTSRequest, source_conn, schema: str, table_name: str, col_hit_name: str,
        pkey_col_name: str, nlines: int, batch_size: int, tkgroup: str, tktemplate: str):
    for offset, fetchnext in offset_fetchnext_iter(nlines, batch_size):
        pkeys, data = get_batch_pkey_data(source_conn, table_name, pkey_col_name, col_hit_name, offset, fetchnext)
        tokens = cts.tokenize(data, tkgroup, tktemplate)
        update_multiple_query = f"""
            UPDATE {table_name}
            SET {col_hit_name} = :1
            WHERE {pkey_col_name} = :2
        """
        params_mult = [(tk, pk) for pk, tk in zip(pkeys, tokens)]
        source_conn.run_query(update_multiple_query, is_multiple=True, params_mult=params_mult)
    source_conn.close_connection()


def get_tokenized_cols_from_comments(comments: list) -> list:
    cols = []
    re_pattern = r"Column (.*) tokenized by Thales"
    for comment_obj in comments["results"]:
        html_comment = comment_obj["comment"]["comment"]
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
    batch_size = int(batch_size)
    for i in range(math.ceil(nlines / batch_size)):
        offset = i * batch_size + start_offset
        fetch_next = min(batch_size, nlines - i * batch_size)
        yield (offset, fetch_next)


def get_batch_pkey_data(ds_conn, table_name: str, primary_key: str, column: str,
        offset: int, fetch_next: int) -> tuple:
    batch = ds_conn.get_batch(table_name, primary_key, column, offset, fetch_next)
    pkeys = [p[0] for p in batch]
    data = [d[1] for d in batch]
    return pkeys, data

