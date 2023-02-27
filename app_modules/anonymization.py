from itertools import groupby
from configparser import RawConfigParser
from typing import Union

from bigid.bigid import BigIDAPI
from cts.cts_request import CTSRequest
from databases.ds_connection import DataSourceConnection
from utils.log import Log
import utils.utils as ut


def run_data_anonymization(config: RawConfigParser, params: dict, tpa_id: str, cts: CTSRequest,
        bigid: BigIDAPI):

    bigid.update_minimization_requests()
    minimization_requests = bigid.get_minimization_requests()
    if len(minimization_requests.keys()) == 0:
        Log.info("No deletion requests found! Exiting action")
        return

    for request_id, del_info in minimization_requests.items():

        Log.info(f"---   Processing {request_id=}")

        # Filter only records that as selected for "Delete Manually"
        selected_objects = del_info["selected"]
        records = bigid.get_sar_report(request_id)
        records = list(filter(lambda x: x["fullObjectName"] in selected_objects, records))

        # Group by data source
        for source_name, grouped_records in groupby(records, lambda x: x["source"]):
            Log.info(f"Initiating the anonymization for the data source {source_name}")
            ds_conn_getter = bigid.get_data_source_conn_from_source_name(source_name)
            ds_conn_getter.set_credentials(
                bigid.get_data_source_credentials(tpa_id, source_name))
            connect_ds_anonymize(ds_conn_getter, cts, list(grouped_records),
                params, config)

        bigid.set_minimization_request_action(request_id,
            "Completion Delete Manually", del_info["ids"])


def update_table(records: Union[list, str], unique_id_record: dict,
        source_conn, tokens: Union[list, str]):
    """
    Generates and runs the update query.
    If the arguments are lists, all fields will anonymized in a single query
    """
    if isinstance(tokens, list) and isinstance(records, list):
        target_cols           = [rec["attr_original_name"] for rec in records]
        target_col_vals       = [rec["value"] for rec in records]
        full_object_name      = records[0]["fullObjectName"]
        _, schema, table_name = full_object_name.split(".")
    else:
        target_cols           = records["attr_original_name"]
        target_col_vals       = records["value"]
        full_object_name      = records["fullObjectName"]
        _, schema, table_name = full_object_name.split(".")

    Log.info(f"{target_cols}, {target_col_vals}, {full_object_name}, {table_name}")

    update_query = source_conn.get_update_query(table_name, tokens,
        target_cols, target_col_vals, unique_id_record["attr_original_name"],
        unique_id_record["value"])
    
    source_conn.run_query(update_query)



def connect_ds_anonymize(ds_conn_getter: DataSourceConnection, cts: CTSRequest,
        grouped_records: list, params: dict, config: RawConfigParser):

    # Data source connection
    connector_class, host, port, db = ds_conn_getter.get_conn_param()
    source_conn = connector_class(host, port, db,
        ds_conn_getter.get_username(config["BigID"]["encryption_key"]),
        ds_conn_getter.get_password(config["BigID"]["encryption_key"]))

    categories = ut.read_categories(params["Categories"])
    Log.info(f"Categories that will be anonymized: {categories}")

    try:
        # Group by proximityId/Line
        for proximity_id, records_groupby_table in groupby(list(grouped_records),
                lambda x: x["proximityId"]):

            Log.info(f"Starting anonymization for {proximity_id=}")

            proximity_group = list(records_groupby_table)

            # Find unique_id
            unique_id_record = ut.get_unique_id_record(proximity_group)
            unique_id_col_name = None
            if unique_id_record is not None:
                unique_id_col_name = unique_id_record["attr_original_name"]
                Log.info(f"Unique ID column: {unique_id_col_name}")
            else:
                Log.info(f"{proximity_id=} does not have a unique_id or primary "
                    + "key. Skipping anonymization to avoid wrong data replacements")
                continue

            # Filter all records that are not primary key or unique id
            filt = lambda x: x["attr_original_name"] != unique_id_col_name and x["value"] \
                    and ut.category_allowed(x["category"], categories) and x["is_primary"] == "FALSE"
            remaining_records = list(filter(filt, proximity_group))
            Log.info(f"Found {len(remaining_records)} records for anonymization, "
                + "except unique identifier")
            
            if len(remaining_records) > 0:
                values = [rec["value"] for rec in remaining_records]
                tokens = cts.tokenize(values, params["CTSTokengroup"], params["CTSTokentemplate"])
                Log.info("Data tokenized successfully")

                Log.info("Updating data with tokens...")
                update_table(remaining_records, unique_id_record, source_conn, tokens)
                Log.info("Updating data with tokens OK")

            if ut.category_allowed(unique_id_record["category"], categories):
                Log.info("Unique identifier is selected for anonymization")
                Log.info(unique_id_record["value"])
                token = cts.tokenize(unique_id_record["value"], params["CTSTokengroup"],
                    params["CTSTokentemplate"])[0]
                Log.info("tokenized")

                update_table(unique_id_record, unique_id_record, source_conn, token)
                Log.info("Unique identifier anonymized")

    except Exception as err:
        Log.error(f"Exception found in connect_ds_anonymize: {err}")
        source_conn.close_connection()
        raise err

    source_conn.close_connection()