from itertools import groupby

from bigid.bigid import BigIDAPI
from cts.cts_request import CTSRequest
from utils.log import Log
from utils.utils import (get_bigid_user_token, get_unique_id_record,
                         read_config_file)


class AppService:

    def __init__(self):
        self.config = read_config_file("config.ini")
        self.bigid_user_token = get_bigid_user_token(self.config["BigID"]["user_token_path"])
        Log.info("AppService Initialized")

    def data_anonimization(self, post_args: dict = None):
        action_params = post_args["actionParams"]
        params = {i["paramName"]: i["paramValue"] for i in action_params}
        tpa_id = post_args["tpaId"]

        bigid = BigIDAPI(self.config, post_args["bigidBaseUrl"])
        bigid.update_minimization_requests()
        minimization_requests = bigid.get_minimization_requests()
        if len(minimization_requests.keys()) == 0:
            Log.info("No deletion requests found! Exiting action")
            return

        # CTS
        cts_hostname = self.config["CTS"]["hostname"]
        cts_cert_path = self.config["CTS"]["certificate"]
        cts = CTSRequest(cts_hostname, params["CTSUsername"], params["CTSPassword"], cts_cert_path)
        Log.info("CTSRequest initialized")


        for request_id, del_info in minimization_requests.items():

            Log.info(f"---   Processing {request_id=}")

            # Filter only records that as selected for "Delete Manually"
            selected_objects = del_info["selected"]
            records = bigid.get_sar_report(request_id)
            records = list(filter(lambda x: x["fullObjectName"] in selected_objects, records))

            # Group by data source
            for source_name, grouped_records in groupby(records, lambda x: x["source"]):
                Log.info(f"Initiating the anonimization for the data source {source_name}")
                ds_conn_getter = bigid.get_data_source_conn_from_source_name(source_name)
                ds_conn_getter.set_credentials(bigid.get_data_source_credentials(tpa_id, source_name))
                connect_ds_anonimize(ds_conn_getter, cts, list(grouped_records), params, self.config)

            for del_id in del_info["ids"]:
                bigid.set_minimization_request_action(request_id, "Completion Delete Manually", del_id)


def read_categories(categories_raw: str):
    categories = [cat.strip() for cat in categories_raw.split(",")]
    return categories


def category_allowed(categories_found: list, categories_allowed: list) -> bool:
    if len(categories_allowed) == 0:
        return True
    for cat_f in categories_found:
        if cat_f in categories_allowed:
            return True
    return False


def update_table(record: dict, unique_id_record: dict,
        source_conn, token: str):
    target_col            = record["attr_original_name"]
    target_col_val        = record["value"]
    full_object_name      = record["fullObjectName"]
    _, schema, table_name = full_object_name.split(".")

    update_query = source_conn.get_update_query(schema, table_name, token,
        target_col, target_col_val, unique_id_record["attr_original_name"],
        unique_id_record["value"])

    source_conn.run_query(update_query)


def connect_ds_anonimize(ds_conn_getter, cts: CTSRequest, grouped_records: list, params: dict, config):
    # Data source connection
    connector_class, host, port, db = ds_conn_getter.get_conn_param()
    source_conn = connector_class(host, port, db,
        ds_conn_getter.get_username(config["BigID"]["encryption_key"]),
        ds_conn_getter.get_password(config["BigID"]["encryption_key"]))

    categories = read_categories(params["Categories"])
    Log.info(f"Categories that will be anonimized: {categories}")

    try:
        # Group by proximityId/Line
        for proximity_id, records_groupby_table in groupby(grouped_records,
                lambda x: x["proximityId"]):
            Log.info(f"Starting anonimization for {proximity_id=}")

            # Find unique_id
            unique_id_record = get_unique_id_record(records_groupby_table)
            unique_id_col_name = None
            if unique_id_record is not None:
                unique_id_col_name = unique_id_record["attr_original_name"]
            else:
                # Unique_id / primary key not found. Skipping to avoid wrong update statements
                Log.info(f"{proximity_id=} does not have a unique_id or primary "
                    + "key. Skipping anonimization to avoid wrong data replacements")
                continue

            # Filter all records that are not primary key or unique id
            filt = lambda x: x["attr_original_name"] != unique_id_col_name and x["value"] \
                    and category_allowed(x["category"], categories) and x["is_primary"] == "FALSE"
            remaining_records = list(filter(filt, grouped_records))
            Log.info(f"Found {len(remaining_records)} records for anonimization, "
                + "except unique identifier")

            values = [rec["value"] for rec in remaining_records]
            tokens = cts.tokenize(values, params["CTSTokengroup"], params["CTSTokentemplate"])
            Log.info("Data tokenized successfully")

            Log.info("Updating data with tokens...")
            for token, rec in zip(tokens, remaining_records):
                update_table(rec, unique_id_record, source_conn, token)
            Log.info("Updating data with tokens OK")

            if category_allowed(unique_id_record["category"], categories):
                Log.info("Unique identifier is selected for anonimization")
                token = cts.tokenize(unique_id_record["value"], params["CTSTokengroup"],
                    params["CTSTokentemplate"])[0]
                update_table(unique_id_record, unique_id_record, source_conn, token)
                Log.info("Unique identifier anonimized")

    except Exception as err:
        Log.error(f"Exception found in connect_ds_anonimize: {err}")
        source_conn.close_connection()
        raise err

    source_conn.close_connection()


if __name__ == "__main__":
    app = AppService()
