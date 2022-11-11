from bigid.bigid import BigIDAPI
from cts.cts_request import CTSRequest
from utils.utils import read_config_file, get_bigid_user_token, get_unique_id_record
from utils.log import Log
from utils.exceptions import ConnectorNotImplementedException

from itertools import groupby


class AppService:

    def __init__(self):
        self.config = read_config_file("config.ini")
        self.bigid_user_token = get_bigid_user_token(self.config["BigID"]["user_token_path"])
        Log.info(f"AppService Initialized")
    
    def data_anonimization(self, post_args: dict = None):
        # Interpreting arguments passed to the application
        action_params = post_args["actionParams"]
        params = {i["paramName"]: i["paramValue"] for i in action_params}

        bigid = BigIDAPI(self.config, post_args["bigidBaseUrl"])
        bigid.update_minimization_requests()
        minimization_requests = bigid.get_minimization_requests()
        if len(minimization_requests) == 0:
            Log.info("No deletion requests found! Exiting action")
            return

        # CTS
        cts_hostname = self.config["CTS"]["hostname"]
        cts_cert_path = self.config["CTS"]["certificate"]
        cts = CTSRequest(cts_hostname, params["CTSUsername"], params["CTSPassword"], cts_cert_path)
        Log.info(f"CTSRequest initialized")

        tpaId = post_args["tpaId"]

        for min_req in minimization_requests:
            request_id = min_req["requestId"]
            
            # For testing purposes
            # if request_id != "6365483a09838e33efa430c5":
            #     continue

            Log.info(f"---   Processing {request_id=}")

            records = bigid.get_sar_report(request_id)

            # Group by data source
            for source_name, grouped_records in groupby(records, lambda x: x["source"]):
                Log.info(f"Initiating the anonimization for the data source {source_name}")
                ds_conn = bigid.get_data_source_conn_from_source_name(source_name)
                ds_conn.set_credentials(bigid.get_data_source_credentials(tpaId, source_name))
                connect_ds_anonimize(ds_conn, cts, list(grouped_records), self.config, params)


def read_categories(categories_raw: str):
    categories = [cat.strip() for cat in categories_raw.split(",")]
    return categories


def category_allowed(categories_found: list, categories_allowed: list) -> bool:
    if len(categories_allowed) == 0: return True
    for cat_f in categories_found:
        if cat_f in categories_allowed:
            return True
    return False


def connect_ds_anonimize(ds_conn, cts: CTSRequest, grouped_records: list, config, params: dict):
    # Data source connection
    Connector, conn_params = ds_conn.get_conn_param()
    # credentials = ds_conn.get_username_password(config["BigID"]["encryption_key"])
    credentials = ["test", "Thales123!"]
    conn_params += credentials
    conn = Connector(*conn_params)

    categories = read_categories(params["Categories"])
    Log.info(f"Categories that will be anonimized: {categories}")

    try:
        # Group by proximityId/Line
        for proximity_id, records_groupby_table in groupby(grouped_records, lambda x: x["proximityId"]):
            Log.info(f"Starting anonimization for {proximity_id=}")
            # Find unique_id
            unique_id_record = get_unique_id_record(records_groupby_table)
            unique_id_col_name = None
            if unique_id_record is not None:
                unique_id_col_name = unique_id_record["attr_original_name"]
            else:
                # Unique_id / primary key not found. Skipping to avoid wrong update statements
                Log.info(f"{proximity_id=} does not have a unique_id or primary key. Skipping anonimization to avoid wrong data replacements")
                continue

            # Separate record containing unique_id to be anonimized
            filt = lambda x: x["attr_original_name"] != unique_id_col_name and x["value"] \
                    and category_allowed(x["category"], categories) and x["is_primary"] == "FALSE"
            remaining_records = list(filter(filt, grouped_records))
            Log.info(f"Found {len(remaining_records)} records for anonimization, except unique identifier")
            values = [rec["value"] for rec in remaining_records]

            tokens = cts.tokenize(values, params["CTSTokengroup"], params["CTSTokentemplate"])
            Log.info("Data tokenized successfully")

            Log.info("Updating data with tokens...")
            for tk, rec in zip(tokens, remaining_records):
                target_col = rec["attr_original_name"]
                target_col_val = rec["value"]
                full_object_name = rec["fullObjectName"]
                _, schema, table_name = full_object_name.split(".")
                
                update_query = conn.get_update_query(schema, table_name, tk,
                                    target_col, target_col_val, unique_id_col_name, unique_id_record["value"])
                
                conn.run_query(update_query)
            Log.info("Updating data with tokens OK")
            
            if category_allowed(unique_id_record["category"], categories):
                Log.info("Unique identifier is selected for anonimization")
                token = cts.tokenize(unique_id_record["value"], params["CTSTokengroup"], params["CTSTokentemplate"])[0]
                target_col = unique_id_record["attr_original_name"]
                target_col_val = unique_id_record["value"]
                full_object_name = unique_id_record["fullObjectName"]
                _, schema, table_name = full_object_name.split(".")
                
                update_query = conn.get_update_query(schema, table_name, token,
                                    target_col, target_col_val, unique_id_col_name, unique_id_record["value"])
                
                conn.run_query(update_query)
                Log.info("Unique identifier anonimized")
    except Exception as e:
        Log.error(f"Exception found in connect_ds_anonimize: {e}")
        conn.close_connection()
        raise e
    
    conn.close_connection()


if __name__ == "__main__":
    app = AppService()
