import time
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.log import Log                         # noqa: E402
from utils.exceptions import BigIDAPIException    # noqa: E402
from utils.utils import json_get_request, read_config_file, get_bigid_user_token
from utils.ds_connection import DataSourceConnection


class BigIDAPI:
    def __init__(self, config, base_url: str):
        self._config                  = config
        self._user_token              = get_bigid_user_token(self._config["BigID"]["user_token_path"])
        self.base_url                 = base_url
        self._access_token_h_duration = 23     # Access token duration in hours

        self._access_token_time        = None
        self._access_token             = None
        self.minimization_requests     = []    # [{"requestId":"<requestId>","database":"<rdb-mysql>"},...]

        self._update_session_token()
    
    def _update_session_token(self):
        token_url = f"{self.base_url}refresh-access-token"
        headers = {
            "Accept": "application/json",
            "Authorization": self._user_token
        }
        get_response = json_get_request(token_url, headers)
        
        if get_response.status_code != 200:
            Log.error(f"BigID session token HTTP {get_response.status_code}")
            raise BigIDAPIException(f"BigID access token request failed with status code {get_response.status_code}: {get_response.text}")
        
        self._access_token = get_response.json()["systemToken"]
        self._access_token_time = time.time()
        Log.info("BigID session token updated")
    
    def validate_session_token(self):
        if time.time() - self._access_token_time > self._access_token_h_duration * 3600:
            self._update_session_token()

    def update_minimization_requests(self):
        self.validate_session_token()
        token_url = f"{self.base_url}data-minimization/objects"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token
        }
        get_response = json_get_request(token_url, headers)
        
        if get_response.status_code != 200:
            Log.error(f"BigID minimization request failed with status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException(f"BigID minimization request failed with status code {get_response.status_code}: {get_response.text}")
        
        get_response = get_response.json()
        for req in get_response["data"]["deleteQueries"]:
            if req["state"] == "Pending":
                database_name = req["scannerType"]
                request_id = req["requestId"]
                self.minimization_requests.append({
                    "requestId": request_id,
                    "database": database_name
                })
        Log.info(f"Got {len(self.minimization_requests)} minimization requests from BigID")
            
    def get_minimization_requests(self):
        return self.minimization_requests
    
    def get_sar_report(self, request_id: str):
        self.validate_session_token()

        sar_url = f"{self.base_url}sar/reports/{request_id}"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token
        }
        get_response = json_get_request(sar_url, headers)

        if get_response.status_code != 200:
            Log.error(f"BigID sar report failed with status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException(f"BigID sar report failed with status code {get_response.status_code}: {get_response.text}")
        
        get_response = get_response.json()
        Log.info(f"Got sar report from BigID for {request_id=}")
        return get_response["records"]
    
    def get_data_source_conn_from_source_name(self, data_source_name: str):
        self.validate_session_token()
        token_url = f"{self.base_url}ds_connections/{data_source_name}"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token
        }
        get_response = json_get_request(token_url, headers)
        
        if get_response.status_code != 200:
            Log.error(f"BigID data source request failed with status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException(f"BigID data source request failed with status code {get_response.status_code}: {get_response.text}")
        
        get_response = get_response.json()
        rdb_url = get_response["ds_connection"]["rdb_url"]
        rdb_name = get_response["ds_connection"]["rdb_name"]
        type = get_response["ds_connection"]["type"]
        return DataSourceConnection(rdb_url, type, rdb_name)
    
    def get_data_source_credentials(self, tpaId: str, data_source_name: str):
        self.validate_session_token()
        token_url = f"{self.base_url}tpa/{tpaId}/credentials/{data_source_name}"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token
        }
        get_response = json_get_request(token_url, headers)
        
        if get_response.status_code != 200:
            Log.error(f"BigID data source credentials request failed with status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException(f"BigID data source credentials request failed with status code {get_response.status_code}: {get_response.text}")
        
        get_response = get_response.json()
        return get_response

        
        



if __name__ == "__main__":
    config = read_config_file("config.ini")
    a = BigIDAPI(config, "https://192.168.0.115/api/v1/")
    a.update_minimization_requests()
    print(a.get_minimization_requests())
    print(a.get_sar_report("6363b7905c59b7c0f545662e"))
    # print(config.sections())
    # print(dict(config["CTS"]))
    # for sec in config.sections():
    #     for key in config[sec]:
    #         print(f"[{sec}].{key} = {config[sec][key]}")
    
    # token_url = f"https://{config['BigID']['hostname']}/api/v1/refresh-access-token"
    # headers = {
    #     "Accept": "application/json",
    #     "Authorization": get_bigid_user_token("bigid_user_token.txt")
    # }
    # print(json_get_request(token_url, headers).text)
    # import os
    # print(os.path.basename(__file__))
    # Log.info("Testing log without filename")
    # Log.info("Testing log with filename", os.path.basename(__file__))