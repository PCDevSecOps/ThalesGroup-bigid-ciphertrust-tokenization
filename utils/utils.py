import configparser
import requests

from requests.adapters import HTTPAdapter, Retry
from utils.log import Log


def read_config_file(config_path):
    config = configparser.RawConfigParser()
    config.read(config_path, encoding="utf-8")
    Log.info("Reading from configuration file: OK")
    return config


def get_bigid_user_token(path: str):
    token = ""
    with open(path, "r") as f:
        for line in f.readlines():
            token += line.strip()
    Log.info("Got user token")
    return token


def json_get_request(url: str, header: dict):
    with requests.Session() as s:
        retries = Retry(total=3,
                backoff_factor=0.2,
                status_forcelist=[ 500, 502, 503, 504 ],
                raise_on_redirect=True)
        s.mount('https://', HTTPAdapter(max_retries=retries))
        response = s.get(
            url,
            verify=False,
            headers=header,
            timeout=5
        )

    return response


def get_unique_id_record(records: list):
    unique_record = list(filter(lambda x: x["identity_unique_id"] == x["value"], records))
    if unique_record:
        # Found column that corresponds to the identity_unique_id
        return unique_record[0]
    
    # Search for the primary key
    pkey_record = list(filter(lambda x: x["is_primary"] == "TRUE", records))
    if pkey_record:
        # Found column that corresponds to the identity_unique_id
        return pkey_record[0]
    return None



