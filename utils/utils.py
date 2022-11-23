import configparser
import requests

from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth
from utils.log import Log
from typing import Union


def read_config_file(config_path):
    config = configparser.RawConfigParser()
    config.read(config_path, encoding="utf-8")
    Log.info("Reading from configuration file: OK")
    return config


def get_bigid_user_token(path: str):
    token = ""
    with open(path, "r", encoding="utf-8") as f:
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


def json_post_request(url: str, header: dict, content: dict, verify: Union[bool, str] = False,
        username: str = None, password: str = None):

    auth = None
    if username and password:
        auth = HTTPBasicAuth(username, password)

    with requests.Session() as s:
        retries = Retry(total=3,
                backoff_factor=0.2,
                status_forcelist=[ 500, 502, 503, 504 ],
                raise_on_redirect=True)
        s.mount('https://', HTTPAdapter(max_retries=retries))
        response = s.post(
            url,
            auth=auth,
            verify=verify,
            headers=header,
            json=content,
            timeout=5
        )

    return response


def get_unique_id_record(records: list):
    unique_record = list(filter(lambda x: x["identity_unique_id"] == x["value"], records))
    if unique_record:
        return unique_record[0]

    # Unique ID not found. Searching for the primary key
    pkey_record = list(filter(lambda x: x["is_primary"] == "TRUE", records))
    if pkey_record:
        return pkey_record[0]
    return None
