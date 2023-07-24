import requests
import math

from configparser import RawConfigParser
from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth
from utils.log import Log
from typing import Union


def read_config_file(config_path: str) -> RawConfigParser:
    config = RawConfigParser()
    config.read(config_path, encoding="utf-8")
    Log.info("Reading from configuration file: OK")
    return config


def get_bigid_user_token(path: str) -> str:
    token = ""
    with open(path, "r", encoding="utf-8") as f:
        for line in f.readlines():
            token += line.strip()
    Log.info("Got user token")
    return token


def json_get_request(url: str, header: dict, proxies: dict = None) -> requests.Response:
    with requests.Session() as s:
        if not proxies:
            s.trust_env = False

        retries = Retry(total=3,
                backoff_factor=0.2,
                status_forcelist=[ 500, 502, 503, 504 ],
                raise_on_redirect=True)
        s.mount('https://', HTTPAdapter(max_retries=retries))
        response = s.get(
            url,
            verify=False,
            proxies=proxies,
            headers=header,
            timeout=5
        )

    return response


def json_post_request(url: str, header: dict, content: dict, proxies: dict = None,
        verify: Union[bool, str] = False, username: str = None,
        password: str = None) -> requests.Response:

    auth = None
    if username and password:
        auth = HTTPBasicAuth(username, password)

    with requests.Session() as s:
        if not proxies:
            s.trust_env = False
        retries = Retry(total=3,
                backoff_factor=0.2,
                status_forcelist=[ 500, 502, 503, 504 ],
                raise_on_redirect=True)
        s.mount('https://', HTTPAdapter(max_retries=retries))
        response = s.post(
            url,
            auth=auth,
            verify=verify,
            proxies=proxies,
            headers=header,
            json=content,
            timeout=5
        )

    return response


def get_unique_id_record(records: list) -> dict:
    unique_record = list(filter(lambda x: x["identity_unique_id"] == x["value"], records))
    if unique_record:
        return unique_record[0]

    # Unique ID not found. Searching for the primary key
    pkey_record = list(filter(lambda x: x["is_primary"] == "TRUE", records))
    if pkey_record:
        return pkey_record[0]
    return None


def read_categories(categories_raw: str) -> set:
    if categories_raw.strip():
        categories = set(cat.strip() for cat in categories_raw.strip().split(","))
        return categories
    return set()


def category_allowed(categories_found: list, categories_allowed: Union[list, set]) -> bool:
    if len(categories_allowed) == 0:
        return True
    return any(map(lambda x: x in categories_allowed, categories_found))


def get_proxy_from_config(config: RawConfigParser):
    cfgproxy = config["Proxy"]
    return {key: cfgproxy[key] for key in cfgproxy if cfgproxy[key]}


def offset_fetchnext_iter(nlines: int, batch_size: int, start_offset: int = 0) -> tuple:
    batch_size = int(batch_size)
    for i in range(math.ceil(nlines / batch_size)):
        offset = i * batch_size + start_offset
        fetch_next = min(batch_size, nlines - i * batch_size)
        yield (offset, fetch_next)
    

def merge_anonymization_dicts(dest: dict, source: dict):
    for request_id in source.keys():
        if request_id in dest:
            dest[request_id]["selected"] += source[request_id]["selected"]
            dest[request_id]["ids"] += source[request_id]["ids"]
        else:
            dest[request_id] = source[request_id]