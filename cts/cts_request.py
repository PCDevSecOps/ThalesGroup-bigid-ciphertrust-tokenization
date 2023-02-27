import os
import json

from http.client import HTTPConnection
from typing import Union

from utils.exceptions import CTSException
from utils.utils import json_post_request


class CTSRequest:
    def __init__(self, cts_hostname: str, cts_username: str,
                cts_password: str, cts_certificate_path: str = None):
        self._base_url = "https://" + cts_hostname + "/vts/rest/v2.0/"
        self._cts_username = cts_username
        self._cts_password = cts_password
        self._header = {
		    "user-agent": "mozilla/4.0",
		    "v_content-type": "application/json",
		    "Content-Length": 0
	    }
        self._verify = False
        if cts_certificate_path != '' and os.path.isfile(cts_certificate_path):
            self._verify = cts_certificate_path

        HTTPConnection._http_vsn_str = "HTTP/1.1"

    def _make_request(self, content: str, method: str) -> Union[list, dict]:
        url = self._base_url + method
        self._header["Content-Length"] = str(len(content))
        response = json_post_request(url, self._header, content, proxies=None, verify=self._verify,
            username=self._cts_username, password=self._cts_password)

        if response.status_code != 200:
            raise CTSException("CTS Request failed with status code "
                + f"{response.status_code}: {response.text}")

        return response.json()

    def tokenize(self, values: Union[str, list], tokengroup: str, tokentemplate: str) -> list:

        if values == "" or values is None:
            return [values]
        if values == []:
            return []
        if isinstance(values, str):
            to_tokenize_array = [{"tokengroup": tokengroup, "data": values, "tokentemplate": tokentemplate}]
        else:
            to_tokenize_array = [{"tokengroup": tokengroup, "data": val, "tokentemplate": tokentemplate} for val in values]

        response = self._make_request(to_tokenize_array, "tokenize")
        tokens = []
        if isinstance(response, dict) and "reason" in response:
            raise CTSException(response["reason"])
        for resp, val in zip(response, values):
            if resp["status"] == "error":
                if resp["reason"].startswith("After accounting for keepleft") or val is None:
                    tokens.append(val)
                    continue
            tokens.append(resp["token"])

        return tokens

