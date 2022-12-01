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
        base = '{{"tokengroup": "{}", "data": "{}", "tokentemplate": "{}"}}'
        if values is None:
            return None
        elif isinstance(values, list):
            if len(values) == 0:
                return values
            content = [base.format(tokengroup, val_i, tokentemplate) for val_i in values]
            content = "[" + ",".join(content) + "]"
        else:
            if len(values) == 0:
                return [""]
            content = "[" + base.format(tokengroup, values, tokentemplate) + "]"

        response = self._make_request(json.loads(content), "tokenize")
        tokens = []
        for resp, val in zip(response, values):
            if resp["status"] == "error":
                if resp["reason"].startswith("After accounting for keepleft") or val is None:
                    tokens.append(val)
                    continue
                raise CTSException(resp["reason"])
            tokens.append(resp["token"])

        return tokens



def main():
    req = CTSRequest("cts", "test", "Thales123!", "cts.pem")
    print(req.tokenize(["testeval1234121", "12", "123876n182", "123onxasbb"], "tokenization_group", "alphanum"))
    # print(req.tokenize("testeval1234121", "tokenization_group", "alphanum"))
    # print(req.tokenize("", "tokenization_group", "alphanum"))


if __name__ == "__main__":
    main()

