import os
import json

from http.client import HTTPConnection

from utils.exceptions import CTSException
from utils.utils import json_post_request


class CTSRequest:
    def __init__(self, cts_hostname: str, cts_username: str,
                cts_password: str, cts_certificate_path: str, n_retry: int = 3):
        self.base_url = "https://" + cts_hostname + "/vts/rest/v2.0/"
        self.n_retry = n_retry
        self.cts_username = cts_username
        self.cts_password = cts_password
        self.header = {
		    "user-agent": "mozilla/4.0",
		    "v_content-type": "application/json",
		    "Content-Length": 0
	    }
        self.verify = False
        if os.path.exists(cts_certificate_path):
            self.verify = cts_certificate_path

        HTTPConnection._http_vsn_str = "HTTP/1.1"

    def make_request(self, content: str, method: str):
        url = self.base_url + method
        self.header["Content-Length"] = str(len(content))
        response = json_post_request(url, self.header, content, self.verify,
            self.cts_username, self.cts_password)

        if response.status_code != 200:
            raise CTSException("CTS Request failed with status code "
                + f"{response.status_code}: {response.text}")

        return response.json()

    def tokenize(self, values, tokengroup: str, tokentemplate: str):
        base = '{{"tokengroup": "{}", "data": "{}", "tokentemplate": "{}"}}'
        if isinstance(values, list):
            if len(values) == 0:
                return values
            content = [base.format(tokengroup, val_i, tokentemplate) for val_i in values]
            content = "[" + ",".join(content) + "]"
        else:
            if len(values) == 0:
                return [""]
            content = "[" + base.format(tokengroup, values, tokentemplate) + "]"

        response = self.make_request(json.loads(content), "tokenize")
        # print(response)
        tokens = []
        for resp, val in zip(response, values):
            if resp["status"] == "error":
                if resp["reason"].startswith("After accounting for keepleft"):
                    tokens.append(val)
                    continue
                else:
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

