import time

from configparser import RawConfigParser
from typing import Union

import utils.utils as ut
from utils.log import Log
from utils.exceptions import BigIDAPIException
from databases.ds_connection import DataSourceConnection


class BigIDAPI:
    def __init__(self, config: RawConfigParser, base_url: str):
        self._config     = config
        self._user_token = ut.get_bigid_user_token(self._config["BigID"]["user_token_path"])
        self._base_url    = base_url
        self._access_token_h_duration = 23     # Access token duration in hours
        self._proxies = ut.get_proxy_from_config(self._config)

        self._access_token_time        = None
        self._access_token             = None
        #self._minimization_requests    = []

        self._update_session_token()

    def _update_session_token(self):
        token_url = f"{self._base_url}refresh-access-token"
        headers = {
            "Accept": "application/json",
            "Authorization": self._user_token
        }
        get_response = ut.json_get_request(token_url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error(f"BigID session token HTTP {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID access token request failed "
                + f"with status code {get_response.status_code}: {get_response.text}")

        self._access_token = get_response.json()["systemToken"]
        self._access_token_time = time.time()
        Log.info("BigID session token updated")

    def validate_session_token(self):
        if time.time() - self._access_token_time > self._access_token_h_duration * 3600:
            self._update_session_token()

    def update_minimization_requests(self, offset: int, fetch_next: int) -> dict:
        self.validate_session_token()
        url = f'{self._base_url}data-minimization/objects?skip={offset}&limit={fetch_next}' +\
            '&requireTotalCount=true&sort=[{"field":"name","order":"asc"}]' +\
            '&filter=[{"field":"state","value":["Pending"],"operator":"in"},' +\
                    '{"field":"markedAs","value":["Delete Manually"],"operator":"in"}]'
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token
        }
        get_response = ut.json_get_request(url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error("BigID minimization request failed with status code "
                + f"{get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID minimization request failed with"
                + f" status code {get_response.status_code}: {get_response.text}")

        get_response = get_response.json()
        min_requests = {}
        # each request = {"<requestId>": {"selected": [fobjn1, fobjn2, ...], "ids": [id1, id2, ...]}}
        for req in get_response["data"]["deleteQueries"]:

            request_id = req["requestId"]
            full_obj_name = req["fullObjectName"]
            obj_id = req["_id"]
            if request_id in min_requests:
                min_requests[request_id]["selected"].append(full_obj_name)
                min_requests[request_id]["ids"].append(obj_id)
            else:
                min_requests[request_id] = {"selected": [full_obj_name]}
                min_requests[request_id]["ids"] = [obj_id]

        #self._minimization_requests = min_requests
        Log.info(f"Got {len(min_requests)} minimization requests from BigID")
        return min_requests

    #def get_minimization_requests(self) -> list:
    #    return self._minimization_requests

    def get_sar_report(self, request_id: str) -> list:
        self.validate_session_token()

        sar_url = f"{self._base_url}sar/reports/{request_id}"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token
        }
        get_response = ut.json_get_request(sar_url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error("BigID sar report failed with status code "
                + f"{get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID sar report failed with status "
                + f"code {get_response.status_code}: {get_response.text}")

        get_response = get_response.json()
        Log.info(f"Got sar report from BigID for {request_id=}")
        return get_response["records"]

    def get_data_source_conn_from_source_name(self, data_source_name: str) -> DataSourceConnection:
        self.validate_session_token()
        url = f"{self._base_url}ds_connections/{data_source_name}"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token
        }
        get_response = ut.json_get_request(url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error("BigID data source request failed with status code"
                + f" {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID data source request failed with"
                + f" status code {get_response.status_code}: {get_response.text}")

        get_response = get_response.json()
        rdb_url = get_response["ds_connection"]["rdb_url"]
        rdb_name = get_response["ds_connection"]["rdb_name"]
        conn_type = get_response["ds_connection"]["type"]
        return DataSourceConnection(rdb_url, conn_type, rdb_name)

    def get_all_data_sources(self, enabled: Union[bool, None] = None) -> list:
        """
        Returns all data sources available. If enabled is None, all are returned.
        If enabled is False, returns all disabled. If enabled is True, returns
        all enabled.
        """
        self.validate_session_token()
        url = f"{self._base_url}ds_connections"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token
        }
        get_response = ut.json_get_request(url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error("BigID data source list request failed with "
                + f"status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID data source list request failed"
                + f" with status code {get_response.status_code}: {get_response.text}")

        get_response = get_response.json()["ds_connections"]
        if enabled is None:
            return get_response
        elif enabled:
            return list(filter(lambda x: x["enabled"] == "yes", get_response))
        else:
            return list(filter(lambda x: x["enabled"] == "no", get_response))

    def get_data_sources_policy_hit(self) -> list:
        self.validate_session_token()
        url = f"{self._base_url}proxy/tpa/api/{self._config['BigID']['remediation_id']}/datasource/auditor-datasource"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token,
            "Accept-version": "v1"
        }
        get_response = ut.json_get_request(url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error("BigID policy hit data source list request failed with "
                + f"status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID policy hit data source list request failed"
                + f" with status code {get_response.status_code}: {get_response.text}")

        get_response = get_response.json()["results"]
        return list(filter(lambda x: len(x["policyHit"]) > 0, get_response))

    def get_all_remediation_objects(self) -> list:
        self.validate_session_token()
        url = f"{self._base_url}proxy/tpa/api/{self._config['BigID']['remediation_id']}/object"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token,
            "Accept-version": "v1"
        }
        get_response = ut.json_get_request(url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error("BigID policy hit data source list request failed with "
                + f"status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID policy hit data source list request failed"
                + f" with status code {get_response.status_code}: {get_response.text}")

        return get_response.json()["results"]

    def get_remediation_objects_by_source(self, source_name: str) -> list:
        """
        Gets remediation objects as they are seen when you click 
        """
        self.validate_session_token()

        if self._base_url.endswith("/api/v1/"):
            url = self._base_url[:-7]
        else:
            url = self._base_url

        url = f"{url}proxy/tpa/api/{self._config['BigID']['remediation_id']}/object"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token,
            "Accept-version": "v1",
            "filterV2": f'[{{"value": ["{source_name}"], "field": "source", "operator": "in"}}]'
        }
        get_response = ut.json_get_request(url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error("BigID remediation objects request failed with "
                + f"status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID remediation objects request failed"
                + f" with status code {get_response.status_code}: {get_response.text}")

        return get_response.json()["results"]

    def get_remediation_objects_by_source_columns(self, source_name: str) -> list:
        """
        Gets remediation objects as they are seen when you click columns option
        Information and IDs are differente from the method above, thats why
        I'm doing both requests and joining the results with full qualified name
        """
        self.validate_session_token()

        if self._base_url.endswith("/api/v1/"):
            url = self._base_url[:-7]
        else:
            url = self._base_url

        url = f"{url}proxy/tpa/api/{self._config['BigID']['remediation_id']}/object/columns-view?source={source_name}"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token,
            "Accept-version": "v1"
        }
        get_response = ut.json_get_request(url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error("BigID remediation objects col request failed with "
                + f"status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID remediation objects col request failed"
                + f" with status code {get_response.status_code}: {get_response.text}")

        return get_response.json()["results"]

    def get_object_comments(self, obj_id: str) -> list:
        self.validate_session_token()

        if self._base_url.endswith("/api/v1/"):
            url = self._base_url[:-7]
        else:
            url = self._base_url

        url = f"{url}proxy/tpa/api/{self._config['BigID']['remediation_id']}/object/{obj_id}/comment"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token,
            "Accept-version": "v1"
        }
        get_response = ut.json_get_request(url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error("BigID object comments request failed with "
                + f"status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID object comments request failed"
                + f" with status code {get_response.status_code}: {get_response.text}")

        return get_response.json()

    def get_bigid_tags(self) -> list:
            """
            Get all tags stored in BigID
            """
            self.validate_session_token()

            url = f"{self._base_url}data-catalog/tags/all-pairs"
            headers = {
                "Accept": "application/json",
                "Authorization": self._access_token
            }
            get_response = ut.json_get_request(url, headers, self._proxies)
            tags = get_response.json()["data"]

            if get_response.status_code != 200:
                Log.error("BigID object tags request failed with "
                    + f"status code {get_response.status_code}: {get_response.text}")
                raise BigIDAPIException("BigID object tags request failed"
                    + f" with status code {get_response.status_code}: {get_response.text}")

            return tags


    def get_object_tags(self, object_name: str) -> list:
        """
        Object name is the fully qualified name
        """
        self.validate_session_token()

        if self._base_url.endswith("/api/v1/"):
            url = self._base_url[:-7]
        else:
            url = self._base_url

        url = f"{url}proxy/tpa/api/{self._config['BigID']['remediation_id']}/object/object-detail?object_name={object_name}"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token,
            "Accept-version": "v1"
        }
        get_response = ut.json_get_request(url, headers, self._proxies)
        tags = get_response.json()["basicDetails"]["tags"]

        if get_response.status_code != 200:
            Log.error("BigID object tags request failed with "
                + f"status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID object tags request failed"
                + f" with status code {get_response.status_code}: {get_response.text}")

        return tags



    def create_main_tag(self, tag_name: str, tag_description: str = "") -> str:
        """
        Make sure the tag does not exist before running. Will return the new tag's
        _id property.
        """
        self.validate_session_token()

        if self._base_url.endswith("/api/v1/"):
            url = self._base_url[:-7]
        else:
            url = self._base_url

        url = f"{url}proxy/tpa/api/{self._config['BigID']['remediation_id']}/object/tags/create-tag"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token,
            "Accept-version": "v1"
        }
        content = {
            "name": tag_name,
            "type": "TAG",
            "description": tag_description
        }
        post_response = ut.json_post_request(url, headers, content, self._proxies)

        if post_response.status_code != 200:
            Log.info(post_response.text)
            Log.error("BigID create main tag request failed with "
                + f"status code {post_response.status_code}")
            raise BigIDAPIException("BigID create main tag request failed"
                + f" with status code {post_response.status_code}")

        post_response = post_response.json()
        return post_response["_id"]

    def create_sub_tag(self, subtag_name: str, parent_id: str,
            subtag_description: str = "") -> tuple:
        """
        Make sure the tag does not exist before running. Will return the new subtag's
        _id property and the parent id.
        """
        self.validate_session_token()

        if self._base_url.endswith("/api/v1/"):
            url = self._base_url[:-7]
        else:
            url = self._base_url

        url = f"{url}proxy/tpa/api/{self._config['BigID']['remediation_id']}/object/tags/create-tag"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token,
            "Accept-version": "v1"
        }
        content = {
            "name": subtag_name,
            "type": "VALUE",
            "description": subtag_description,
            "parentId": parent_id
        }
        post_response = ut.json_post_request(url, headers, content, self._proxies)

        if post_response.status_code != 200:
            Log.error("BigID create subtag request failed with "
                + f"status code {post_response.status_code}")
            raise BigIDAPIException("BigID create subtag request failed"
                + f" with status code {post_response.status_code}")

        post_response = post_response.json()
        return (post_response["_id"], post_response["parent_id"])

    def add_tag(self, fully_qual_name: str, source_name: str, tag_id: str,
            value_id: str) -> str:
        """
        tag_id is the parent id returned by the create_subtag method
        value_id is the _id returned by create_main_tag
        """
        self.validate_session_token()

        if self._base_url.endswith("/api/v1/"):
            url = self._base_url[:-7]
        else:
            url = self._base_url

        url = f"{url}proxy/tpa/api/{self._config['BigID']['remediation_id']}/object/tags/add-tags"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token,
            "Accept-version": "v1"
        }
        content = {
            "data": [
                {
                    "type": "OBJECT",
                    "fullyQualifiedName": fully_qual_name,
                    "source": source_name,
                    "tags": [
                        {
                            "tagId": tag_id,
                            "valueId": value_id
                        }
                    ]
                }
            ]
        }
        post_response = ut.json_post_request(url, headers, content, self._proxies)

        if post_response.status_code != 200:
            Log.error("BigID add tags request failed with "
                + f"status code {post_response.status_code}")
            raise BigIDAPIException("BigID add tags request failed"
                + f" with status code {post_response.status_code}")

    def add_comment(self, comment: str, annotation_id: str):
        self.validate_session_token()

        if self._base_url.endswith("/api/v1/"):
            url = self._base_url[:-7]
        else:
            url = self._base_url

        url = f"{url}proxy/tpa/api/{self._config['BigID']['remediation_id']}/object/{annotation_id}/comment"

        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token,
            "Accept-version": "v1"
        }
        content = {
            "comment": comment
        }
        post_response = ut.json_post_request(url, headers, content, self._proxies)

        if post_response.status_code != 200:
            Log.error("BigID submit comment request failed with "
                + f"status code {post_response.status_code}")
            raise BigIDAPIException("BigID submit comment request failed"
                + f" with status code {post_response.status_code}")

    def get_data_source_credentials(self, tpa_id: str, data_source_name: str) -> dict:
        self.validate_session_token()
        url = f"{self._base_url}tpa/{tpa_id}/credentials/{data_source_name}"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token
        }
        get_response = ut.json_get_request(url, headers, self._proxies)

        if get_response.status_code != 200:
            Log.error("BigID data source credentials request failed with "
                + f"status code {get_response.status_code}: {get_response.text}")
            raise BigIDAPIException("BigID data source credentials request failed"
                + f" with status code {get_response.status_code}: {get_response.text}")

        get_response = get_response.json()
        return get_response
    
    def set_minimization_request_action(self, request_id: str, action_type: str,
            secondary_ids: Union[str, list] = None):
        self.validate_session_token()
        url = f"{self._base_url}data-minimization/objects/action"
        headers = {
            "Accept": "application/json",
            "Authorization": self._access_token
        }
        content = {
            "query": {
                "filter": [
                    {
                        "field": "requestId",
                        "operator": "equal",
                        "value": request_id
                    }
                ]
            },
            "actionType": action_type,
            "reason": "Thales BigID Anonymization API"
        }

        if isinstance(secondary_ids, str):
            secondary_ids = [secondary_ids]

        if secondary_ids:
            content["query"]["filter"].append({
                "field": "_id",
                "operator": "in",
                "value": secondary_ids
            })

        post_response = ut.json_post_request(url, headers, content, self._proxies).json()

        if post_response["statusCode"] != 200:
            Log.error("BigID minimization action request failed with "
                + f"status code {post_response['statusCode']}: {post_response['message']}")
            raise BigIDAPIException("BigID minimization action request failed"
                + f" with status code {post_response['statusCode']}: {post_response['message']}")

