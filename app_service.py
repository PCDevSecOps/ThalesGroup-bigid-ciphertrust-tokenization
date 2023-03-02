import utils.utils as ut

from bigid.bigid import BigIDAPI
from cts.cts_request import CTSRequest
from app_modules import anonymization, remediation
from utils.log import Log


class AppService:

    def __init__(self):
        self.config = ut.read_config_file("config.ini")

        # User token not used yet
        self.bigid_user_token = ut.get_bigid_user_token(self.config["BigID"]["user_token_path"])
        Log.info("AppService Initialized")
    
    def initialize_from_post_params(self, arguments: dict):
        self.tpa_id = arguments["tpaId"]
        self.bigid = BigIDAPI(self.config, arguments["bigidBaseUrl"])

        action_params = arguments["actionParams"]
        self.params = {i["paramName"]: i["paramValue"] for i in action_params}
        self.validate_params()
        cts_hostname = self.config["CTS"]["hostname"]
        cts_cert_path = self.config["CTS"]["certificate"]
        self.cts = CTSRequest(cts_hostname, self.params["CTSUsername"], self.params["CTSPassword"],
            cts_cert_path)
        Log.info("CTSRequest initialized")

    def validate_params(self):
        if "BatchSize" in self.params:
            batch_size = int(self.params["BatchSize"])
            if batch_size <= 0:
                Log.error("Batchsize menor que 0.")
                raise ValueError("BatchSize menor que 0.")
            self.params["BatchSize"] = batch_size


    def data_anonymization(self):
        anonymization.run_data_anonymization(self.config, self.params, self.tpa_id, self.cts, self.bigid)
    
    def data_remediation(self):
        remediation.run_data_remediation(self.cts, self.bigid, self.config, self.params, self.tpa_id)