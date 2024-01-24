from bigid.bigid import BigIDAPI
import utils.utils as ut
import app_modules.remediation as remed

config = ut.read_config_file("config.ini")
bigid = BigIDAPI(config, "https://192.168.82.125/api/v1/")
#remed.tag_column_thales_tokenized(bigid, "tokendb", "cpf", "tokendb.TOKEN_USER.CUSTOMER_TRANSACTIONS")
bigid.create_main_tag("tag2", "description_2")