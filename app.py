from flask import Flask, request

from app_service import AppService
from waitress import serve

import json
import argparse

app = Flask(__name__)


@app.route("/api/manifest", methods=["GET"])
def manifest():
    with open("Manifest", "r") as f:
        man = json.load(f)
    return man


@app.route("/api/assets/icon", methods=["GET"])
def get_icon():
    with open("assets/thales-icon.png", "rb") as f:
        icon = f.read()
    return icon


@app.route("/api/assets/sideBarIcon", methods=["GET"])
def get_sidebar_icon():
    with open("assets/thales-sidebar-icon.png", "rb") as f:
        sidebar_icon = f.read()
    return sidebar_icon


@app.route("/api/logs", methods=["GET"])
def logs():
    with open("log.txt", "r") as f:
        log = f.read()
    return log


@app.route("/api/execute/helloWorld", methods=["POST"])
def hello_world():
    print(request.args)
    print(request.form)
    print(request.json)
    return "Hello, World!"


@app.route("/api/execute", methods=["POST"])
def execute():
    app_service = AppService()
    args = request.json

    json_response = {
        "executionId": args["executionId"],
    }

    action_name = args["actionName"]
    if action_name == "Anonimize":
        try:
            app_service.data_anonimization(args)
            json_response["statusEnum"] = "COMPLETED"
            json_response["progress"] = 1
            json_response["message"] = f"Completed action {action_name} successfully"
        except Exception as e:
            json_response["statusEnum"] = "ERROR"
            json_response["progress"] = 0.5
            json_response["message"] = f"Error - attempt to execute action {action_name} failed: {e}"

    else:
        json_response["statusEnum"] = "ERROR"
        json_response["progress"] = 0.5
        json_response["message"] =  f"No such action: {action_name}"
    
    return json.dumps(json_response)



@app.route("/")
def home():
    raise Exception("Test Exception")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Thales <> BigID Flask API for data anonimization")
    parser.add_argument("--host", action = 'store', dest = 'host',
                        default = "0.0.0.0", required = False,
                        help = "API hostname")
    parser.add_argument("--port", action = 'store', dest = 'port',
                        default = "5000", required = False,
                        help = "API port")
    # app.run(host="0.0.0.0")
    parser.parse_args()
    serve(app, host="0.0.0.0", port=5000)