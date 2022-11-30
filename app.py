from flask import Flask, request

from app_service import AppService
# from waitress import serve

import json
import argparse

app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    return "Application up and running!"


@app.route("/api/manifest", methods=["GET"])
def manifest():
    with open("Manifest", "r", encoding="utf-8") as manifest_file:
        man = json.load(manifest_file)
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
    try:
        with open("log.txt", "r", encoding="utf-8") as logfile:
            log = logfile.read()
    except FileNotFoundError:
        log = "File log.txt does not exist"
    return log

@app.route("/api/execute", methods=["POST"])
def execute():
    app_service = AppService()
    arguments = request.json

    json_response = {
        "executionId": arguments["executionId"],
    }

    action_name = arguments["actionName"]
    if action_name == "Anonymize":
        try:
            app_service.data_anonymization(arguments)
            json_response["statusEnum"] = "COMPLETED"
            json_response["progress"] = 1
            json_response["message"] = f"Completed action {action_name} successfully"
        except Exception as err:
            json_response["statusEnum"] = "ERROR"
            json_response["progress"] = 0.5
            json_response["message"] = "Error - attempt to execute action "\
                + f"{action_name} failed: {err}"

    else:
        json_response["statusEnum"] = "ERROR"
        json_response["progress"] = 0.5
        json_response["message"] =  f"No such action: {action_name}"

    return json.dumps(json_response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = "Thales <> BigID Flask API for data anonymization"
    )
    parser.add_argument("--host", action = 'store', dest = 'host',
                        default = "0.0.0.0", required = False,
                        help = "API hostname")
    parser.add_argument("--port", action = 'store', dest = 'port',
                        default = "5000", required = False,
                        help = "API port")
    args = parser.parse_args()
    # app.run(host=args.host, port=args.port)  # Uncomment to flask run
    app.run(host="192.168.0.107", port=5000)
    # serve(app, host=args.host, port=args.port)  # Waitress
    # serve(app, host="192.168.0.107", port=5000)