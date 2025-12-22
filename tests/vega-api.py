import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app, origins=["*"])

os.environ["FLASK_DEBUG"] = "1"
os.environ["LIVEDOCS_FILES_PATH"] = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "livedocs_files"
)


@app.route("/test-set-datasource", methods=["POST", "OPTIONS"])
def set_datasource():
    if request.method == "OPTIONS":
        return jsonify({"message": "OK"}), 200

    data = request.json

    # This goes in the header cell
    from livedocs import Livedocs

    livedocs = Livedocs()
    livedocs.initialize(data["report_id"], data["idToken"])

    schema = livedocs._get_chart_schema(json.dumps(data["datasource"]))

    return jsonify({"schema": schema}), 200


@app.route("/test-run-chart", methods=["POST", "OPTIONS"])
def run_chart():
    if request.method == "OPTIONS":
        return jsonify({"message": "OK"}), 200

    data = request.json

    # This goes in the header cell
    from livedocs import Livedocs

    livedocs = Livedocs()
    livedocs.initialize(data["report_id"], data["idToken"])

    chart_config = livedocs._get_vega_spec(
        json.dumps(data["settings"]), json.dumps(data["datasource"])
    )

    return json.dumps(chart_config), 200, {"Content-Type": "application/json"}
