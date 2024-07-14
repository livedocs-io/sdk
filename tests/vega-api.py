from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


@app.route("/test-set-datasource", methods=["POST", "OPTIONS"])
def set_datasource():
    if request.method == "OPTIONS":
        return jsonify({"message": "OK"}), 200

    data = request.json

    # This goes in the header cell
    from livedocs import Livedocs

    livedocs = Livedocs(
        data["report_id"],
        data["idToken"],
    )

    schema = livedocs._get_table_schema(data["datasource"])
    return jsonify({"schema": schema}), 200


@app.route("/test-run-chart", methods=["POST", "OPTIONS"])
def run_chart():
    if request.method == "OPTIONS":
        return jsonify({"message": "OK"}), 200

    data = request.json

    # This goes in the header cell
    from livedocs import Livedocs

    livedocs = Livedocs(
        data["report_id"],
        data["idToken"],
    )

    chart_config = livedocs.run_chart(data["config"], data["data"])
    return jsonify({"chart_config": chart_config}), 200
