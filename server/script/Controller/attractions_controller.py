from flask import Blueprint, jsonify
from Database.SnowflakeConnection import SnowflakeConnection

attractions = Blueprint("attractions", __name__)

@attractions.route("/attractions", methods=["GET"])
def get_attractions():

    attractions = SnowflakeConnection.execute_query("SELECT * FROM Public.ATTRACTIONS")

    if not attractions:
        return jsonify({"error": "No attractions found"}), 404

    return jsonify([
        {
            "ATTRACTION_ID": row[0],
            "NAME": row[1],
            "TYPE": row[2],
            "CAPACITY": row[3],
            "LOCATION": row[4],
        } for row in attractions
    ]), 200

@attractions.route("/attractions/<int:ATTRACTION_ID>", methods=["GET"])
def get_attraction_by_id(ATTRACTION_ID):
    row = SnowflakeConnection.execute_query(
        "SELECT * FROM Public.ATTRACTIONS WHERE ATTRACTION_ID = %s",
        (ATTRACTION_ID,),  # Now passing parameters correctly
        single=True  # Fetch only one row
    )

    if not row:
        return jsonify({"error": "No attraction found"}), 404

    return jsonify({
        "ATTRACTION_ID": row[0],
        "NAME": row[1],
        "TYPE": row[2],
        "CAPACITY": row[3],
        "LOCATION": row[4],
    }), 200

