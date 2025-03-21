from flask import Blueprint, jsonify, request
from Database.SnowflakeConnection import SnowflakeConnection

visitors = Blueprint("visitors", __name__)

@visitors.route("/visitor")
def get_visitor():
    connection = SnowflakeConnection.get_instance() 
    cursor = connection.cursor()

    first_name = request.args.get("first_name")  
    last_name = request.args.get("last_name")  
    
    try:
        query = "SELECT * FROM Public.VISITORS"
        params = []
        
        conditions = []
        if first_name:
            conditions.append("FIRST_NAME ILIKE %s")
            params.append(f"%{first_name}%")
        
        if last_name:
            conditions.append("LAST_NAME ILIKE %s")
            params.append(f"%{last_name}%")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        cursor.execute(query, tuple(params))
        visitors_data = cursor.fetchall()

        if visitors_data:
            return jsonify([
                {
                    "VISITOR_ID": visitor[0],  
                    "FIRST_NAME": visitor[1], 
                    "LAST_NAME": visitor[2],
                    "REGISTRATION_DATE": visitor[3],
                    "ACCESS_CARD_NUMBER": visitor[4],
                } for visitor in visitors_data
            ]), 200
        
        return jsonify({"error": "No visitors found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch visitors data"}), 500
    finally:
        cursor.close()
       
@visitors.route("/visitor/<int:VISITOR_ID>")
def get_visitor_by_id(VISITOR_ID):
    connection = SnowflakeConnection.get_instance() 
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT * FROM Public.VISITORS WHERE VISITOR_ID = %s", (VISITOR_ID,))
        row = cursor.fetchone()  

        if row:
            return jsonify({
                "VISITOR_ID": row[0],  
                "FIRST_NAME": row[1], 
                "LAST_NAME": row[2],
                "REGISTRATION_DATE": row[3],
                "ACCESS_CARD_NUMBER": row[4],
            }), 200
        
        return jsonify({"error": "Visitor not found"}), 404

    except Exception as e:
        return jsonify({"error": "Failed to fetch visitor data"}), 500
    finally:
        cursor.close()
