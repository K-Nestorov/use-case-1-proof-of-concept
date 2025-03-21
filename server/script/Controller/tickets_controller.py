from flask import Blueprint, jsonify, request
from Database.SnowflakeConnection import SnowflakeConnection

tickets = Blueprint("tickets", __name__)

# Route to retrieve all tickets (GET)
@tickets.route("/tickets", methods=["GET"])
def get_tickets():
    try:
        rows = SnowflakeConnection.execute_query("SELECT * FROM TICKETS") 

        if rows:
            return jsonify([
                { 
                    "TICKET_ID": row[0],
                    "VISITOR_ID": row[1],
                    "PURCHASE_DATE": row[2],
                    "TICKET_PRICE": row[3],
                    "VALID_DATE": row[4]
                } for row in rows
            ]), 200

        return jsonify({"error": "No tickets found"}), 404

    except Exception as e:
        print(f"Error fetching tickets: {e}")
        return jsonify({"error": "Failed to fetch tickets"}), 500

# Route to create a new ticket (POST)
@tickets.route("/tickets", methods=["POST"])
def create_ticket():
    try:
        # Get the data from query parameters (not JSON)
        visitor_id = request.args.get("visitor_id")
        purchase_date = request.args.get("purchase_date")
        ticket_price = request.args.get("ticket_price")
        valid_date = request.args.get("valid_date")

        # Insert into Snowflake
        connection = SnowflakeConnection.get_instance()
        cursor = connection.cursor()

        cursor.execute("""
            INSERT INTO TICKETS (VISITOR_ID, PURCHASE_DATE, TICKET_PRICE, VALID_DATE)
            VALUES (%s, %s, %s, %s)
        """, (visitor_id, purchase_date, ticket_price, valid_date))

        connection.commit()

        return jsonify({
            "ticket": {
                "visitor_id": visitor_id,
                "purchase_date": purchase_date,
                "ticket_price": ticket_price,
                "valid_date": valid_date
            }
        }), 201

    except Exception as e:
        print(f"Error creating ticket: {e}")
        return jsonify({"error": "Failed to create ticket"}), 500
