from flask import Blueprint, jsonify
from Database.SnowflakeConnection import SnowflakeConnection

sales = Blueprint("sales", __name__)







@sales.route("/sales", defaults={"filter_today": False})
@sales.route("/sales/today", defaults={"filter_today": True})
def get_sales(filter_today):
    connection = SnowflakeConnection.get_instance()
    cursor = connection.cursor()

    try:
    
        ticket_query = "SELECT COUNT(*) FROM PUBLIC.Tickets"
        attraction_query = """
            SELECT A.NAME, COUNT(*)
            FROM PUBLIC.AttractionVisits AV
            JOIN PUBLIC.Attractions A ON AV.attraction_id = A.ATTRACTION_ID
            GROUP BY A.NAME
        """
        restaurant_query = """
            SELECT R.restaurant_name, M.dish_name, SUM(P.quantity) AS total_quantity, SUM(P.total_price) AS total_sales
            FROM PUBLIC.Purchases P
            JOIN PUBLIC.Restaurants R ON P.restaurant_id = R.restaurant_id
            JOIN PUBLIC.Menu M ON P.dish_id = M.dish_id
        """
        if filter_today:
            ticket_query += " WHERE CAST(VALID_DATE AS DATE) = CURRENT_DATE"
            restaurant_query += " WHERE CAST(P.purchase_date AS DATE) = CURRENT_DATE"

        restaurant_query += " GROUP BY R.restaurant_name, M.dish_name ORDER BY R.restaurant_name"

        cursor.execute(ticket_query)
        visitors_today = cursor.fetchone()[0]

        cursor.execute(attraction_query)
        attractions_today = cursor.fetchall()

        cursor.execute(restaurant_query)
        restaurant_sales = cursor.fetchall()

        all_visitors_data = [{
            "visitors": visitors_today,
            "attractions": attractions_today,
            "restaurants": restaurant_sales
        }]
        
        return jsonify(all_visitors_data), 200

    except Exception as e:
        return jsonify({"error": "Failed to fetch data", "details": str(e)}), 500

    finally:
        cursor.close()
       
@sales.route("/sales/tickets/<int:TICKET_ID>")
def get_sales_ticket(TICKET_ID):
    if row := SnowflakeConnection.execute_query(
        "SELECT * FROM PUBLIC.Tickets T "
        "JOIN VISITORS V ON T.VISITOR_ID = V.VISITOR_ID "
        "WHERE T.TICKET_ID = %s",
        (TICKET_ID,), 
        single=True
    ):
        return jsonify({
            "ticket_id": row[0],
            "visitor_id": row[1],               
            "purchase_date": row[4], 
            "price": row[3],
            "visitor": {
                "first_name": row[6],
                "last_name": row[7], 
            }
        }), 200
    return jsonify({"error": "Ticket not found"}), 404

   

@sales.route("/sales/attraction/<int:ATTRACTION_ID>")
def get_sales_attraction(ATTRACTION_ID):
    if row := SnowflakeConnection.execute_query(
       """
            SELECT A.NAME, COUNT(*) 
            FROM PUBLIC.AttractionVisits AV
            JOIN PUBLIC.Attractions A ON AV.attraction_id = A.ATTRACTION_ID
            WHERE  A.ATTRACTION_ID = %s
            GROUP BY A.NAME;
        """, (ATTRACTION_ID,),single=True):
             return jsonify({
                "NAME": row[0], 
                "broi prodajbi": row[1] 
             }), 200
    return jsonify({"error": "No sales data found for this attraction"}), 404

  
@sales.route("/sales/<int:RESTAURANT_ID>")
def get_sales_restaurant(RESTAURANT_ID):
    if row := SnowflakeConnection.execute_query("""
            SELECT R.restaurant_name, M.dish_name, SUM(P.quantity) AS total_quantity, SUM(P.total_price) AS total_sales
            FROM PUBLIC.Purchases P
            JOIN PUBLIC.Restaurants R ON P.restaurant_id = R.restaurant_id
            JOIN PUBLIC.Menu M ON P.dish_id = M.dish_id
            WHERE R.restaurant_id = %s
            GROUP BY R.restaurant_name, M.dish_name
            ORDER BY R.restaurant_name;
        """, (RESTAURANT_ID,),single=True):

                    return jsonify({
                    "restaurant_name": row[0],  
                    "dish_name": row[1],         
                    "total_quantity": row[2],   
                    "total_sales": row[3]      
                }),200
    return jsonify({"error": "No sales data found for this attraction"}), 404

@sales.route("/tickets")
def get_tickets():
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