from flask import Flask, request, jsonify
import snowflake.connector as sf
import sys
from datetime import datetime
from dotenv import load_dotenv
import os
app = Flask(__name__)

load_dotenv()
def get_connection():
    """Establish a connection to the Snowflake LUNAPARK database."""
    try:
        connection = sf.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            account=os.getenv("DB_ACCOUNT"),
            warehouse=os.getenv("DB_WAREHOUSE"),
            database=os.getenv("DB_DATABASE")
        )
        return connection
    except Exception as e:
        print(f"Error: Unable to connect to Snowflake - {e}")
        sys.exit()
connection = get_connection()

@app.route("/attractions")
def get_attractions():
    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute("""SELECT * FROM Public.ATTRACTIONS""")
        attraction_data = cursor.fetchall()

        if attraction_data:
            all_attractions_data = []
            for attraction in attraction_data:
                attraction_info = {
                    "ATTRACTION_ID": attraction[0],
                    "NAME": attraction[1],
                    "TYPE": attraction[2],
                    "CAPACITY": attraction[3],
                    "LOCATION": attraction[4]
                }
                all_attractions_data.append(attraction_info)

            return jsonify(all_attractions_data), 200
        else:
            return jsonify({"error": "No attractions found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch attractions data"}), 500
    finally:
        cursor.close()
        connection.close()

@app.route("/attractions/<int:ATTRACTION_ID>")
def get_attractions_byid(ATTRACTION_ID):
    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute("""SELECT * FROM Public.ATTRACTIONS WHERE ATTRACTION_ID = %s""", (ATTRACTION_ID,))
        attraction_data = cursor.fetchone() 

        if attraction_data:
            attraction_info = {
                "ATTRACTION_ID": attraction_data[0],
                "NAME": attraction_data[1],
                "TYPE": attraction_data[2],
                "CAPACITY": attraction_data[3],
                "LOCATION": attraction_data[4]
            }

            return jsonify(attraction_info), 200
        else:
            return jsonify({"error": "No attraction found with the given ID"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch attractions data"}), 500
    finally:
        cursor.close()
        connection.close()

@app.route("/employees")
def get_employees():
    connection = get_connection() 
    cursor = connection.cursor()
    try:
        cursor.execute("""SELECT * FROM Public.Employees""")
        employees_data = cursor.fetchall() 

        if employees_data:
           
            all_employees_data = []

            for employees in employees_data:
                employees = {
                    "EMPLOYEE_ID": employees[0], 
                    "FIRST_NAME": employees[1],  
                    "LAST_NAME": employees[2],
                    "PHONE_NUMBER": employees[3],
                    "HIRE_DATE": employees[4],
                    "JOB_TITLE": employees[5],
                    "SALARY": employees[6],
                    "DEPARTMENT": employees[7]
                }
                all_employees_data.append(employees)

            return jsonify(all_employees_data), 200
        else:
            return jsonify({"error": "No Employees found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch employees data"}), 500
    finally:
        cursor.close()
        connection.close()
@app.route("/employees/<int:employee_id>")
def get_employee_by_id(employee_id):
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT * FROM Public.Employees WHERE EMPLOYEE_ID = %s
        """, (employee_id,))
        employee_data = cursor.fetchone()

        if employee_data:
            employee = {
                "EMPLOYEE_ID": employee_data[0],
                "FIRST_NAME": employee_data[1],
                "LAST_NAME": employee_data[2],
                "PHONE_NUMBER": employee_data[3],
                "HIRE_DATE": employee_data[4],
                "JOB_TITLE": employee_data[5],
                "SALARY": employee_data[6],
                "DEPARTMENT": employee_data[7]
            }
            return jsonify(employee), 200
        else:
            return jsonify({"error": f"Employee with ID {employee_id} not found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch employee data"}), 500
    finally:
        cursor.close()
        connection.close()

@app.route("/sales")
def get_sales():
    connection = get_connection()
    cursor = connection.cursor()
#maham WHERE TO_DATE(VALID_DATE) = CURRENT_DATE; sled tva otmetka za today 
    try:
        cursor.execute("""
                SELECT COUNT(*) FROM PUBLIC.Tickets 
                
                """)
        visitors_today = cursor.fetchone()[0]
        cursor.execute("""
                SELECT A.NAME, COUNT(*) 
                FROM PUBLIC.AttractionVisits AV
                JOIN PUBLIC.Attractions A ON AV.attraction_id = A.ATTRACTION_ID
                
                GROUP BY A.NAME;
                """)
        attractions_today = cursor.fetchall()
        cursor.execute("""
                SELECT R.restaurant_name, M.dish_name, SUM(P.quantity) AS total_quantity, SUM(P.total_price) AS total_sales
                FROM PUBLIC.Purchases P
                JOIN PUBLIC.Restaurants R ON P.restaurant_id = R.restaurant_id
                JOIN PUBLIC.Menu M ON P.dish_id = M.dish_id
              
                GROUP BY R.restaurant_name, M.dish_name
                ORDER BY R.restaurant_name;
                """)
        restaurant_sales = cursor.fetchall()

        if visitors_today:
            all_visitors_data = []

            visitors_info = {
                "visitors today": visitors_today,
                "attractions": attractions_today,
                "restorans": restaurant_sales
            }
            all_visitors_data.append(visitors_info)

            return jsonify(all_visitors_data), 200
        else:
            return jsonify({"error": "No visitors found"}), 404

    except Exception as e:

        return jsonify({"error": "Failed to fetch  data"}), 500
    finally:
        cursor.close()
        connection.close()
@app.route("/sales/tickets/<int:TICKET_ID>")
def get_sales_ticket(TICKET_ID):
    connection = get_connection()
    cursor = connection.cursor()

    try:
       
        cursor.execute("""
SELECT *
   
    FROM PUBLIC.Tickets T
    JOIN VISITORS V ON T.VISITOR_ID = V.VISITOR_ID
    WHERE T.TICKET_ID = %s
""", (TICKET_ID,))


        ticket_info = cursor.fetchone()

        if ticket_info:
         
            ticket_details = {
                "ticket_id": ticket_info[0],
                "visitor_id": ticket_info[1],
               
                "purchase_date": ticket_info[3], 
                "price": ticket_info[4],
                "visitor": {
                    "visitor_id": ticket_info[5],
                    "first_name": ticket_info[6],
                    "last_name": ticket_info[7],
                  
                }
            }

            return jsonify(ticket_details), 200
        else:
            return jsonify({"error": "Ticket not found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch ticket data"}), 500
    finally:
        cursor.close()
        connection.close()



@app.route("/sales/attraction/<int:ATTRACTION_ID>")
def get_sales_attraction(ATTRACTION_ID):
    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute("""
            SELECT A.NAME, COUNT(*) 
            FROM PUBLIC.AttractionVisits AV
            JOIN PUBLIC.Attractions A ON AV.attraction_id = A.ATTRACTION_ID
            WHERE  A.ATTRACTION_ID = %s
            GROUP BY A.NAME;
        """, (ATTRACTION_ID,))
        
        attractions_today = cursor.fetchall()

        if attractions_today:
          
            attraction_info = {
                "NAME": attractions_today[0][0], 
                "broi prodajbi": attractions_today[0][1] 
            }

            return jsonify(attraction_info), 200
        else:
            return jsonify({"error": "No sales data found for this attraction"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch sales data"}), 500
    finally:
        cursor.close()
        connection.close()


@app.route("/sales/<int:RESTAURANT_ID>")
def get_sales_restaurant(RESTAURANT_ID):
    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute("""
            SELECT R.restaurant_name, M.dish_name, SUM(P.quantity) AS total_quantity, SUM(P.total_price) AS total_sales
            FROM PUBLIC.Purchases P
            JOIN PUBLIC.Restaurants R ON P.restaurant_id = R.restaurant_id
            JOIN PUBLIC.Menu M ON P.dish_id = M.dish_id
            WHERE R.restaurant_id = %s
            GROUP BY R.restaurant_name, M.dish_name
            ORDER BY R.restaurant_name;
        """, (RESTAURANT_ID,))

        restaurant_sales = cursor.fetchall()

        if restaurant_sales:
         
            sales_info = []

            for sale in restaurant_sales:
                sale_info = {
                    "restaurant_name": sale[0],  
                    "dish_name": sale[1],         
                    "total_quantity": sale[2],   
                    "total_sales": sale[3]      
                }
                sales_info.append(sale_info)

            return jsonify(sales_info), 200
        else:
            return jsonify({"error": "No sales data found for this restaurant"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch sales data"}), 500
    finally:
        cursor.close()
        connection.close()

@app.route("/tickets")
def get_tickets():
    connection = get_connection()
    cursor = connection.cursor()

    try:

        cursor.execute("SELECT * FROM TICKETS")
        tickets = cursor.fetchall()
        if tickets:

            all_tickets_data = []
            for ticket in tickets:
                attraction_info = {
                    "TICKET_ID": ticket[0],
                    "VISITOR_ID": ticket[1],
                    "PURCHASE_DATE": ticket[2],
                    "TICKET_PRICE": ticket[3],
                    "VALID_DATE": ticket[4]
                }
                all_tickets_data.append(attraction_info)

            return jsonify(all_tickets_data), 200

    except Exception as e:

        return jsonify({"error": "Failed to fetch  data"}), 500
    finally:
        cursor.close()
        connection.close()


@app.route("/financial")
def get_finalncial():
    connection = get_connection()  
    cursor = connection.cursor()
    try:

        cursor.execute("""SELECT * FROM Public.Transactions""")
        employees_data = cursor.fetchall() 

        if employees_data:      
            all_employees_data = []

            for employees in employees_data:
                employees = {
                    "TRANSACTION_ID": employees[0],  
                    "TRANSACTION_DATE": employees[1], 
                    "TRANSACTION_TYPE": employees[2],
                    "AMOUNT": employees[3],
                    "DETAILS": employees[4],
                  
                }
                all_employees_data.append(employees)

            return jsonify(all_employees_data), 200
        else:
            return jsonify({"error": "No Employees found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch employees data"}), 500
    finally:
        cursor.close()
        connection.close() 
@app.route("/financial/<int:TRANSACTION_ID>")
def get_finalncial_byid(TRANSACTION_ID):
    connection = get_connection()  
    cursor = connection.cursor()
    try:
        cursor.execute("""SELECT * FROM Public.Transactions WHERE TRANSACTION_ID=%s""",(TRANSACTION_ID,))
        employees_data = cursor.fetchall() 

        if employees_data:      
            all_employees_data = []

            for employees in employees_data:
                employees = {
                    "TRANSACTION_ID": employees[0],  
                    "TRANSACTION_DATE": employees[1], 
                    "TRANSACTION_TYPE": employees[2],
                    "AMOUNT": employees[3],
                    "DETAILS": employees[4],
                  
                }
                all_employees_data.append(employees)

            return jsonify(all_employees_data), 200
        else:
            return jsonify({"error": "No Employees found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch employees data"}), 500
    finally:
        cursor.close()
        connection.close()  

@app.route("/visitor")
def get_visitor():
    connection = get_connection()  
    cursor = connection.cursor()
    try:

        cursor.execute("""SELECT * FROM Public.VISITORS""")
        employees_data = cursor.fetchall() 

        if employees_data:      
            all_employees_data = []

            for employees in employees_data:
                employees = {
                    "VISITOR_ID": employees[0],  
                    "FIRST_NAME": employees[1], 
                    "LAST_NAME": employees[2],
                    "REGISTRATION_DATE": employees[3],
                    "ACCESS_CARD_NUMBER": employees[4],
                  
                }
                all_employees_data.append(employees)

            return jsonify(all_employees_data), 200
        else:
            return jsonify({"error": "No Employees found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch employees data"}), 500
    finally:
        cursor.close()
        connection.close() 
@app.route("/visitor/<int:VISITOR_ID>")
def get_visitor_byid(VISITOR_ID):
    connection = get_connection()  
    cursor = connection.cursor()
    try:

        cursor.execute("""SELECT * FROM Public.VISITORS WHERE VISITOR_ID=%s""",(VISITOR_ID,))
        employees_data = cursor.fetchall() 

        if employees_data:      
            all_employees_data = []

            for employees in employees_data:
                employees = {
                     "VISITOR_ID": employees[0],  
                    "FIRST_NAME": employees[1], 
                    "LAST_NAME": employees[2],
                    "REGISTRATION_DATE": employees[3],
                    "ACCESS_CARD_NUMBER": employees[4],
                  
                }
                all_employees_data.append(employees)

            return jsonify(all_employees_data), 200
        else:
            return jsonify({"error": "No Employees found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch employees data"}), 500
    finally:
        cursor.close()
        connection.close()

@app.route("/event")
def get_event():
    connection = get_connection()  
    cursor = connection.cursor()
    try:
        cursor.execute("""SELECT * FROM Public.EVENTS""")
        events_data = cursor.fetchall()



        if events_data:
            all_events_data = []

            for event in events_data:
                start_time = event[3].strftime('%H:%M:%S') if isinstance(event[3], datetime) else str(event[3])
                end_time = event[4].strftime('%H:%M:%S') if isinstance(event[4], datetime) else str(event[4])

                event_info = {
                    "EVENT_ID": event[0],  
                    "EVENT_NAME": event[1], 
                    "EVENT_DATE": event[2],
                    "START_TIME": start_time,
                    "END_TIME": end_time,
                    "DESCRIPTION": event[5],
                    "TICKET_REQUIRED": event[6]
                }
                all_events_data.append(event_info)

            return jsonify(all_events_data), 200
        else:
            return jsonify({"error": "No events found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch events data"}), 500
    finally:
        cursor.close()
        connection.close()


@app.route("/event/<int:EVENT_ID>")
def get_event_byid(EVENT_ID):
    connection = get_connection()  
    cursor = connection.cursor()
    try:

        cursor.execute("""SELECT * FROM Public.EVENTS WHERE EVENT_ID=%s""", (EVENT_ID,))
        event_data = cursor.fetchone()


        if event_data:         
            start_time = event_data[3].strftime('%H:%M:%S') if isinstance(event_data[3], datetime) else str(event_data[3])
            end_time = event_data[4].strftime('%H:%M:%S') if isinstance(event_data[4], datetime) else str(event_data[4])

            event_info = {
                "EVENT_ID": event_data[0],  
                "EVENT_NAME": event_data[1], 
                "EVENT_DATE": event_data[2],
                "START_TIME": start_time,
                "END_TIME": end_time,
                "DESCRIPTION": event_data[5],
                "TICKET_REQUIRED": event_data[6]
            }

            return jsonify(event_info), 200
        else:
            return jsonify({"error": f"Event with ID {EVENT_ID} not found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch event data"}), 500
    finally:
        cursor.close()
        connection.close()

@app.route("/tickets", methods=["POST"])
def create_ticket():

    visitor_id = request.args.get("visitor_id")
    purchase_date = request.args.get("purchase_date")
    ticket_price = request.args.get("ticket_price")
    valid_date = request.args.get("valid_date")

    return jsonify({
        "message": "Ticket created successfully!",
        "ticket": {
            "visitor_id": visitor_id,
            "purchase_date": purchase_date,
            "ticket_price": ticket_price,
            "valid_date": valid_date
        }
    }), 201
 
if __name__ == "__main__":
    app.run(debug=True)                            