from flask import Blueprint, jsonify, request
from Database.SnowflakeConnection import SnowflakeConnection
from datetime import datetime
events = Blueprint("events", __name__)



@events.route("/event")
def get_event():
    connection = SnowflakeConnection.get_instance() 
    cursor = connection.cursor()
    
    event_name = request.args.get("name")  
    
    try:
        query = "SELECT * FROM Public.EVENTS"
        params = ()
        
        if event_name:
            query += " WHERE EVENT_NAME ILIKE %s"
            params = (f"%{event_name}%",)
        
        cursor.execute(query, params)
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
       
        return jsonify({"error": "No events found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch events data"}), 500
    finally:
        cursor.close()

@events.route("/event/<int:EVENT_ID>")
def get_event_byid(EVENT_ID):
    connection = SnowflakeConnection.get_instance()  
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
        
        return jsonify({"error": f"Event with ID {EVENT_ID} not found"}), 404

    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": "Failed to fetch event data"}), 500
    finally:
        cursor.close()