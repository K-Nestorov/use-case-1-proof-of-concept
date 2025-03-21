from flask import Blueprint, jsonify
from Database.SnowflakeConnection import SnowflakeConnection

financial = Blueprint("financial", __name__)


@financial.route("/financial", methods=["GET"])
def get_financial():


            financial_data=SnowflakeConnection.execute_query("SELECT * FROM Public.Transactions")
            if not financial_data:
                  return jsonify({"error": "No transactions found"}), 404

            return jsonify([
                    {
                        "TRANSACTION_ID": row[0],  
                        "TRANSACTION_DATE": row[1], 
                        "TRANSACTION_TYPE": row[2],
                        "AMOUNT": row[3],
                        "DETAILS": row[4],
                    } for row in financial_data
                ]), 200



@financial.route("/financial/<int:TRANSACTION_ID>")
def get_financial_by_id(TRANSACTION_ID):
    connection = SnowflakeConnection.get_instance() 
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT * FROM Public.Transactions WHERE TRANSACTION_ID = %s", (TRANSACTION_ID,))
        row = cursor.fetchone()  

        if row:
            return jsonify({
                "TRANSACTION_ID": row[0],  
                "TRANSACTION_DATE": row[1], 
                "TRANSACTION_TYPE": row[2],
                "AMOUNT": row[3],
                "DETAILS": row[4],
            }), 200
        
        return jsonify({"error": "Transaction not found"}), 404

    except Exception as e:      
        return jsonify({"error": "Failed to fetch transaction data"}), 500
    finally:
        cursor.close()
