from flask import Blueprint, jsonify
from Database.SnowflakeConnection import SnowflakeConnection

employees = Blueprint("employees", __name__)


@employees.route("/employees", methods=["GET"])
def get_employees():
    employees_data = SnowflakeConnection.execute_query("SELECT * FROM Public.Employees")

    if not employees_data:
        return jsonify({"error": "No employees found"}), 404

    return jsonify([
        {
            "EMPLOYEE_ID": row[0],
            "FIRST_NAME": row[1],
            "LAST_NAME": row[2],
            "PHONE_NUMBER": row[3],
            "HIRE_DATE": row[4],
            "JOB_TITLE": row[5],
            "SALARY": row[6],
            "DEPARTMENT": row[7],
        } for row in employees_data
    ]), 200


@employees.route("/employees/<int:employee_id>", methods=["GET"])
def get_employee_by_id(employee_id):
    row = SnowflakeConnection.execute_query(
        "SELECT * FROM Public.Employees WHERE EMPLOYEE_ID = %s", 
        (employee_id,), 
        single=True
    )

    if not row:
        return jsonify({"error": "Employee not found"}), 404

    return jsonify({
        "EMPLOYEE_ID": row[0],
        "FIRST_NAME": row[1],
        "LAST_NAME": row[2],
        "PHONE_NUMBER": row[3],
        "HIRE_DATE": row[4],
        "JOB_TITLE": row[5],
        "SALARY": row[6],
        "DEPARTMENT": row[7],
    }), 200

       