from flask import Flask
from Controller.attractions_controller import attractions 
from Controller.employees_controller import employees
from Controller.sales_controller import sales
from Controller.tickets_controller import tickets
from Controller.financial_controller import financial
from Controller.visitors_controller import visitors
from Controller.events_controller import events

app = Flask(__name__)

app.register_blueprint(sales,url_prefix="/api")
app.register_blueprint(attractions, url_prefix="/api")
app.register_blueprint(employees,url_prefix="/api")
app.register_blueprint(tickets,url_prefix="/api")
app.register_blueprint(financial,url_prefix="/api")
app.register_blueprint(visitors,url_prefix="/api")
app.register_blueprint(events,url_prefix="/api")
if __name__ == "__main__":
    app.run(debug=True)
