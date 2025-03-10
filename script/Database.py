import random
import datetime
import argparse
import snowflake.connector as sf

def get_connection():
    """
    Establish a connection to the Snowflake LUNAPARK database.
    """
    connection = sf.connect(
        user="KAFETO111",
        password="12qwaszxKafeto-",
        account="pmmclla-kn79190",
        warehouse="COMPUTE_WH",  # Your warehouse
        database="LUNAPARK"      # Your database
    )
    return connection

def setup_schema(connection):
    """
    Create the database, schema, and tables if they do not already exist.
    """
    cursor = connection.cursor()
    try:
        # Create database and schema (if needed)
        cursor.execute("CREATE DATABASE IF NOT EXISTS LUNAPARK;")
        cursor.execute("USE DATABASE LUNAPARK;")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC;")
        
        # Visitors and Tickets
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Visitors (
                visitor_id INT AUTOINCREMENT PRIMARY KEY,
                first_name STRING NOT NULL,
                last_name STRING NOT NULL,
                birth_date DATE,
                registration_date DATE DEFAULT CURRENT_DATE,
                access_card_number STRING UNIQUE NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Tickets (
                ticket_id INT AUTOINCREMENT PRIMARY KEY,
                visitor_id INT NOT NULL,
                purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ticket_price DECIMAL(10,2) NOT NULL,
                valid_date DATE NOT NULL,
                FOREIGN KEY (visitor_id) REFERENCES PUBLIC.Visitors(visitor_id)
            );
        """)
        
        # Attractions, Visits, Maintenance
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Attractions (
                attraction_id INT AUTOINCREMENT PRIMARY KEY,
                name STRING NOT NULL,
                type STRING,
                capacity INT NOT NULL,
                location STRING
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Visits (
                visit_id INT AUTOINCREMENT PRIMARY KEY,
                visitor_id INT NOT NULL,
                attraction_id INT NOT NULL,
                visit_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (visitor_id) REFERENCES PUBLIC.Visitors(visitor_id),
                FOREIGN KEY (attraction_id) REFERENCES PUBLIC.Attractions(attraction_id)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Maintenance (
                maintenance_id INT AUTOINCREMENT PRIMARY KEY,
                attraction_id INT NOT NULL,
                check_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status STRING NOT NULL,
                notes STRING,
                FOREIGN KEY (attraction_id) REFERENCES PUBLIC.Attractions(attraction_id)
            );
        """)
        
        # Employees, Shifts, Payments
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Employees (
                employee_id INT AUTOINCREMENT PRIMARY KEY,
                first_name STRING NOT NULL,
                last_name STRING NOT NULL,
                phone_number STRING,
                hire_date DATE NOT NULL,
                job_title STRING NOT NULL,
                salary DECIMAL(10,2),
                department STRING
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Shifts (
                shift_id INT AUTOINCREMENT PRIMARY KEY,
                employee_id INT NOT NULL,
                shift_date DATE NOT NULL,
                shift_start TIME NOT NULL,
                shift_end TIME NOT NULL,
                FOREIGN KEY (employee_id) REFERENCES PUBLIC.Employees(employee_id)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Payments (
                payment_id INT AUTOINCREMENT PRIMARY KEY,
                employee_id INT NOT NULL,
                payment_date DATE DEFAULT CURRENT_DATE,
                amount DECIMAL(10,2) NOT NULL,
                description STRING,
                FOREIGN KEY (employee_id) REFERENCES PUBLIC.Employees(employee_id)
            );
        """)
    
        # Restaurants, Menu, Purchases
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Restaurants (
                restaurant_id INT AUTOINCREMENT PRIMARY KEY,
                restaurant_name STRING NOT NULL,
                location STRING
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Menu (
                dish_id INT AUTOINCREMENT PRIMARY KEY,
                dish_name STRING NOT NULL,
                dish_price DECIMAL(10,2) NOT NULL,
                restaurant_id INT NOT NULL,
                FOREIGN KEY (restaurant_id) REFERENCES PUBLIC.Restaurants(restaurant_id)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Purchases (
                purchase_id INT AUTOINCREMENT PRIMARY KEY,
                visitor_id INT NOT NULL,
                restaurant_id INT NOT NULL,
                dish_id INT NOT NULL,
                purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                quantity INT NOT NULL,
                total_price DECIMAL(10,2) NOT NULL,
                FOREIGN KEY (visitor_id) REFERENCES PUBLIC.Visitors(visitor_id),
                FOREIGN KEY (restaurant_id) REFERENCES PUBLIC.Restaurants(restaurant_id),
                FOREIGN KEY (dish_id) REFERENCES PUBLIC.Menu(dish_id)
            );
        """)
        
        # Financial Transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Transactions (
                transaction_id INT AUTOINCREMENT PRIMARY KEY,
                transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transaction_type STRING NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                details STRING
            );
        """)
        
        # Special Events
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PUBLIC.Events (
                event_id INT AUTOINCREMENT PRIMARY KEY,
                event_name STRING NOT NULL,
                event_date DATE NOT NULL,
                start_time TIME NOT NULL,
                end_time TIME NOT NULL,
                description STRING,
                ticket_required BOOLEAN DEFAULT FALSE
            );
        """)
        cursor.execute("""
            CREATE TABLE PUBLIC.AttractionVisits (
                visit_id INT AUTOINCREMENT PRIMARY KEY,
                visitor_id INT,
                attraction_id INT,
                visit_date TIMESTAMP,
                FOREIGN KEY (visitor_id) REFERENCES PUBLIC.Visitors(visitor_id),
                FOREIGN KEY (attraction_id) REFERENCES PUBLIC.Attractions(attraction_id)
            );
""")
        
        connection.commit()
        print("Schema and tables created successfully.")
    except Exception as e:
        print("Error setting up schema:", e)
    finally:
        cursor.close()

# ------------------ Data Generation Functions ------------------ #

def generate_visitors(n):
    inserts = []
    for i in range(n):
        first_name = random.choice(["Ivan", "Georgi", "Petar", "Maria", "Elena", "Stefka"])
        last_name = random.choice(["Ivanov", "Georgiev", "Petrov", "Dimitrova", "Stoyanova"])
        birth_year = random.randint(1950, 2010)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)
        birth_date = datetime.date(birth_year, birth_month, birth_day)
        registration_date = datetime.date.today() - datetime.timedelta(days=random.randint(0, 365))
        access_card_number = f"AC{1000 + i}"
        stmt = f"INSERT INTO PUBLIC.Visitors (first_name, last_name, birth_date, registration_date, access_card_number) VALUES ('{first_name}', '{last_name}', '{birth_date}', '{registration_date}', '{access_card_number}');"
        inserts.append(stmt)
    return inserts

def generate_tickets(n, visitor_ids, climate):
    inserts = []
    for _ in range(n):
        visitor_id = random.choice(visitor_ids)
        purchase_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
        ticket_price = round(random.uniform(20, 50), 2) if climate == "positive" else round(random.uniform(5, 20), 2)
        valid_date = datetime.date.today() + datetime.timedelta(days=random.randint(0, 10))
        stmt = f"INSERT INTO PUBLIC.Tickets (visitor_id, purchase_date, ticket_price, valid_date) VALUES ({visitor_id}, '{purchase_date}', {ticket_price}, '{valid_date}');"
        inserts.append(stmt)
    return inserts

def generate_attractions(n):
    inserts = []
    attraction_types = [("Влакче", "Roller Coaster"), ("Въртележка", "Carousel"), ("Симулатор", "Simulator"), ("Бамперни коли", "Bumper Cars")]
    for i in range(n):
        name, _ = random.choice(attraction_types)
        capacity = random.randint(10, 100)
        location = random.choice(["Зона А", "Зона Б", "Зона В"])
        stmt = f"INSERT INTO PUBLIC.Attractions (name, type, capacity, location) VALUES ('{name} {i}', '{name}', {capacity}, '{location}');"
        inserts.append(stmt)
    return inserts

def generate_visits(n, visitor_ids, attraction_ids):
    inserts = []
    for _ in range(n):
        visitor_id = random.choice(visitor_ids)
        attraction_id = random.choice(attraction_ids)
        visit_time = datetime.datetime.now() - datetime.timedelta(hours=random.randint(0, 12))
        stmt = f"INSERT INTO PUBLIC.Visits (visitor_id, attraction_id, visit_time) VALUES ({visitor_id}, {attraction_id}, '{visit_time}');"
        inserts.append(stmt)
    return inserts

def generate_maintenance(n, attraction_ids):
    inserts = []
    statuses = ["OK", "Needs Repair", "Out of Service"]
    for _ in range(n):
        attraction_id = random.choice(attraction_ids)
        check_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 7))
        status = random.choice(statuses)
        notes = "Автоматична проверка"
        stmt = f"INSERT INTO PUBLIC.Maintenance (attraction_id, check_date, status, notes) VALUES ({attraction_id}, '{check_date}', '{status}', '{notes}');"
        inserts.append(stmt)
    return inserts

def generate_employees(n):
    inserts = []
    for _ in range(n):
        first_name = random.choice(["Ivan", "Georgi", "Petar", "Maria", "Elena"])
        last_name = random.choice(["Ivanov", "Georgiev", "Petrov", "Dimitrova", "Stoyanova"])
        phone_number = f"+359{random.randint(100000000, 999999999)}"
        hire_date = datetime.date.today() - datetime.timedelta(days=random.randint(0, 2000))
        job_title = random.choice(["Мениджър", "Оператор", "Техник", "Служител"])
        salary = round(random.uniform(2000, 4000), 2) if job_title == "Мениджър" else round(random.uniform(1000, 2500), 2)
        department = random.choice(["Администрация", "Обслужване", "Поддръжка"])
        stmt = f"INSERT INTO PUBLIC.Employees (first_name, last_name, phone_number, hire_date, job_title, salary, department) VALUES ('{first_name}', '{last_name}', '{phone_number}', '{hire_date}', '{job_title}', {salary}, '{department}');"
        inserts.append(stmt)
    return inserts

def generate_shifts(n, employee_ids):
    inserts = []
    for _ in range(n):
        employee_id = random.choice(employee_ids)
        shift_date = datetime.date.today() - datetime.timedelta(days=random.randint(0, 30))
        shift_start = datetime.time(hour=random.randint(9, 16), minute=0)
        shift_end = datetime.time(hour=random.randint(17, 22), minute=0)
        stmt = f"INSERT INTO PUBLIC.Shifts (employee_id, shift_date, shift_start, shift_end) VALUES ({employee_id}, '{shift_date}', '{shift_start}', '{shift_end}');"
        inserts.append(stmt)
    return inserts

def generate_payments(n, employee_ids):
    inserts = []
    for _ in range(n):
        employee_id = random.choice(employee_ids)
        payment_date = datetime.date.today() - datetime.timedelta(days=random.randint(0, 30))
        amount = round(random.uniform(500, 2000), 2)
        description = "Месечно плащане"
        stmt = f"INSERT INTO PUBLIC.Payments (employee_id, payment_date, amount, description) VALUES ({employee_id}, '{payment_date}', {amount}, '{description}');"
        inserts.append(stmt)
    return inserts

def generate_restaurants(n):
    inserts = []
    for i in range(n):
        restaurant_name = random.choice(["Фуд Парк", "Бърза храна", "Кафене", "Бистро"])
        location = random.choice(["Център", "Зона А", "Зона Б"])
        stmt = f"INSERT INTO PUBLIC.Restaurants (restaurant_name, location) VALUES ('{restaurant_name} {i}', '{location}');"
        inserts.append(stmt)
    return inserts

def generate_menu(n, restaurant_ids, climate):
    inserts = []
    for i in range(n):
        dish_name = random.choice(["Хот дог", "Бургер", "Пица", "Сандвич", "Салата"])
        dish_price = round(random.uniform(10, 25), 2) if climate == "positive" else round(random.uniform(5, 15), 2)
        restaurant_id = random.choice(restaurant_ids)
        stmt = f"INSERT INTO PUBLIC.Menu (dish_name, dish_price, restaurant_id) VALUES ('{dish_name} {i}', {dish_price}, {restaurant_id});"
        inserts.append(stmt)
    return inserts

def generate_purchases(n, visitor_ids, restaurant_ids, dish_ids):
    inserts = []
    for _ in range(n):
        visitor_id = random.choice(visitor_ids)
        restaurant_id = random.choice(restaurant_ids)
        dish_id = random.choice(dish_ids)
        purchase_date = datetime.datetime.now() - datetime.timedelta(hours=random.randint(0, 12))
        quantity = random.randint(1, 5)
        total_price = round(quantity * random.uniform(5, 25), 2)
        stmt = f"INSERT INTO PUBLIC.Purchases (visitor_id, restaurant_id, dish_id, purchase_date, quantity, total_price) VALUES ({visitor_id}, {restaurant_id}, {dish_id}, '{purchase_date}', {quantity}, {total_price});"
        inserts.append(stmt)
    return inserts

def generate_transactions(n, climate):
    inserts = []
    transaction_types = ["Ticket Sale", "Food Sale", "Merch Sale"]
    for i in range(n):
        transaction_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
        transaction_type = random.choice(transaction_types)
        amount = round(random.uniform(50, 500), 2) if climate == "positive" else round(random.uniform(10, 100), 2)
        details = f"Автоматична транзакция {i}"
        stmt = f"INSERT INTO PUBLIC.Transactions (transaction_date, transaction_type, amount, details) VALUES ('{transaction_date}', '{transaction_type}', {amount}, '{details}');"
        inserts.append(stmt)
    return inserts

def generate_events(n):
    inserts = []
    for i in range(n):
        event_name = random.choice(["Концерт", "Промоция", "Събитие"])
        event_date = datetime.date.today() + datetime.timedelta(days=random.randint(1, 30))
        start_time = datetime.time(hour=random.randint(17, 20), minute=0)
        end_time = datetime.time(hour=start_time.hour + 2, minute=0)
        description = "Специално събитие"
        ticket_required = random.choice([True, False])
        stmt = f"INSERT INTO PUBLIC.Events (event_name, event_date, start_time, end_time, description, ticket_required) VALUES ('{event_name} {i}', '{event_date}', '{start_time}', '{end_time}', '{description}', {str(ticket_required).upper()});"
        inserts.append(stmt)
    return inserts

def execute_queries(connection, queries):
    cursor = connection.cursor()
    try:
        for query in queries:
            cursor.execute(query)
        connection.commit()
    except Exception as e:
        print("Error executing queries:", e)
    finally:
        cursor.close()

# ------------------ Main Application ------------------ #

def main():
    parser = argparse.ArgumentParser(description="Lunapark System Data Loader")
    parser.add_argument("--climate", choices=["positive", "negative"], required=True,
                        help="Business climate: 'positive' for high attendance/prices, 'negative' for low attendance/prices.")
    args = parser.parse_args()
    climate = args.climate

    # Set record counts based on business climate
    if climate == "positive":
        num_visitors = 100
        num_tickets = 150
        num_attractions = 10
        num_visits = 300
        num_maintenance = 20
        num_employees = 30
        num_shifts = 60
        num_payments = 30
        num_restaurants = 5
        num_menu = 20
        num_purchases = 100
        num_transactions = 200
        num_events = 5
    else:
        num_visitors = 20
        num_tickets = 25
        num_attractions = 5
        num_visits = 40
        num_maintenance = 5
        num_employees = 10
        num_shifts = 20
        num_payments = 10
        num_restaurants = 2
        num_menu = 5
        num_purchases = 20
        num_transactions = 30
        num_events = 2

    # Connect to Snowflake and set up schema
    conn = get_connection()
    setup_schema(conn)

    all_queries = []

    # Generate and collect INSERT statements
    visitor_inserts = generate_visitors(num_visitors)
    all_queries.extend(visitor_inserts)
    visitor_ids = list(range(1, num_visitors + 1))

    ticket_inserts = generate_tickets(num_tickets, visitor_ids, climate)
    all_queries.extend(ticket_inserts)

    attraction_inserts = generate_attractions(num_attractions)
    all_queries.extend(attraction_inserts)
    attraction_ids = list(range(1, num_attractions + 1))

    visit_inserts = generate_visits(num_visits, visitor_ids, attraction_ids)
    all_queries.extend(visit_inserts)

    maintenance_inserts = generate_maintenance(num_maintenance, attraction_ids)
    all_queries.extend(maintenance_inserts)

    employee_inserts = generate_employees(num_employees)
    all_queries.extend(employee_inserts)
    employee_ids = list(range(1, num_employees + 1))

    shift_inserts = generate_shifts(num_shifts, employee_ids)
    all_queries.extend(shift_inserts)

    payment_inserts = generate_payments(num_payments, employee_ids)
    all_queries.extend(payment_inserts)

    restaurant_inserts = generate_restaurants(num_restaurants)
    all_queries.extend(restaurant_inserts)
    restaurant_ids = list(range(1, num_restaurants + 1))

    menu_inserts = generate_menu(num_menu, restaurant_ids, climate)
    all_queries.extend(menu_inserts)
    dish_ids = list(range(1, num_menu + 1))

    purchase_inserts = generate_purchases(num_purchases, visitor_ids, restaurant_ids, dish_ids)
    all_queries.extend(purchase_inserts)

    transaction_inserts = generate_transactions(num_transactions, climate)
    all_queries.extend(transaction_inserts)

    event_inserts = generate_events(num_events)
    all_queries.extend(event_inserts)

    # Execute all the generated INSERT statements
    execute_queries(conn, all_queries)
    print("Test data has been inserted successfully.")

    conn.close()

if __name__ == "__main__":
    main()