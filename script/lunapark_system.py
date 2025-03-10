import random
import datetime
import snowflake.connector as sf
import sys


def get_connection():
    """Establish a connection to the Snowflake LUNAPARK database."""
    try:
        connection = sf.connect(
            user="KAFETO111",
            password="12qwaszxKafeto-",
            account="pmmclla-kn79190",
            warehouse="COMPUTE_WH",
            database="LUNAPARK"
        )
        return connection
    except Exception as e:
        print(f"Error: Unable to connect to Snowflake - {e}")
        sys.exit()


def fetch_restaurants(connection):
    """Fetch the list of restaurants from the database."""
    cursor = connection.cursor()
    cursor.execute("SELECT restaurant_id, restaurant_name FROM PUBLIC.Restaurants;")
    restaurants = cursor.fetchall()
    cursor.close()
    return restaurants


def fetch_menu(connection, restaurant_id):
    """Fetch the menu of a specific restaurant."""
    cursor = connection.cursor()
    cursor.execute("""
        SELECT dish_id, dish_name, dish_price 
        FROM PUBLIC.Menu 
        WHERE restaurant_id = %s;
    """, (restaurant_id,))
    menu = cursor.fetchall()
    cursor.close()
    return menu


def fetch_attractions(connection):
    """Fetch the list of attractions from the database without price."""
    cursor = connection.cursor()
    cursor.execute("SELECT ATTRACTION_ID, NAME FROM PUBLIC.Attractions;")
    attractions = cursor.fetchall()
    cursor.close()

    print("Fetched attractions:", attractions)

    return attractions


def fetch_ticket_details(connection, ticket_number):
    """Fetch details related to a visitor's ticket."""
    cursor = connection.cursor()
    cursor.execute("""
        SELECT V.first_name, V.last_name, T.TICKET_PRICE, V.visitor_id
        FROM PUBLIC.Visitors V
        JOIN PUBLIC.Tickets T ON V.visitor_id = T.VISITOR_ID
        WHERE T.TICKET_ID = %s;
    """, (ticket_number,))
    ticket_details = cursor.fetchone()
    cursor.close()
    return ticket_details


def make_purchase(connection, visitor_id, restaurant_id, dish_id, quantity):
    """Insert a purchase transaction into the database."""
    cursor = connection.cursor()
    cursor.execute("""
        SELECT dish_price FROM PUBLIC.Menu WHERE dish_id = %s;
    """, (dish_id,))
    price_per_dish = cursor.fetchone()[0]
    total_price = price_per_dish * quantity
    purchase_date = datetime.datetime.now()

    cursor.execute("""
        INSERT INTO PUBLIC.Purchases (visitor_id, restaurant_id, dish_id, purchase_date, quantity, total_price)
        VALUES (%s, %s, %s, %s, %s, %s);
    """, (visitor_id, restaurant_id, dish_id, purchase_date, quantity, total_price))
    connection.commit()
    cursor.close()

    return total_price


def save_attraction_visit(connection, visitor_id, attraction_id):
    """Save the visit to an attraction."""
    visit_date = datetime.datetime.now()
    cursor = connection.cursor()
    cursor.execute("""
        INSERT INTO PUBLIC.ATTRACTIONVISITS (visitor_id, attraction_id, visit_date)
        VALUES (%s, %s, %s);
    """, (visitor_id, attraction_id, visit_date))
    connection.commit()
    cursor.close()

    print("\nHope you enjoyed the attraction! Your visit has been recorded.")


def fetch_employee(connection, employee_id):
    """Fetch employee details based on employee_id."""
    cursor = connection.cursor()
    cursor.execute("""
        SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, PHONE_NUMBER, HIRE_DATE, JOB_TITLE, SALARY, DEPARTMENT
        FROM PUBLIC.Employees
        WHERE EMPLOYEE_ID = %s;
    """, (employee_id,))
    employee = cursor.fetchone()
    cursor.close()
    return employee


def add_employee(connection):
    """Add a new employee to the database."""

    first_name = input("Enter the employee's first name: ").strip()
    last_name = input("Enter the employee's last name: ").strip()
    phone_number = input("Enter the employee's phone number: ").strip()
    hire_date = input("Enter the employee's hire date (YYYY-MM-DD): ").strip()
    job_title = input("Enter the employee's job title: ").strip()
    salary = input("Enter the employee's salary: ").strip()
    department = input("Enter the employee's department: ").strip()

    try:
        salary = float(salary)
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO PUBLIC.Employees (FIRST_NAME, LAST_NAME, PHONE_NUMBER, HIRE_DATE, JOB_TITLE, SALARY, DEPARTMENT)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (first_name, last_name, phone_number, hire_date, job_title, salary, department))
        connection.commit()
        cursor.close()
        print(f"Employee {first_name} {last_name} added successfully.")
    except ValueError:
        print("Invalid salary entered. Please enter a numeric value.")
    except Exception as e:
        print(f"Error: {e}")


def update_employee(connection):
    """Update details of an existing employee."""
    employee_id = input("Enter the employee ID to update: ").strip()

    employee = fetch_employee(connection, employee_id)
    if not employee:
        print("Employee not found.")
        return

    print(f"Employee found: {employee[1]} {employee[2]} ({employee[0]})")

    first_name = input(f"Enter new first name (leave blank to keep '{employee[1]}'): ").strip() or employee[1]
    last_name = input(f"Enter new last name (leave blank to keep '{employee[2]}'): ").strip() or employee[2]
    phone_number = input(f"Enter new phone number (leave blank to keep '{employee[3]}'): ").strip() or employee[3]
    hire_date = input(f"Enter new hire date (leave blank to keep '{employee[4]}'): ").strip() or employee[4]
    job_title = input(f"Enter new job title (leave blank to keep '{employee[5]}'): ").strip() or employee[5]
    salary = input(f"Enter new salary (leave blank to keep '{employee[6]}'): ").strip() or employee[6]
    department = input(f"Enter new department (leave blank to keep '{employee[7]}'): ").strip() or employee[7]

    try:
        if salary:
            salary = float(salary)

        cursor = connection.cursor()
        cursor.execute("""
            UPDATE PUBLIC.Employees
            SET FIRST_NAME = %s, LAST_NAME = %s, PHONE_NUMBER = %s, HIRE_DATE = %s, JOB_TITLE = %s, SALARY = %s, DEPARTMENT = %s
            WHERE EMPLOYEE_ID = %s;
        """, (first_name, last_name, phone_number, hire_date, job_title, salary, department, employee_id))
        connection.commit()
        cursor.close()
        print(f"Employee {employee_id}'s details updated successfully.")
    except ValueError:
        print("Invalid salary entered. Please enter a numeric value.")
    except Exception as e:
        print(f"Error: {e}")


def view_employee(connection):
    """View details of an employee by employee_id."""
    employee_id = input("Enter the employee ID to view: ").strip()
    employee = fetch_employee(connection, employee_id)

    if employee:
        print("\nEmployee Details:")
        print(f"ID: {employee[0]}")
        print(f"Name: {employee[1]} {employee[2]}")
        print(f"Phone: {employee[3]}")
        print(f"Hire Date: {employee[4]}")
        print(f"Job Title: {employee[5]}")
        print(f"Salary: ${employee[6]:.2f}")
        print(f"Department: {employee[7]}")
    else:
        print("Employee not found.")


def employee_management(connection):
    """Employee management functions for admins."""
    while True:
        print("\n----- Employee Management -----")
        print("1. Add new employee")
        print("2. Update employee details")
        print("3. View employee details")
        print("4. Exit employee management")

        choice = input("Choose an option: ").strip()

        if choice == '1':
            add_employee(connection)
        elif choice == '2':
            update_employee(connection)
        elif choice == '3':
            view_employee(connection)
        elif choice == '4':
            break
        else:
            print("Invalid choice. Please try again.")


def admin_panel(connection):
    """Admin panel to manage restaurants, menu, attractions, employees, and show daily statistics."""
    while True:
        print("\n----- Admin Panel -----")
        print("1. Add new restaurant")
        print("2. Add new dish to a restaurant")
        print("3. Add new attraction")
        print("4. Employee Management")
        print("5. View daily statistics")
        print("6. Exit admin panel")

        choice = input("Choose an option: ").strip()

        if choice == '1':
            name = input("Enter the restaurant name: ").strip()
            cursor = connection.cursor()
            cursor.execute("""
                INSERT INTO PUBLIC.Restaurants (restaurant_name)
                VALUES (%s);
            """, (name,))
            connection.commit()
            cursor.close()
            print(f"Restaurant '{name}' added successfully.")
        elif choice == '2':
            restaurant_id = input("Enter the restaurant ID: ").strip()
            dish_name = input("Enter the dish name: ").strip()
            dish_price = input("Enter the dish price: ").strip()
            try:
                dish_price = float(dish_price)
                cursor = connection.cursor()
                cursor.execute("""
                    INSERT INTO PUBLIC.Menu (restaurant_id, dish_name, dish_price)
                    VALUES (%s, %s, %s);
                """, (restaurant_id, dish_name, dish_price))
                connection.commit()
                cursor.close()
                print(f"Dish '{dish_name}' added successfully.")
            except ValueError:
                print("Invalid price entered. Please enter a numeric value.")
        elif choice == '3':
            attraction_name = input("Enter the attraction name: ").strip()
            attraction_type = input("Enter the attraction type: ").strip()
            capacity = input("Enter the attraction capacity: ").strip()
            location = input("Enter the attraction location: ").strip()

            # Validate and convert capacity to integer
            try:
                capacity = int(capacity)
            except ValueError:
                print("Invalid capacity. Please enter a valid number.")
                continue

            # Insert into Attractions table
            cursor = connection.cursor()
            cursor.execute("""
                INSERT INTO PUBLIC.Attractions (NAME, TYPE, CAPACITY, LOCATION)
                VALUES (%s, %s, %s, %s);
            """, (attraction_name, attraction_type, capacity, location))
            connection.commit()
            cursor.close()
            print(f"Attraction '{attraction_name}' added successfully.")
        elif choice == '4':
            employee_management(connection)
        elif choice == '5':
            print("\n----- Daily Statistics -----")

            cursor = connection.cursor()

            # 1. Total visitors today
            cursor.execute("""
                SELECT COUNT(*) FROM PUBLIC.Tickets 
                WHERE TO_DATE(VALID_DATE) = CURRENT_DATE;
                """)
            visitors_today = cursor.fetchone()[0]
            print(f"\nTotal visitors today: {visitors_today}")

            # 2. Attractions visited today
            cursor.execute("""
                SELECT A.NAME, COUNT(*) 
                FROM PUBLIC.AttractionVisits AV
                JOIN PUBLIC.Attractions A ON AV.attraction_id = A.ATTRACTION_ID
                WHERE TO_DATE(AV.visit_date) = CURRENT_DATE
                GROUP BY A.NAME;
                """)
            attractions_today = cursor.fetchall()

            if attractions_today:
                print("\nAttractions visited today:")
                for attraction, count in attractions_today:
                    print(f"- {attraction}: {count} visitors")
            else:
                print("\nNo attractions visited today.")

            # 3. Restaurant transactions today
            cursor.execute("""
                SELECT R.restaurant_name, M.dish_name, SUM(P.quantity) AS total_quantity, SUM(P.total_price) AS total_sales
                FROM PUBLIC.Purchases P
                JOIN PUBLIC.Restaurants R ON P.restaurant_id = R.restaurant_id
                JOIN PUBLIC.Menu M ON P.dish_id = M.dish_id
                WHERE TO_DATE(P.purchase_date) = CURRENT_DATE
                GROUP BY R.restaurant_name, M.dish_name
                ORDER BY R.restaurant_name;
                """)
            restaurant_sales = cursor.fetchall()

            if restaurant_sales:
                print("\nRestaurant Sales Today:")
                for restaurant, dish, quantity, total_sales in restaurant_sales:
                    print(f"- {restaurant} sold {quantity}x {dish}, Total Sales: ${total_sales:.2f}")
            else:
                print("\nNo restaurant transactions today.")

            cursor.close()

        elif choice == '6':
            break

        else:
            print("Invalid option. Please try again.")


def main():
    conn = get_connection()
    print("Welcome to the Lunapark console app!\n")

    ticket_number = input("Enter your ticket number to validate: ").strip()
    ticket_details = fetch_ticket_details(conn, ticket_number)

    if ticket_details:
        print(
            f"\nTicket Validated! Welcome {ticket_details[0]} {ticket_details[1]}.\nYour ticket price: {ticket_details[2]}")
        visitor_id = ticket_details[3]
    else:
        print("\nInvalid ticket number. Exiting the app.")
        conn.close()
        sys.exit()

    while True:
        print("\n----- Main Menu -----")
        print("1. Visit a restaurant")
        print("2. Visit an attraction")
        print("3. Admin panel (for admins only)")
        print("4. Exit the application")

        main_choice = input("Choose an option: ").strip()

        if main_choice == '1':
            print("\n----- Available Restaurants -----")
            restaurants = fetch_restaurants(conn)
            for idx, (restaurant_id, restaurant_name) in enumerate(restaurants, 1):
                print(f"{idx}. {restaurant_name}")

            restaurant_choice = input("\nSelect a restaurant by number (or type 'exit' to go back): ").strip()

            if restaurant_choice.lower() == 'exit':
                continue

            try:
                restaurant_choice = int(restaurant_choice)
                selected_restaurant = restaurants[restaurant_choice - 1]
                restaurant_id = selected_restaurant[0]
                print(f"\nYou selected: {selected_restaurant[1]}")
            except (ValueError, IndexError):
                print("Invalid choice, please try again.")
                continue

            menu = fetch_menu(conn, restaurant_id)
            print("\n----- Menu -----")
            for idx, (dish_id, dish_name, dish_price) in enumerate(menu, 1):
                print(f"{idx}. {dish_name} - ${dish_price}")

            dish_choice = input("\nSelect a dish by number (or type 'back' to go back): ").strip()

            if dish_choice.lower() == 'back':
                continue

            try:
                dish_choice = int(dish_choice)
                selected_dish = menu[dish_choice - 1]
                dish_id = selected_dish[0]
                dish_name = selected_dish[1]
                dish_price = selected_dish[2]
                print(f"\nYou selected: {dish_name} - ${dish_price}")
            except (ValueError, IndexError):
                print("Invalid choice, please try again.")
                continue

            quantity = input(f"How many {dish_name}s would you like to order? ").strip()

            try:
                quantity = int(quantity)
                if quantity <= 0:
                    print("Invalid quantity. Please enter a positive number.")
                    continue
            except ValueError:
                print("Invalid quantity. Please enter a number.")
                continue

            total_price = make_purchase(conn, visitor_id, restaurant_id, dish_id, quantity)
            print(f"\nYour total order price for {dish_name} x {quantity}: ${total_price:.2f}")

        elif main_choice == '2':
            print("\n----- Available Attractions -----")
            attractions = fetch_attractions(conn)
            for idx, (attraction_id, attraction_name) in enumerate(attractions, 1):
                print(f"{idx}. {attraction_name} ")

            attraction_choice = input("\nSelect an attraction by number (or type 'back' to go back): ").strip()

            if attraction_choice.lower() == 'back':
                continue

            try:
                attraction_choice = int(attraction_choice)
                selected_attraction = attractions[attraction_choice - 1]
                attraction_id = selected_attraction[0]
                attraction_name = selected_attraction[1]

                print(f"\nYou selected: {attraction_name}")
                save_attraction_visit(conn, visitor_id, attraction_id)
            except (ValueError, IndexError):
                print("Invalid choice, please try again.")
                continue

        elif main_choice == '3':
            admin_panel(conn)

        elif main_choice == '4':
            print("\nThank you for using the Lunapark app!")
            break

        else:
            print("Invalid choice. Please try again.")

    conn.close()


if __name__ == "__main__":
    main()