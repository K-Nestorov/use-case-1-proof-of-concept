import os
import sys
import snowflake.connector as sf
from dotenv import load_dotenv

load_dotenv()

class SnowflakeConnection:
    _instance = None   

    @staticmethod
    def get_instance():
        if SnowflakeConnection._instance is None:
            try:
                SnowflakeConnection._instance = sf.connect(
                    user=os.getenv("DB_USER"),
                    password=os.getenv("DB_PASSWORD"),
                    account=os.getenv("DB_ACCOUNT"),
                    warehouse=os.getenv("DB_WAREHOUSE"),
                    database=os.getenv("DB_DATABASE")
                )
                print("Connected to Snowflake!")
            except Exception as e:
                print(f"Error: Unable to connect to Snowflake - {e}")
                sys.exit()
        return SnowflakeConnection._instance

    @staticmethod
    def execute_query(query, params=None, single=False):  
        connection = SnowflakeConnection.get_instance()
        try:
            cursor = connection.cursor()
            cursor.execute(query, params)  
            result = cursor.fetchone() if single else cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            print(f"Error: {e}")
            return None
