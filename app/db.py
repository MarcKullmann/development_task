import psycopg2

from .db_utils import *

from config.settings import DATABASE

def create_tables(database=DATABASE):
    """ creates predefined tables with schema to the Postgres Database
    
    """
    
    commands = [
    """
        CREATE TABLE IF NOT EXISTS cc050 (
            id SERIAL PRIMARY KEY,
            date VARCHAR(255) NOT NULL,
            clearing_member VARCHAR(255) NOT NULL,
            account VARCHAR(255) NOT NULL,
            margin_type VARCHAR(255) NOT NULL,
            margin VARCHAR(255) NOT NULL
        )
    """,
    """
        CREATE TABLE IF NOT EXISTS ci050 (
            id SERIAL PRIMARY KEY,
            date VARCHAR(255) NOT NULL,
            time_of_day VARCHAR(255) NOT NULL,
            clearing_member VARCHAR(255) NOT NULL,
            account VARCHAR(255) NOT NULL,
            margin_type VARCHAR(255) NOT NULL,
            margin VARCHAR(255) NOT NULL
        )
    """
    ]
    
    connection = create_connection(database)
    cur = connection.cursor()
        
    try:
        for command in commands:
            cur.execute(command)
            
        connection.commit()
        
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")
        return None
    except Exception as e:
        print(f"Error creating tables: {e}")
        return None
    finally:
        cur.close()
        connection.close()
        
def setup_module():
    
    """populates predefined files into the Postgres database
    """
    
    try:
        cc050_data = load_fixtures("cc050.json")
        ci050_data = load_fixtures("ci050.json")
        
        upload_helper(
            "cc050",
            cc050_data,
            ["date", "clearing_member", "account", "margin_type", "margin"])
        upload_helper(
            "ci050", 
            ci050_data, 
            ["date", "time_of_day", "clearing_member", "account", "margin_type", "margin"])
        
        test_population()
    except Exception as e:
        print(f"Error populating tables: {e}")

if __name__ == '__main__':
    create_tables()
    setup_module()
