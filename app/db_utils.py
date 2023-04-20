import json
import psycopg2
import os

from config.settings import DATABASE

def create_connection(database=DATABASE):
    """creates a connection to the Postgres Database

    Returns:
        connection
    """

    try:
        connection = psycopg2.connect(
            host=DATABASE['host'],
            port=DATABASE['port'],
            user=DATABASE['user'],
            password=DATABASE['password'],
            dbname=DATABASE['name'],
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def insert_rows(table, columns, values, database=DATABASE):
    
    """insets records into the Postgres database
    
    """
    
    query = (f"INSERT INTO {table} "
             f"({', '.join(columns)}) "
             f"VALUES ({', '.join(['%s'] * len(values))})"
             )
    
    connection = create_connection(database)
    cur = connection.cursor()
    
    try:
        cur.execute(query, values)
        connection.commit()
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")
        return None
    except Exception as e:
        print(f"Error inserting row: {e}")
        connection.rollback()
    finally:
        cur.close()
        connection.close()
        
def select_rows(table, columns, n, database=DATABASE):
    
    """selects a limited number of recrods and columns from a table

    Returns:
        rows
    """
    
    query = (f"SELECT "
             f"{', '.join(columns)} " 
             f"FROM {table} "
             f"ORDER BY id DESC LIMIT {n}")
    
    connection = create_connection(database)
    cur = connection.cursor()
    
    try:
        cur.execute(query)
        rows = cur.fetchall()
        return rows
    except Exception as e:
        print(f"Error selecting rows: {e}")
    finally:
        cur.close()
        connection.close()
        
def execute_query(query, database=DATABASE):
    
    """executes a predefined query

    Returns:
        rows
    """
    
    connection = create_connection(database)
    cur = connection.cursor()
    
    try:
        cur.execute(query)
        rows = cur.fetchall()
        return rows
    except Exception as e:
        print(f"Error selecting rows: {e}")
    finally:
        cur.close()
        connection.close()
        
def load_fixtures(file_name):
    fixture_file = os.path.join(os.path.dirname(__file__), "fixtures", file_name)
    with open(fixture_file, "r") as f:
        data = json.load(f)
    return data

def test_population(n=2):
    rows = select_rows(
        "cc050",
        ["date", "clearing_member", "account", "margin_type", "margin"],
        n)
    print(rows)

def upload_helper(table, input_data, col_names):
    for key in input_data.keys():
        for value in input_data[key]:
            insert_rows(
                table, 
                col_names,
                value
                )
    return True

def value_list_generator(columns, values):
    insert_list = []
    insert_list.append(dict(zip(columns, values)))
    return insert_list