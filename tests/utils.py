from app.db_utils import create_connection

def database_integrity():
    try:
        connection = create_connection()
        cur = connection.cursor()

        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'cc050' OR table_name = 'ci050'")
        columns = cur.fetchall()
        
        if not columns:
            raise ValueError("No columns found for the specified tables.")
        
        return True
    
    except Exception as e:
        print(f"Error at database integrity check: {e}")
        return None

