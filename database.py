import sqlite3

def get_connection(port):
    return sqlite3.connect(f"students_{port}.db")


def init_db(port):

    conn = sqlite3.connect(f"students_{port}.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS students(
        id INTEGER PRIMARY KEY,
        name TEXT,
        age INTEGER
    )
    """)

    conn.commit()
    conn.close()