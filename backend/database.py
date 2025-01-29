import duckdb
import os

# Initialize or connect to the DuckDB database
DB_FILE = os.path.join(os.getcwd(), "spreadsheet_data.db")

def init_db():
    try:
        conn = duckdb.connect(database=DB_FILE, read_only=False)
        conn.execute("DROP TABLE IF EXISTS cells")  # Drop the table if it exists
        conn.execute("""
            CREATE TABLE cells (
                row INTEGER,
                "column" INTEGER,
                value TEXT,
                PRIMARY KEY (row, "column")  -- Composite primary key
            )
        """)
        print(f"Database initialized at: {DB_FILE}")
        return conn
    except Exception as e:
        print(f"Error initializing database: {e}")
        return None

def update_cell(conn, row, col, value):
    conn.execute("""
        INSERT OR REPLACE INTO cells (row, "column", value)
        VALUES (?, ?, ?)
    """, [row, col, value])


def get_all_cells(conn):
    return conn.execute("SELECT * FROM cells").fetchall()


def close_db(conn):
    conn.close()

def print_table_schema(conn):
    schema = conn.execute("DESCRIBE cells").fetchall()
    print("Table Schema:")
    for column in schema:
        print(column)
