import snowflake.connector
from dotenv import load_dotenv
import os

# Load .env variables
load_dotenv()

conn = snowflake.connector.connect(
        user=os.getenv("user_name"),
        password=os.getenv("pass"),
account=os.getenv("account_name"),
        warehouse=os.getenv("warehouse_name"),
        database=os.getenv("database_name"),
        schema=os.getenv("schema_name"),
            role=os.getenv("role_name")
)

cur = conn.cursor()
try:
    cur.execute("SELECT CURRENT_VERSION()")
    version = cur.fetchone()
    print("Snowflake Version:", version)
finally:
    cur.close()
    conn.close()
