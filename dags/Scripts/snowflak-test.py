import snowflake.connector

conn = snowflake.connector.connect(
        user='PRAJWAL',
        password='Internship@123',
account='ng48387.ap-southeast-1',
        warehouse='COMPUTE_WH',
        database='PRAJWAL_ELT',
        schema='WEATHER_API',
            role='ACCOUNTADMIN'
)

cur = conn.cursor()
try:
    cur.execute("SELECT CURRENT_VERSION()")
    version = cur.fetchone()
    print("Snowflake Version:", version)
finally:
    cur.close()
    conn.close()
