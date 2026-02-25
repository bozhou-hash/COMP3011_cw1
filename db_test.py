import psycopg2

conn = psycopg2.connect(
    dbname="supermarket_price_db",
    user="postgres",
    password="bozhou0211",
    host="localhost",
    port="5432"
)

print("Connected successfully!")

conn.close()