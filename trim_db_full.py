import os
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

ORIGINAL_DB_URL = os.getenv("DATABASE_URL")
TRIMMED_DB_URL = os.getenv("TRIMMED_DATABASE_URL")
MAX_PRODUCTS = 25000

if not ORIGINAL_DB_URL or not TRIMMED_DB_URL:
    raise ValueError("Make sure both DATABASE_URL and TRIMMED_DATABASE_URL are set in .env")

# Extract DB info for creation
import re
match = re.match(r"postgresql\+psycopg2://([^:]+):([^@]+)@([^:]+):(\d+)/(\w+)", TRIMMED_DB_URL)
if not match:
    raise ValueError("TRIMMED_DATABASE_URL is not valid")
user, password, host, port, new_db_name = match.groups()

# Step 1: Connect to default DB to create the trimmed database if not exists
conn = psycopg2.connect(
    dbname="postgres", user=user, password=password, host=host, port=port
)
conn.autocommit = True
cur = conn.cursor()
cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{new_db_name}'")
exists = cur.fetchone()
if not exists:
    print(f"Creating database {new_db_name}...")
    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db_name)))
else:
    print(f"Database {new_db_name} already exists.")
cur.close()
conn.close()

# Step 2: Connect to both databases
orig_engine = create_engine(ORIGINAL_DB_URL)
trim_engine = create_engine(TRIMMED_DB_URL)

OrigSession = sessionmaker(bind=orig_engine)
TrimSession = sessionmaker(bind=trim_engine)

orig_session = OrigSession()
trim_session = TrimSession()

# Step 3: Create tables in trimmed database (matching your original schema)
print("Creating tables in trimmed database...")
sql_commands = [
    """
    CREATE TABLE IF NOT EXISTS retailers (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        product_name_clean TEXT NOT NULL,
        product_group_id INT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS product_listings (
        id SERIAL PRIMARY KEY,
        product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        retailer_id INT NOT NULL REFERENCES retailers(id) ON DELETE CASCADE,
        original_name TEXT,
        own_brand BOOLEAN,
        category TEXT,
        UNIQUE(product_id, retailer_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS prices (
        id SERIAL PRIMARY KEY,
        listing_id INT NOT NULL REFERENCES product_listings(id) ON DELETE CASCADE,
        date DATE NOT NULL,
        price NUMERIC(10,2) NOT NULL,
        unit_price NUMERIC(10,4),
        UNIQUE(listing_id, date)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_prices_listing ON prices(listing_id);",
    "CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);",
    "CREATE INDEX IF NOT EXISTS idx_listings_product ON product_listings(product_id);",
    "CREATE INDEX IF NOT EXISTS idx_listings_retailer ON product_listings(retailer_id);",
    """
    CREATE TABLE IF NOT EXISTS product_groups (
        id SERIAL PRIMARY KEY,
        group_name TEXT NOT NULL,
        category TEXT,
        quantity TEXT
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_product_groups_category ON product_groups(category);",
    "CREATE INDEX IF NOT EXISTS idx_product_groups_quantity ON product_groups(quantity);"
]

with trim_engine.begin() as conn:
    for command in sql_commands:
        conn.execute(text(command))

# Step 4: Truncate tables in trimmed database before inserting
print("Truncating existing data in trimmed database...")
with trim_engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE prices CASCADE"))
    conn.execute(text("TRUNCATE TABLE product_listings CASCADE"))
    conn.execute(text("TRUNCATE TABLE products CASCADE"))
    conn.execute(text("TRUNCATE TABLE retailers CASCADE"))
    conn.execute(text("TRUNCATE TABLE product_groups CASCADE"))

# Step 5: Select top MAX_PRODUCTS
print(f"Selecting {MAX_PRODUCTS} products to include...")
with orig_engine.begin() as conn:
    top_products = conn.execute(text(f"""
        SELECT id FROM products
        ORDER BY id
        LIMIT :limit
    """), {"limit": MAX_PRODUCTS}).fetchall()
    top_product_ids = [row[0] for row in top_products]

# Step 6: Copy data
print("Copying data to trimmed database...")
with trim_engine.begin() as trim_conn, orig_engine.begin() as orig_conn:
    # Copy product_groups for selected products
    group_ids = orig_conn.execute(text("""
        SELECT DISTINCT product_group_id FROM products WHERE id = ANY(:ids)
    """), {"ids": top_product_ids}).fetchall()
    group_ids = [row[0] for row in group_ids if row[0] is not None]
    for gid in group_ids:
        g = orig_conn.execute(text("SELECT * FROM product_groups WHERE id=:id"), {"id": gid}).fetchone()
        trim_conn.execute(text("""
            INSERT INTO product_groups (id, group_name, category, quantity)
            VALUES (:id, :name, :category, :quantity) ON CONFLICT DO NOTHING
        """), {"id": g.id, "name": g.group_name, "category": g.category, "quantity": g.quantity})

    # Copy products
    products = orig_conn.execute(text("SELECT * FROM products WHERE id = ANY(:ids)"), {"ids": top_product_ids}).fetchall()
    for p in products:
        trim_conn.execute(text("""
            INSERT INTO products (id, product_name_clean, product_group_id)
            VALUES (:id, :name, :group_id)
        """), {"id": p.id, "name": p.product_name_clean, "group_id": p.product_group_id})

    # Copy retailers
    retailer_ids = orig_conn.execute(text("""
        SELECT DISTINCT retailer_id FROM product_listings WHERE product_id = ANY(:ids)
    """), {"ids": top_product_ids}).fetchall()
    retailer_ids = [r[0] for r in retailer_ids]
    for rid in retailer_ids:
        r = orig_conn.execute(text("SELECT * FROM retailers WHERE id=:id"), {"id": rid}).fetchone()
        trim_conn.execute(text("""
            INSERT INTO retailers (id, name) VALUES (:id, :name) ON CONFLICT DO NOTHING
        """), {"id": r.id, "name": r.name})

    # Copy product_listings for selected products
    listing_ids = []
    for pid in top_product_ids:
        listings = orig_conn.execute(text("SELECT * FROM product_listings WHERE product_id=:pid"), {"pid": pid}).fetchall()
        for l in listings:
            trim_conn.execute(text("""
                INSERT INTO product_listings
                    (id, product_id, retailer_id, original_name, own_brand, category)
                VALUES (:id, :product_id, :retailer_id, :original_name, :own_brand, :category)
            """), {
                "id": l.id,
                "product_id": l.product_id,
                "retailer_id": l.retailer_id,
                "original_name": l.original_name,
                "own_brand": l.own_brand,
                "category": l.category
            })
            listing_ids.append(l.id)

    # Copy prices
    for lid in listing_ids:
        prices = orig_conn.execute(text("SELECT * FROM prices WHERE listing_id=:lid"), {"lid": lid}).fetchall()
        for price in prices:
            trim_conn.execute(text("""
                INSERT INTO prices (id, listing_id, date, price, unit_price)
                VALUES (:id, :listing_id, :date, :price, :unit_price)
            """), {"id": price.id, "listing_id": price.listing_id, "date": price.date, "price": price.price, "unit_price": price.unit_price})

print("Trimmed database creation complete!")
orig_session.close()
trim_session.close()