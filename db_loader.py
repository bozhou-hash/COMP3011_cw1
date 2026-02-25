import psycopg2
import pandas as pd
from io import StringIO

conn = psycopg2.connect(
    dbname="supermarket_price_db",
    user="postgres",
    password="bozhou0211",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

df = pd.read_csv("full_dataset_with_unit_price.csv")

# Fix date
df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")

# ===============================
# INSERT RETAILERS
# ===============================

retailers = df["retailer"].unique()

for r in retailers:
    cur.execute(
        "INSERT INTO retailers (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
        (r,)
    )

conn.commit()
print("Retailers inserted.")

# ===============================
# COPY PRODUCTS
# ===============================

product_df = df[["product_name", "category"]].drop_duplicates()
product_df.columns = ["product_name_clean", "category"]

buffer = StringIO()
product_df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cur.copy_expert(
    """
    COPY products(product_name_clean, category)
    FROM STDIN WITH CSV
    """,
    buffer
)

conn.commit()
print("Products copied.")

# Build retailer map
cur.execute("SELECT id, name FROM retailers")
retailer_map = dict(cur.fetchall())

# Build product map
cur.execute("SELECT id, product_name_clean FROM products")
product_map = dict(cur.fetchall())

print("Lookup maps built.")

listing_df = df[["product_name", "retailer", "own_brand"]].drop_duplicates()

listing_df["product_id"] = listing_df["product_name"].map(product_map)
listing_df["retailer_id"] = listing_df["retailer"].map(retailer_map)

listing_df = listing_df[["product_id", "retailer_id", "product_name", "own_brand"]]
listing_df.columns = ["product_id", "retailer_id", "original_name", "own_brand"]

buffer = StringIO()
listing_df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cur.copy_expert(
    """
    COPY product_listings(product_id, retailer_id, original_name, own_brand)
    FROM STDIN WITH CSV
    """,
    buffer
)

conn.commit()
print("Listings copied.")

cur.execute("""
SELECT pl.id, p.product_name_clean, r.name
FROM product_listings pl
JOIN products p ON pl.product_id = p.id
JOIN retailers r ON pl.retailer_id = r.id
""")

listing_map = {
    (product, retailer): listing_id
    for listing_id, product, retailer in cur.fetchall()
}

print("Listing map ready.")

price_df = df.copy()

price_df["listing_id"] = price_df.apply(
    lambda x: listing_map.get((x["product_name"], x["retailer"])),
    axis=1
)

price_df = price_df[["listing_id", "date", "price", "unit_price_computed"]]
price_df.columns = ["listing_id", "date", "price", "unit_price"]

buffer = StringIO()
price_df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cur.copy_expert(
    """
    COPY prices(listing_id, date, price, unit_price)
    FROM STDIN WITH CSV
    """,
    buffer
)

conn.commit()
print("Prices copied successfully.")