import psycopg2
import pandas as pd
import numpy as np
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

df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")

# ==================================
# INSERT RETAILERS
# ==================================

retailers = df["retailer"].dropna().unique()

for r in retailers:
    cur.execute(
        "INSERT INTO retailers (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
        (r,)
    )

conn.commit()
print("Retailers inserted.")

# ==================================
# COPY PRODUCTS
# ==================================

product_df = df[["product_name"]].drop_duplicates().copy()

product_df.columns = ["product_name_clean"]

# Optional but recommended: strip whitespace
product_df["product_name_clean"] = product_df["product_name_clean"].str.strip()

buffer = StringIO()
product_df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cur.copy_expert(
    """
    COPY products(product_name_clean)
    FROM STDIN WITH CSV
    """,
    buffer
)

conn.commit()
print("Products copied.")

# ==================================
# BUILD LOOKUP MAPS (FIXED)
# ==================================

cur.execute("SELECT id, name FROM retailers")
retailer_map = {name: id for id, name in cur.fetchall()}

cur.execute("SELECT id, product_name_clean FROM products")
product_map = {name: id for id, name in cur.fetchall()}

print("Lookup maps built.")

# ==================================
# INSERT LISTINGS
# ==================================

listing_df = df[["product_name", "retailer", "own_brand", "category"]].copy()

listing_df["product_id"] = listing_df["product_name"].map(product_map)
listing_df["retailer_id"] = listing_df["retailer"].map(retailer_map)

# Remove rows where mapping failed
listing_df = listing_df.dropna(subset=["product_id", "retailer_id"])

listing_df = listing_df.sort_values("category")  # optional stability
listing_df = listing_df.drop_duplicates(
    subset=["product_id", "retailer_id"],
    keep="first"
)

listing_df = listing_df[["product_id", "retailer_id", "product_name", "own_brand", "category"]]
listing_df.columns = ["product_id", "retailer_id", "original_name", "own_brand", "category"]

buffer = StringIO()
listing_df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cur.copy_expert(
    """
    COPY product_listings(product_id, retailer_id, original_name, own_brand, category)
    FROM STDIN WITH CSV
    """,
    buffer
)

conn.commit()
print("Listings copied.")

# ==================================
# BUILD LISTING MAP
# ==================================

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

# ==================================
# INSERT PRICES
# ==================================

price_df = df.copy()

# Build listing_id mapping
price_df["listing_id"] = price_df.apply(
    lambda x: listing_map.get((x["product_name"], x["retailer"])),
    axis=1
)

# Keep only required columns
price_df = price_df[[
    "listing_id",
    "date",
    "price",
    "unit_price_computed"
]].copy()

price_df.columns = ["listing_id", "date", "price", "unit_price"]

# ===============================
# CLEAN DATA
# ===============================

# Remove rows with missing required fields
price_df = price_df.dropna(subset=["listing_id", "date", "price"])

# Convert date properly (ensures correct format)
price_df["date"] = pd.to_datetime(price_df["date"]).dt.date

# Replace infinite values with NaN
price_df["unit_price"] = price_df["unit_price"].replace([np.inf, -np.inf], np.nan)

# Optional: round to 4 decimal places to match NUMERIC(10,4)
price_df["unit_price"] = price_df["unit_price"].round(4)
price_df["price"] = price_df["price"].round(4)

# Remove duplicate (listing_id, date)
price_df = price_df.sort_values("date")
price_df = price_df.drop_duplicates(
    subset=["listing_id", "date"],
    keep="last"
)

print("Final price rows to insert:", len(price_df))

# ===============================
# COPY TO POSTGRES
# ===============================

buffer = StringIO()
price_df.to_csv(buffer, index=False, header=False, na_rep="")
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

cur.close()
conn.close()