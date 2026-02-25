import pandas as pd
import os
import numpy as np

# =====================================
# 1. LOAD ALL FILES
# =====================================

folder_path = r"C:\Users\User\Downloads\archive"

files = [
    "All_Data_Tesco.csv",
    "All_Data_Sains.csv",
    "All_Data_Morrisons.csv",
    "All_Data_Aldi.csv",
    "All_Data_ASDA.csv"
]

dataframes = []

for filename in files:
    path = os.path.join(folder_path, filename)
    print(f"Loading {filename}...")

    df = pd.read_csv(path, low_memory=False)
    dataframes.append(df)

df_all = pd.concat(dataframes, ignore_index=True)

print("Total rows loaded:", len(df_all))


# =====================================
# 2. CLEAN & STANDARDISE COLUMN NAMES
# =====================================

df_all.columns = (
    df_all.columns
    .str.strip()
    .str.lower()
)

print("Original columns:")
print(df_all.columns.tolist())

# Rename using EXACT column names
df_all.rename(columns={
    "supermarket": "retailer",
    "names": "product_name",
    "prices_(£)": "price",
    "prices_unit_(£)": "price_per_unit"
}, inplace=True)

print("Renamed columns:")
print(df_all.columns.tolist())


# =====================================
# 3. CLEAN PRICE DATA
# =====================================

df_all["price"] = pd.to_numeric(df_all["price"], errors="coerce")
df_all["price_per_unit"] = pd.to_numeric(df_all["price_per_unit"], errors="coerce")

df_all = df_all.dropna(subset=["product_name", "price"])

print("After dropping invalid rows:", len(df_all))


# =====================================
# 4. EXTRACT UNIQUE PRODUCTS
# =====================================

unique_products = df_all[["product_name", "retailer", "category"]].drop_duplicates()

print("Unique product-retailer combinations:", len(unique_products))


# =====================================
# 5. EXTRACT QUANTITY FROM NAME (VECTORISED)
# =====================================

pattern = r'(\d+(?:\.\d+)?)\s?(kg|g|ml|l|pk|pack|bags|ct|pcs)'

extracted = unique_products["product_name"].str.lower().str.extract(pattern)

unique_products["quantity"] = pd.to_numeric(extracted[0], errors="coerce")
unique_products["unit_extracted"] = extracted[1]


# =====================================
# 6. STANDARDISE UNITS
# =====================================

unique_products["standard_quantity"] = np.nan

# kg → grams
mask = unique_products["unit_extracted"] == "kg"
unique_products.loc[mask, "standard_quantity"] = (
    unique_products.loc[mask, "quantity"] * 1000
)

# g
mask = unique_products["unit_extracted"] == "g"
unique_products.loc[mask, "standard_quantity"] = unique_products.loc[mask, "quantity"]

# l → ml
mask = unique_products["unit_extracted"] == "l"
unique_products.loc[mask, "standard_quantity"] = (
    unique_products.loc[mask, "quantity"] * 1000
)

# ml
mask = unique_products["unit_extracted"] == "ml"
unique_products.loc[mask, "standard_quantity"] = unique_products.loc[mask, "quantity"]

# count-based units
mask = unique_products["unit_extracted"].isin(["pk", "pack", "bags", "ct", "pcs"])
unique_products.loc[mask, "standard_quantity"] = unique_products.loc[mask, "quantity"]

print("Unit extraction complete.")


# =====================================
# 7. MERGE BACK INTO MAIN DATASET
# =====================================

df_all = df_all.merge(
    unique_products[["product_name", "retailer", "standard_quantity"]],
    on=["product_name", "retailer"],
    how="left"
)


# =====================================
# 8. COMPUTE UNIT PRICE (IF POSSIBLE)
# =====================================

df_all["unit_price_computed"] = np.where(
    df_all["standard_quantity"].notna(),
    df_all["price"] / df_all["standard_quantity"],
    np.nan
)

print("Unit price computed.")


# =====================================
# 9. SAVE OUTPUT FILES
# =====================================

unique_products.to_csv("unique_products_cleaned.csv", index=False)
df_all.to_csv("full_dataset_with_unit_price.csv", index=False)

print("Processing complete.")
print("Files saved successfully.")