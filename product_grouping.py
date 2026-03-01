import psycopg2
import pandas as pd
import re
from rapidfuzz import fuzz
from io import StringIO

# ======================================
# DATABASE CONNECTION
# ======================================

conn = psycopg2.connect(
    dbname="supermarket_price_db",
    user="postgres",
    password="bozhou0211",
    host="localhost",
    port="5432"
)

# ======================================
# LOAD DATA
# ======================================

query = """
SELECT DISTINCT p.id, p.product_name_clean, pl.category
FROM products p
JOIN product_listings pl ON pl.product_id = p.id
"""

df = pd.read_sql(query, conn)
print("Loaded products:", len(df))

# ======================================
# WORD FILTERS
# ======================================

RETAILER_WORDS = {
    "tesco", "aldi", "asda", "morrisons",
    "sainsbury", "sains", "lidl"
}

FILLER_WORDS = {
    "the", "and", "with", "for", "of",
    "new", "fresh", "british"
}

# Words to remove from display names (retailer branding etc.)
DISPLAY_REMOVE_WORDS = {
    "tesco", "aldi", "asda", "morrisons",
    "sainsbury", "sains", "lidl",
    "finest", "everyday", "essentials",
    "extra", "special", "choice",
    "own", "brand"
}

# ======================================
# QUANTITY EXTRACTION
# ======================================

def extract_quantity(text):
    pattern = r"(\d+\.?\d*\s?(g|kg|ml|l|cl|pk|pack|x\d+))"
    match = re.search(pattern, text.lower())
    return match.group(0) if match else None

# ======================================
# NORMALISATION
# ======================================

def normalize_name(name):

    name = name.lower()
    name = re.sub(r"[^\w\s]", " ", name)

    quantity = extract_quantity(name)

    if quantity:
        name = name.replace(quantity, "")

    tokens = name.split()

    tokens = [
        t for t in tokens
        if t not in RETAILER_WORDS
        and t not in FILLER_WORDS
    ]

    tokens = sorted(set(tokens))

    clean_name = " ".join(tokens).strip()

    return clean_name, quantity


df[["normalized", "quantity"]] = df["product_name_clean"].apply(
    lambda x: pd.Series(normalize_name(x))
)

# ======================================
# STAGE 1 — EXACT TOKEN FINGERPRINT
# ======================================

group_id = 1
assigned_groups = {}
fingerprint_groups = {}

for idx, row in df.iterrows():

    key = (
        row["category"],
        frozenset(row["normalized"].split()),
        row["quantity"]
    )

    if key not in fingerprint_groups:
        fingerprint_groups[key] = group_id
        group_id += 1

    assigned_groups[idx] = fingerprint_groups[key]

print("Groups after exact fingerprint:", len(fingerprint_groups))

# ======================================
# STAGE 2 — SMART TOKEN-BLOCKED MERGE
# ======================================

SIMILARITY_THRESHOLD = 90

# Build group representatives
group_lookup = {}
for idx, gid in assigned_groups.items():
    group_lookup.setdefault(gid, []).append(idx)

group_reps = {}

for gid, indices in group_lookup.items():
    rep_idx = indices[0]
    group_reps[gid] = {
        "name": df.loc[rep_idx, "normalized"],
        "category": df.loc[rep_idx, "category"],
        "quantity": df.loc[rep_idx, "quantity"],
        "tokens": set(df.loc[rep_idx, "normalized"].split())
    }

# Build inverted token index
token_index = {}

for gid, data in group_reps.items():
    for token in data["tokens"]:
        token_index.setdefault(token, set()).add(gid)

merged = {}

for gid1, data1 in group_reps.items():

    if gid1 in merged:
        continue

    candidate_groups = set()

    for token in data1["tokens"]:
        candidate_groups |= token_index.get(token, set())

    for gid2 in candidate_groups:

        if gid2 <= gid1:
            continue
        if gid2 in merged:
            continue

        data2 = group_reps[gid2]

        if data1["category"] != data2["category"]:
            continue
        if data1["quantity"] != data2["quantity"]:
            continue

        score = fuzz.token_set_ratio(data1["name"], data2["name"])

        if score >= SIMILARITY_THRESHOLD:
            merged[gid2] = gid1

# Apply merge chains
for idx, gid in assigned_groups.items():
    while gid in merged:
        gid = merged[gid]
    assigned_groups[idx] = gid

final_group_ids = set(assigned_groups.values())
print("Final groups after smart merge:", len(final_group_ids))

# ======================================
# GENERATE DISPLAY NAME FOR THE FINAL TABLE
# ======================================

def generate_display_name(names_series, quantity):

    combined = " ".join(names_series.tolist()).lower()
    combined = re.sub(r"[^\w\s]", " ", combined)

    tokens = combined.split()

    # Remove retailer branding
    tokens = [t for t in tokens if t not in DISPLAY_REMOVE_WORDS]

    # Count frequency
    token_freq = pd.Series(tokens).value_counts()

    # Keep most common meaningful tokens
    common_tokens = token_freq.head(6).index.tolist()

    name = " ".join(common_tokens)

    # Add quantity back if exists
    if quantity:
        name = f"{name} {quantity}"

    return name.title().strip()

# ======================================
# BUILD FINAL GROUP TABLE (GENERIC CLEAN NAMES)
# ======================================

group_records = []
group_id_map = {}
new_group_id = 1

for gid in final_group_ids:

    indices = [i for i, g in assigned_groups.items() if g == gid]

    group_names = df.loc[indices, "product_name_clean"]
    category = df.loc[indices[0], "category"]
    quantity = df.loc[indices[0], "quantity"]

    best_name = generate_display_name(group_names, quantity)

    group_id_map[gid] = new_group_id

    group_records.append({
        "id": new_group_id,
        "group_name": best_name,
        "category": category,
        "quantity": quantity
    })

    new_group_id += 1

print("Total groups to insert:", len(group_records))

# ======================================
# INSERT INTO product_groups
# ======================================

cur = conn.cursor()

cur.execute("TRUNCATE TABLE product_groups RESTART IDENTITY CASCADE;")
conn.commit()

group_df = pd.DataFrame(group_records)

buffer = StringIO()
group_df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cur.copy_expert(
    """
    COPY product_groups(id, group_name, category, quantity)
    FROM STDIN WITH CSV
    """,
    buffer
)

conn.commit()
print("Product groups inserted.")

# ======================================
# UPDATE PRODUCTS TABLE
# ======================================

update_df = df.copy()
update_df["product_group_id"] = update_df.index.map(
    lambda x: group_id_map[assigned_groups[x]]
)

update_df = update_df[["id", "product_group_id"]]
update_df["id"] = update_df["id"].astype(int)
update_df["product_group_id"] = update_df["product_group_id"].astype(int)

cur.execute("""
CREATE TEMP TABLE temp_group_update (
    id INTEGER,
    product_group_id INTEGER
);
""")
conn.commit()

buffer = StringIO()
update_df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cur.copy_expert(
    """
    COPY temp_group_update(id, product_group_id)
    FROM STDIN WITH CSV
    """,
    buffer
)

cur.execute("""
UPDATE products p
SET product_group_id = t.product_group_id
FROM temp_group_update t
WHERE p.id = t.id;
""")

conn.commit()

print("Products updated successfully.")

cur.close()
conn.close()