"""
example_quickstart.py — Copy into a UKB RAP Jupyter notebook cell-by-cell.

Requirements:
    - JupyterLab with Spark cluster on UKB RAP
    - Clone or pip install ukbraptools
    - polars: pip install polars --break-system-packages
"""

# %% Cell 1 — Setup (run once per session)
import sys
sys.path.insert(0, "..")  # if running from examples/ dir after git clone

from ukbraptools import quick_setup

sc, spark, participant, db = quick_setup()

# %% Cell 2 — Discover fields
# By UKB Showcase field ID
height_fields = participant.field_by_id(50)
print("Standing height fields:", height_fields)

# By keyword search
bmi_fields = participant.field_by_keyword("body mass")
print("BMI-related fields:", bmi_fields)

# %% Cell 3 — Retrieve participant data (Spark)
df = participant.get_data(
    field_name_dict={
        "person_id": "eid",
        "sex": "p31",
        "age_at_recruitment": "p21022",
        "bmi_baseline": "p21001_i0",
    },
    coding_values="replace",
)
df.show(5, truncate=False)
print(f"Total participants: {df.count()}")

# %% Cell 4 — Convert to Polars for local analysis
plf = participant.get_data_polars(
    field_name_dict={
        "person_id": "eid",
        "sex": "p31",
        "age_at_recruitment": "p21022",
        "bmi_baseline": "p21001_i0",
    },
    coding_values="replace",
)
print(plf.describe())

# Or convert an existing Spark DataFrame
from ukbraptools import spark_to_polars

plf2 = spark_to_polars(df)
print(plf2.head())

# %% Cell 5 — SQL query on record tables
icd_diabetes = db.get_query("""
    SELECT eid, diag_icd10, epistart
    FROM hesin_diag
    WHERE diag_icd10 LIKE 'E11%'
    LIMIT 100
""")
icd_diabetes.show(10)

# %% Cell 6 — List all available tables
all_tables = db.list_all_tables()
print(f"Available tables ({len(all_tables)}):")
for t in sorted(all_tables):
    print(f"  {t}")

# %% Cell 7 — List all entities in the dataset
entities = participant.list_entities()
print("Dataset entities:", entities)

# %% Cell 8 — File operations (upload results)
from ukbraptools import FileTools

ft = FileTools()

# Save Polars DataFrame and upload
plf.write_csv("demo_phenotypes.csv")
file_id = ft.upload("demo_phenotypes.csv", folder="/results")
print(f"Uploaded as: {file_id}")
