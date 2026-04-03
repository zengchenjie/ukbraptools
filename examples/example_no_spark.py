"""
example_no_spark.py — Extract UKB data without a Spark cluster.

This approach uses `dx extract_dataset` under the hood and works on any
JupyterLab instance (even the cheapest single-node without Spark).

Requirements:
    - polars: pip install polars --break-system-packages

Recommended for:
    - Small-to-medium extractions (≲ 50 fields)
    - Quick phenotype pulls before spinning up a Spark cluster
    - Generating data dictionaries
"""

# %% Cell 1 — Setup
import sys
sys.path.insert(0, "..")

from ukbraptools import extract_fields_cli, extract_data_dictionary

# %% Cell 2 — Generate the data dictionary (entity, field, coding tables)
dict_files = extract_data_dictionary()
print("Generated:", dict_files)

# Inspect the data dictionary
import polars as pl

dd = pl.read_csv([f for f in dict_files if "data_dictionary" in f][0])
print(f"Total fields available: {dd.height}")
print(dd.head(10))

# %% Cell 3 — Extract specific fields to CSV
extract_fields_cli(
    fields=["eid", "p31", "p21022", "p21001_i0", "p21001_i1"],
    output_path="basic_phenotypes.csv",
)

df = pl.read_csv("basic_phenotypes.csv")
print(df.shape)
print(df.head())

# %% Cell 4 — Extract with original UKB-style headers (e.g. "21022-0.0")
extract_fields_cli(
    fields=["eid", "p31", "p21022"],
    output_path="phenotypes_ukb_headers.csv",
    header_style="UKB-FORMAT",
)

df2 = pl.read_csv("phenotypes_ukb_headers.csv")
print(df2.columns)

# %% Cell 5 — Extract from a non-participant entity
# For record-level data (hesin, gp_clinical, etc.), specify the entity:
extract_fields_cli(
    fields=["eid", "data_provider", "event_dt", "read_2", "read_3"],
    entity="gp_clinical",
    output_path="gp_clinical_extract.csv",
)

gp = pl.read_csv("gp_clinical_extract.csv")
print(f"GP clinical records: {gp.height}")
print(gp.head())
