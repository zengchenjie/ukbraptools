# ukbraptools — Updated Utilities for UK Biobank RAP

Modernised rewrite of [nhgritctran/ukbutils](https://github.com/nhgritctran/ukbutils), updated for the current DNAnexus UKB Research Analysis Platform environment (2025–2026).

## What Changed from the Original

| Issue in original `datatools.py` | Fix in `ukbraptools.py` |
|---|---|
| `distutils.version.LooseVersion` — removed in Python 3.12+ (RAP cluster images now ship 3.12) | Custom `_natural_sort_key()` using regex-based natural sort |
| `dxdata.connect()` raises `ValueError: ca_certs is needed …` on Spark cluster ≥ v2.5.0 (July 2025) | `_safe_connect()` uses `dialect="hive+pyspark"` with automatic fallback |
| Hard-coded `dxpy.find_one_data_object` for dataset discovery only | Auto-discovery via `DX_PROJECT_CONTEXT_ID` env var + `dx` CLI, matching the official UKB-RAP notebook patterns |
| No non-Spark extraction path | `extract_fields_cli()` wraps `dx extract_dataset` for lightweight instances |
| No file upload/download helpers | `FileTools` class with `upload()`, `download()`, `find_files()` |
| No Polars conversion | `get_data_polars()`, `spark_to_polars()`, `spark_to_csv()` |
| No access to non-participant entities (OMOP, hesin, GP, death, etc.) | `Participant.get_entity()` and `Participant.list_entities()` |
| No coding value replacement | `coding_values="replace"` parameter support |
| No type hints or docstrings | Full type hints + NumPy-style docstrings |
| `SparkContext()` creates new context each time | Uses `SparkContext.getOrCreate()` to be safe across notebooks |
| No data dictionary generation | `extract_data_dictionary()` produces the three standard CSVs |

## Setup

On a UKB RAP JupyterLab instance (Spark cluster recommended for `dxdata` operations):

**Option A — Clone and import (no install):**

```bash
!git clone https://github.com/zengchenjie/ukbraptools
```

```python
import sys; sys.path.insert(0, "ukbutils")
from ukbraptools import Participant, Database, quick_setup
```

**Option B — pip install (editable):**

```bash
!pip install -e /path/to/ukbutils --break-system-packages
```

```python
from ukbraptools import Participant, Database, FileTools
from ukbraptools import extract_fields_cli, extract_data_dictionary, quick_setup
```

No extra dependencies needed — `dxpy`, `dxdata`, and `pyspark` are all pre-installed on RAP.

## Quick Start

### One-liner setup

```python
sc, spark, participant, db = quick_setup()
```

### Discover fields

```python
p = Participant()

# By UKB Showcase field ID
p.field_by_id(50)        # Standing height
p.field_by_id(21001)     # BMI

# By keyword
p.field_by_keyword("cholesterol")

# By regex
p.field_by_title_regex(r"blood pressure.*instance 0")
```

### Retrieve data (Spark)

```python
df = p.get_data(
    field_name_dict={
        "person_id": "eid",
        "sex": "p31",
        "bmi": "p21001_i0",
        "age": "p21022",
    },
    coding_values="replace",
)
df.show(5)
```

### Retrieve data (Polars)

```python
plf = p.get_data_polars(
    field_name_dict={"person_id": "eid", "sex": "p31", "age": "p21022"},
    coding_values="replace",
)
print(plf.head())
```

### SQL queries on record tables

```python
db = Database()

# List tables containing "gp"
db.table_names_by_keyword("gp")

# Direct SQL
icd_df = db.get_query("""
    SELECT eid, diag_icd10
    FROM hesin_diag
    WHERE diag_icd10 LIKE 'E11%'
""")
```

### Extract without Spark (CLI)

```python
# Works on lightweight (non-Spark) instances too
extract_fields_cli(
    fields=["eid", "p31", "p21022", "p21001_i0"],
    output_path="phenotypes.csv",
)

# Generate the full data dictionary
extract_data_dictionary()
```

### Access OMOP and other entities

```python
p = Participant()
print(p.list_entities())      # ['participant', 'hesin', 'gp_clinical', ...]
gp = p.get_entity("gp_clinical")
```

### File management

```python
ft = FileTools()

# Upload results
ft.upload("results.csv", folder="/results")

# Find files
beds = ft.find_files("*.bed", folder="/Bulk/Exome")

# Download
ft.download("file-XXXX", "local_copy.bed")
```

## Dependencies

Pre-installed on UKB RAP Spark cluster instances:

- `dxpy` — DNAnexus Python SDK
- `dxdata` — DNAnexus dataset access library
- `pyspark` — Apache Spark Python API

**Not** pre-installed (install once per session):

```bash
pip install polars --break-system-packages
```

## Key Compatibility Notes

1. **Spark cluster v2.5.0+** (deployed ~July 2025): The default `dxdata.connect()` call fails with an SSL error. This library's `_safe_connect()` automatically uses the `dialect="hive+pyspark"` workaround.

2. **Python 3.12+**: The `distutils` module was removed. The original code's use of `LooseVersion` for field sorting has been replaced with a regex-based natural sort.

3. **Multiple notebooks**: `SparkContext.getOrCreate()` is used instead of `SparkContext()` to avoid errors when re-running cells.

## References

- [DNAnexus Documentation](https://documentation.dnanexus.com)
- [UKB RAP Phenotype Access](https://dnanexus.gitbook.io/uk-biobank-rap/working-on-the-research-analysis-platform/accessing-data/accessing-phenotypic-data)
- [UKB RAP Official Notebooks](https://github.com/UK-Biobank/UKB-RAP-Notebooks-Access)
- [DNAnexus OpenBio Notebooks](https://github.com/dnanexus/OpenBio)
- [Spark v2.5 dxdata.connect() fix](https://community.ukbiobank.ac.uk/hc/en-gb/community/posts/28602153129245)
- [Original ukbutils](https://github.com/nhgritctran/ukbutils)

## License

MIT
