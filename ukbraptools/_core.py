"""
ukbraptools.py — Updated utilities for UK Biobank Research Analysis Platform (RAP).

Modernised rewrite of nhgritctran/ukbutils (datatools.py), incorporating:
  - Spark cluster v2.5+ compatibility (dialect="hive+pyspark" workaround)
  - Python 3.12+ compatibility (replaces deprecated distutils.version.LooseVersion)
  - Automatic project / dataset / record discovery via env vars + dx CLI
  - Non-Spark data extraction via `dx extract_dataset`
  - OMOP entity access alongside participant data
  - File upload / download helpers via dxpy
  - Polars conversion utilities (Spark → Polars)
  - Type hints and structured error handling

Setup on UKB RAP JupyterLab (Spark cluster):
    !git clone https://github.com/zengchenjie/ukbraptools
    from ukbraptools import Participant, Database, FileTools, extract_fields_cli

Dependencies: dxpy, dxdata, pyspark — all pre-installed on UKB RAP Spark instances.

References:
    - DNAnexus docs: https://documentation.dnanexus.com
    - UKB RAP phenotype access: https://dnanexus.gitbook.io/uk-biobank-rap/
    - UKB RAP notebooks: https://github.com/UK-Biobank/UKB-RAP-Notebooks-Access
    - Spark cluster v2.5 dxdata.connect() fix:
        https://community.ukbiobank.ac.uk/hc/en-gb/community/posts/28602153129245
    - Original ukbutils: https://github.com/nhgritctran/ukbutils

Author: updated from nhgritctran/ukbutils
License: MIT
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _natural_sort_key(name: str):
    """
    Sort field names in natural order (p50_i0, p50_i1, p50_i10, ...).
    Replaces the deprecated distutils.version.LooseVersion which was removed
    in Python 3.12+ on the updated RAP Spark cluster images.
    """
    return [
        int(tok) if tok.isdigit() else tok.lower()
        for tok in re.split(r"(\d+)", name)
    ]


def _get_project_id() -> str:
    """Return the current DNAnexus project ID from the environment."""
    project = os.getenv("DX_PROJECT_CONTEXT_ID")
    if project:
        return project
    # Fallback: parse `dx env`
    try:
        out = subprocess.check_output(
            "dx env | grep project- | awk -F'\\t' '{print $2}'",
            shell=True,
            text=True,
        ).strip()
        if out:
            return out
    except subprocess.CalledProcessError:
        pass
    raise RuntimeError(
        "Cannot determine project ID. "
        "Ensure DX_PROJECT_CONTEXT_ID is set or `dx` CLI is configured."
    )


def _get_record_id() -> str:
    """Find the dispensed dataset record ID in the current project."""
    try:
        out = subprocess.check_output(
            "dx find data --type Dataset --delimiter ',' "
            "| sort -t ',' -k2,2r | head -n1 | awk -F ',' '{print $5}'",
            shell=True,
            text=True,
        ).strip()
        if out:
            return out
    except subprocess.CalledProcessError:
        pass
    raise RuntimeError(
        "Cannot find a dispensed Dataset record in the current project."
    )


def _get_dataset_id() -> str:
    """Return 'project-XXX:record-XXX' for dxdata.load_dataset()."""
    return f"{_get_project_id()}:{_get_record_id()}"


# ---------------------------------------------------------------------------
# Participant — phenotype field discovery and retrieval via dxdata
# ---------------------------------------------------------------------------

class Participant:
    """
    High-level access to the UKB participant dataset via dxdata.

    Usage::

        p = Participant()
        p.field_by_id(50)       # Standing height
        p.field_by_keyword("cholesterol")
        df = p.get_data({"person_id": "eid", "sex": "p31"})
    """

    def __init__(self, dataset_id: Optional[str] = None):
        import dxdata

        self._dataset_id = dataset_id or _get_dataset_id()
        dataset = dxdata.load_dataset(id=self._dataset_id)
        self.participant = dataset["participant"]
        self._dataset = dataset

    # ---- field discovery --------------------------------------------------

    def field_by_id(self, field_id: Union[int, str]) -> Dict[str, str]:
        """
        Look up field(s) by UKB Showcase numeric ID.

        Returns ``{title: spark_field_name}`` for all instances/arrays of
        the requested field, sorted in natural order.

        Example::

            p.field_by_id(50)
            # {'Standing height | Instance 0': 'p50_i0', ...}
        """
        field_id = str(field_id)
        fields = self.participant.find_fields(
            name_regex=rf"^p{field_id}(_i\d+)?(_a\d+)?$"
        )
        fields = sorted(fields, key=lambda f: _natural_sort_key(f.name))
        return {f.title: f.name for f in fields}

    def field_by_keyword(self, keyword: str) -> Dict[str, str]:
        """
        Search for fields whose title contains *keyword* (case-insensitive).

        Returns ``{title: spark_field_name}`` sorted naturally.
        """
        fields = list(
            self.participant.find_fields(
                lambda f: keyword.lower() in f.title.lower()
            )
        )
        fields = sorted(fields, key=lambda f: _natural_sort_key(f.name))
        return {f.title: f.name for f in fields}

    def field_by_title(self, title: str):
        """
        Look up a single field by its exact title string.
        Wraps ``participant.find_field(title=...)``.
        """
        return self.participant.find_field(title=title)

    def field_by_title_regex(self, pattern: str) -> Dict[str, str]:
        """
        Search for fields whose title matches a regex pattern.
        """
        compiled = re.compile(pattern, re.IGNORECASE)
        fields = list(
            self.participant.find_fields(
                lambda f: compiled.search(f.title)
            )
        )
        fields = sorted(fields, key=lambda f: _natural_sort_key(f.name))
        return {f.title: f.name for f in fields}

    # ---- data retrieval ---------------------------------------------------

    def get_data(
        self,
        field_name_dict: Dict[str, str],
        eid_list: Optional[List] = None,
        coding_values: Optional[str] = None,
        year_of_last_event_data=None,
    ):
        """
        Retrieve participant data as a Spark DataFrame.

        Parameters
        ----------
        field_name_dict : dict
            ``{desired_column_name: spark_field_name}``.
            Example: ``{"person_id": "eid", "sex": "p31"}``
        eid_list : list, optional
            Restrict to these participant EIDs.
        coding_values : str, optional
            Pass ``"replace"`` to decode coded values to their labels.
        year_of_last_event_data : Spark DataFrame, optional
            Output of ``Database.get_year_of_last_event_data()``.
            When provided, an ``age_at_last_event`` column is appended.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        engine = _safe_connect()

        retrieve_kwargs: Dict[str, Any] = dict(
            names=list(field_name_dict.values()),
            engine=engine,
        )
        if coding_values:
            retrieve_kwargs["coding_values"] = coding_values

        spark_df = self.participant.retrieve_fields(**retrieve_kwargs)

        if eid_list:
            spark_df = spark_df.filter(spark_df["eid"].isin(eid_list))

        spark_df = spark_df.toDF(*list(field_name_dict.keys()))

        if year_of_last_event_data is not None:
            import pyspark.sql.functions as fx

            spark_df = spark_df.join(
                year_of_last_event_data,
                spark_df["person_id"] == year_of_last_event_data["eid"],
                "left",
            )
            spark_df = spark_df.withColumn(
                "age_at_last_event",
                spark_df["year_of_last_event"] - spark_df["year_of_birth"],
            )
            cols = list(field_name_dict.keys()) + ["age_at_last_event"]
            spark_df = spark_df.select(*cols)

        return spark_df

    def get_data_polars(
        self,
        field_name_dict: Dict[str, str],
        eid_list: Optional[List] = None,
        coding_values: Optional[str] = None,
    ):
        """
        Convenience wrapper: retrieve fields and convert to a Polars DataFrame.

        Internally goes Spark → Pandas → Polars.  Suitable for datasets
        that fit in driver memory.
        """
        import polars as pl

        spark_df = self.get_data(
            field_name_dict=field_name_dict,
            eid_list=eid_list,
            coding_values=coding_values,
        )
        return pl.from_pandas(spark_df.toPandas())

    # ---- entity access ----------------------------------------------------

    def get_entity(self, entity_name: str):
        """
        Access any entity in the dispensed dataset (e.g. ``"hesin"``,
        ``"gp_clinical"``, ``"death"``, ``"covid19_result_england"``).
        """
        return self._dataset[entity_name]

    def list_entities(self) -> List[str]:
        """List all entity names in the dispensed dataset."""
        return [e.name for e in self._dataset.entities]


# ---------------------------------------------------------------------------
# Database — SQL access to all tables via Spark
# ---------------------------------------------------------------------------

class Database:
    """
    Direct SQL interface to UKB RAP dispensed database tables.

    Usage::

        db = Database()
        db.table_names_by_keyword("gp")
        gp = db.get_table_by_name("gp_clinical")
        df = db.get_query("SELECT COUNT(*) FROM gp_clinical")
    """

    def __init__(self, database_name: Optional[str] = None):
        import pyspark

        self.sc = pyspark.SparkContext.getOrCreate()
        self.spark = pyspark.sql.SparkSession(self.sc)

        db_name = database_name or self._discover_database()
        self.spark.sql(f"USE {db_name}")
        self._db_name = db_name
        logger.info("Using database: %s", db_name)

    @staticmethod
    def _discover_database() -> str:
        """Auto-discover the dispensed database name via dxpy."""
        import dxpy

        result = dxpy.find_one_data_object(
            classname="database",
            name="app*",
            folder="/",
            name_mode="glob",
            describe=True,
        )
        return result["describe"]["name"]

    # ---- table discovery --------------------------------------------------

    def table_names_by_keyword(self, keyword: str) -> List[str]:
        """Return table names containing *keyword*."""
        tables = self.get_query("SHOW TABLES")
        filtered = tables.filter(
            tables["tableName"].contains(str(keyword).lower())
        )
        return [row[0] for row in filtered.select("tableName").collect()]

    def list_all_tables(self) -> List[str]:
        """Return every table name in the database."""
        tables = self.get_query("SHOW TABLES")
        return [row[0] for row in tables.select("tableName").collect()]

    # ---- data access ------------------------------------------------------

    def get_table_by_name(self, table_name: str):
        """
        Return a full Spark DataFrame for *table_name*.

        .. note:: This pulls the entire table — filter afterwards or use
           ``get_query()`` with a WHERE clause for large tables.
        """
        return self.get_query(f"SELECT * FROM {table_name}")

    def get_query(self, query: str):
        """Execute a SQL query and return a Spark DataFrame."""
        return self.spark.sql(query)

    # ---- convenience methods ----------------------------------------------

    def get_year_of_last_event_data(self, table: str = "gp_clinical"):
        """
        Compute the year of last event per participant from a record table.

        Parameters
        ----------
        table : str
            Any table with ``eid`` and ``event_dt`` columns
            (default ``"gp_clinical"``).

        Returns
        -------
        Spark DataFrame with columns ``[eid, year_of_last_event]``
        """
        import pyspark.sql.functions as fx

        df = self.get_table_by_name(table)
        return (
            df.groupby("eid")
            .agg(fx.max(fx.year("event_dt")))
            .toDF(*["eid", "year_of_last_event"])
        )

    def get_hesin_summary(self):
        """
        Quick summary of hospital episode statistics (hesin table).
        Returns eid, admission count, date range.
        """
        import pyspark.sql.functions as fx

        hesin = self.get_table_by_name("hesin")
        return (
            hesin.groupby("eid")
            .agg(
                fx.count("*").alias("admission_count"),
                fx.min("epistart").alias("first_admission"),
                fx.max("epistart").alias("last_admission"),
            )
        )


# ---------------------------------------------------------------------------
# _safe_connect — resilient dxdata.connect() for Spark ≥ v2.5
# ---------------------------------------------------------------------------

def _safe_connect():
    """
    Create a dxdata engine connection with automatic fallback.

    On Spark cluster v2.5.0+ (deployed ~July 2025), the default
    ``dxdata.connect()`` raises ``ValueError: ca_certs is needed when
    cert_reqs is not ssl.CERT_NONE``. The workaround is to explicitly
    request the ``"hive+pyspark"`` dialect.

    This function tries the robust dialect first, then falls back to the
    bare ``dxdata.connect()`` for older cluster versions.

    See: https://community.ukbiobank.ac.uk/hc/en-gb/community/posts/28602153129245
    """
    import dxdata

    try:
        return dxdata.connect(dialect="hive+pyspark")
    except TypeError:
        # Older dxdata versions may not accept the dialect keyword
        logger.info("Falling back to dxdata.connect() without dialect arg.")
        return dxdata.connect()


# ---------------------------------------------------------------------------
# FileTools — upload / download helpers
# ---------------------------------------------------------------------------

class FileTools:
    """
    Convenience wrappers around ``dxpy`` for file operations.

    Usage::

        ft = FileTools()
        ft.upload("local_results.csv", folder="/results")
        ft.download("file-XXXX", "local_copy.csv")
        ft.find_files("*.bed", folder="/Bulk/Exome")
    """

    def __init__(self, project_id: Optional[str] = None):
        self._project = project_id or _get_project_id()

    def upload(
        self,
        local_path: str,
        folder: str = "/",
        name: Optional[str] = None,
        wait_on_close: bool = True,
    ) -> str:
        """
        Upload a local file to the current project.

        Returns the file ID (``file-XXXX``).
        """
        import dxpy

        remote = dxpy.upload_local_file(
            local_path,
            project=self._project,
            folder=folder,
            name=name,
            wait_on_close=wait_on_close,
        )
        file_id = remote.get_id()
        logger.info("Uploaded %s → %s:%s/%s", local_path, self._project, folder, file_id)
        return file_id

    def download(self, file_id: str, local_path: str) -> str:
        """
        Download a platform file to a local path.

        *file_id* can be ``"file-XXXX"`` or ``"project-XXX:file-XXXX"``.
        Returns the local path.
        """
        import dxpy

        dxpy.download_dxfile(file_id, local_path, project=self._project)
        logger.info("Downloaded %s → %s", file_id, local_path)
        return local_path

    def find_files(
        self,
        name_pattern: str,
        folder: Optional[str] = None,
        classname: str = "file",
    ) -> List[Dict[str, str]]:
        """
        Search for files matching a glob pattern.

        Returns list of ``{"id": "file-XXX", "name": "..."}`` dicts.
        """
        import dxpy

        kwargs: Dict[str, Any] = dict(
            name=name_pattern,
            name_mode="glob",
            classname=classname,
            project=self._project,
            describe=True,
        )
        if folder:
            kwargs["folder"] = folder

        results = []
        for item in dxpy.find_data_objects(**kwargs):
            results.append(
                {
                    "id": item["id"],
                    "name": item["describe"]["name"],
                    "folder": item["describe"].get("folder", "/"),
                }
            )
        return results


# ---------------------------------------------------------------------------
# CLI-based extraction (no Spark required)
# ---------------------------------------------------------------------------

def extract_fields_cli(
    fields: List[str],
    output_path: str = "extracted_data.csv",
    entity: str = "participant",
    delimiter: str = ",",
    header_style: Optional[str] = None,
    dataset_id: Optional[str] = None,
) -> str:
    """
    Extract fields via ``dx extract_dataset`` — no Spark cluster needed.

    This runs the dx CLI command which works from any JupyterLab terminal
    or even a lightweight (non-Spark) instance. Recommended by UKB for
    small-to-medium extractions (≲ 50 fields).

    Parameters
    ----------
    fields : list of str
        Field names, e.g. ``["participant.eid", "participant.p31",
        "participant.p21022"]``. The ``"participant."`` prefix is added
        automatically if missing.
    output_path : str
        Local output file path.
    entity : str
        Entity prefix (default ``"participant"``).
    delimiter : str
        Column delimiter (default ``","``).
    header_style : str, optional
        ``"UKB-FORMAT"`` for original UKB-style headers (e.g. ``123-4.5``),
        or ``None`` for Spark-style names (e.g. ``p123_i4_a5``).
    dataset_id : str, optional
        Explicit ``"project-XXX:record-XXX"``; auto-discovered if omitted.

    Returns
    -------
    str
        The output file path.

    Example
    -------
    ::

        extract_fields_cli(
            fields=["eid", "p31", "p21001_i0", "p21022"],
            output_path="phenotypes.csv",
        )
    """
    ds = dataset_id or _get_dataset_id()

    # Ensure entity prefix
    prefixed = []
    for f in fields:
        if "." not in f:
            prefixed.append(f"{entity}.{f}")
        else:
            prefixed.append(f)

    cmd = [
        "dx", "extract_dataset", ds,
        "--fields", ",".join(prefixed),
        "--delimiter", delimiter,
        "--output", output_path,
    ]
    if header_style:
        cmd.extend(["--header-style", header_style])

    logger.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)
    logger.info("Extracted %d fields → %s", len(fields), output_path)
    return output_path


def extract_data_dictionary(
    output_prefix: Optional[str] = None,
    dataset_id: Optional[str] = None,
    delimiter: str = ",",
) -> List[str]:
    """
    Generate the data dictionary CSVs for the dispensed dataset.

    Produces three files:
      - ``<prefix>.entity_dictionary.csv``
      - ``<prefix>.data_dictionary.csv``
      - ``<prefix>.coding_dictionary.csv``

    Parameters
    ----------
    output_prefix : str, optional
        Prefix for output files. If None, uses the dataset record name.
    dataset_id : str, optional
        Explicit ``"project-XXX:record-XXX"``; auto-discovered if omitted.
    delimiter : str
        Column delimiter.

    Returns
    -------
    list of str
        Paths to the three generated files.
    """
    ds = dataset_id or _get_dataset_id()

    cmd = ["dx", "extract_dataset", ds, "-ddd", "--delimiter", delimiter]
    logger.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)

    # The command generates files in the current directory based on the
    # dataset descriptor; list them.
    generated = [
        f
        for f in os.listdir(".")
        if f.endswith(
            (
                ".entity_dictionary.csv",
                ".data_dictionary.csv",
                ".coding_dictionary.csv",
            )
        )
    ]
    logger.info("Generated data dictionary files: %s", generated)
    return sorted(generated)


# ---------------------------------------------------------------------------
# Spark ↔ Polars convenience
# ---------------------------------------------------------------------------

def spark_to_polars(spark_df, n_rows: Optional[int] = None):
    """
    Convert a Spark DataFrame to a Polars DataFrame, with optional row limit.

    Internally bridges through Pandas (``toPandas()``), then converts to
    Polars via ``pl.from_pandas()``.  For very large tables, pass *n_rows*
    to avoid OOM on the driver.
    """
    import polars as pl

    if n_rows is not None:
        return pl.from_pandas(spark_df.limit(n_rows).toPandas())
    return pl.from_pandas(spark_df.toPandas())


def spark_to_csv(
    spark_df,
    path: str,
    n_rows: Optional[int] = None,
):
    """Convert Spark DataFrame → Polars → CSV in one call."""
    plf = spark_to_polars(spark_df, n_rows=n_rows)
    plf.write_csv(path)
    logger.info("Wrote %d rows to %s", plf.height, path)
    return path


# ---------------------------------------------------------------------------
# Quick-start helpers
# ---------------------------------------------------------------------------

def init_spark():
    """
    Initialise a PySpark context and session.

    Call once per notebook/session. Returns ``(sc, spark)``.

    .. warning:: Do not call this cell more than once. If you need to
       restart, use *Kernel → Restart*.
    """
    import pyspark

    sc = pyspark.SparkContext.getOrCreate()
    spark = pyspark.sql.SparkSession(sc)
    return sc, spark


def quick_setup():
    """
    All-in-one setup: init Spark, connect to the dispensed dataset, and
    return ``(sc, spark, participant, database)``.

    Usage::

        sc, spark, participant, db = quick_setup()
    """
    sc, spark = init_spark()
    p = Participant()
    db = Database()
    return sc, spark, p, db
