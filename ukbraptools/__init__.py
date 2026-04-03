"""
ukbraptools — Updated utilities for UK Biobank Research Analysis Platform.

Quick start on UKB RAP JupyterLab (Spark cluster)::

    !pip install -e /path/to/ukbutils --break-system-packages
    from ukbraptools import quick_setup
    sc, spark, participant, db = quick_setup()

Or clone-and-import (no install required)::

    !git clone https://github.com/zengchenjie/ukbraptools
    import sys; sys.path.insert(0, "ukbutils")
    from ukbraptools import Participant, Database
"""

from ukbraptools._core import (
    Database,
    FileTools,
    Participant,
    extract_data_dictionary,
    extract_fields_cli,
    init_spark,
    quick_setup,
    spark_to_csv,
    spark_to_polars,
)

__all__ = [
    "Database",
    "FileTools",
    "Participant",
    "extract_data_dictionary",
    "extract_fields_cli",
    "init_spark",
    "quick_setup",
    "spark_to_csv",
    "spark_to_polars",
]

__version__ = "0.2.0"
