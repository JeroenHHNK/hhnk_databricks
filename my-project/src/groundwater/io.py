"""I/O helpers for Databricks (Spark-based)

These are thin wrappers around Spark read/write. Keep logic here minimal;
production projects should include error handling, retries and logging.
"""
from pyspark.sql import SparkSession, DataFrame


def read_table(spark: SparkSession, table: str) -> DataFrame:
    """Read a table from the Spark catalog/metastore.

    Parameters
    - spark: SparkSession
    - table: table identifier (e.g. `schema.table_name`)

    Returns
    - pyspark.sql.DataFrame
    """
    # Intentionally minimal: Databricks typically mounts a repo and uses
    # `spark.table` to access managed or external tables in the metastore.
    return spark.table(table)


def write_table(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """Write a DataFrame to a managed/unmanaged table via `saveAsTable`.

    Parameters
    - df: DataFrame to write
    - table: destination table identifier
    - mode: write mode (default: 'overwrite')
    """
    df.write.mode(mode).saveAsTable(table)
