"""Reusable timeseries metrics computed with Spark DataFrame APIs.

The functions in this module return Spark DataFrames so they can be
composed into larger pipelines and written out by the orchestration code.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def timeseries_summary(df: DataFrame, id_col: str = "meetreeks_id", ts_col: str = "timestamp") -> DataFrame:
    """Compute simple per-timeseries summary metrics.

    Produces columns: `count`, `first_timestamp`, `last_timestamp`, `span_days`.

    Parameters
    - df: input DataFrame containing timeseries rows
    - id_col: identifier for each timeseries
    - ts_col: timestamp column name (must be a Spark timestamp/date type)

    Returns
    - Spark DataFrame grouped by `id_col` containing summary metrics.

    Example usage::
        summary = timeseries_summary(df, id_col='meetreeks_id', ts_col='timestamp')
    """
    agg_exprs = [
        F.count("*").alias("count"),
        F.min(ts_col).alias("first_timestamp"),
        F.max(ts_col).alias("last_timestamp"),
        F.datediff(F.max(ts_col), F.min(ts_col)).alias("span_days"),
    ]
    return df.groupBy(id_col).agg(*agg_exprs)
