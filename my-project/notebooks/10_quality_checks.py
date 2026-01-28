"""Basic data quality checks for timeseries data.

Contains small, reusable checks that can be invoked from jobs or manually
from the Databricks workspace.
"""
from pyspark.sql import SparkSession
from groundwater import io
from pyspark.sql import functions as F
from pyspark.sql import Window


def check_row_count(spark: SparkSession, table: str) -> int:
    df = io.read_table(spark, table)
    return df.count()


def check_nulls(df, cols):
    """Return DataFrame with counts of nulls per requested column."""
    exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]
    return df.agg(*exprs)


def check_timestamp_order(df, id_col: str = "meetreeks_id", ts_col: str = "timestamp"):
    """Find timeseries where timestamps are not monotonically increasing."""
    w = Window.partitionBy(id_col).orderBy(ts_col)
    prev_ts = F.lag(F.col(ts_col)).over(w)
    anomalies = df.withColumn("prev_ts", prev_ts).where(
        F.col("prev_ts").isNotNull() & (F.col(ts_col) < F.col("prev_ts"))
    )
    return anomalies.select(id_col).distinct()


def main():
    spark = SparkSession.builder.getOrCreate()
    sample_table = "<raw_schema>.<raw_table>"

    df = io.read_table(spark, sample_table)
    print("Row count:", check_row_count(spark, sample_table))
    print("Null summary:")
    print(check_nulls(df, ["meetreeks_id", "timestamp"]).show())
    print("Timestamp order anomalies:")
    print(check_timestamp_order(df).show())


if __name__ == "__main__":
    main()
