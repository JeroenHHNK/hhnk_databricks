"""Thin orchestration notebook to build gold tables.

Intended to be run as a Databricks job. Keep heavy logic in `groundwater`.
Replace `raw_table` and `gold_table` with environment-appropriate names
when configuring a job.
"""
from pyspark.sql import SparkSession
from groundwater import io, metrics


def main():
    spark = SparkSession.builder.getOrCreate()

    # Replace these with the actual table identifiers in your environment
    raw_table = "<raw_schema>.<raw_table>"
    gold_table = "<gold_schema>.timeseries_summary"

    # Read, compute summary, and write result
    df = io.read_table(spark, raw_table)
    summary = metrics.timeseries_summary(df)
    io.write_table(summary, gold_table)


if __name__ == "__main__":
    main()
