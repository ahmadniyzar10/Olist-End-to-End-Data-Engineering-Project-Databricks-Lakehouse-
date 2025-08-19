# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

CONTROL_COLS = {"start_ts","end_ts","is_current","change_hash"}

def ensure_dim_table(full_name, sample_df):
    # Build the create schema from business/tracked columns only (no control cols)
    base_fields = [f for f in sample_df.schema.fields if f.name not in CONTROL_COLS and f.name != "change_hash"]
    cols = ", ".join([f"`{f.name}` {f.dataType.simpleString()}" for f in base_fields])
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
          {cols},
          start_ts TIMESTAMP,
          end_ts TIMESTAMP,
          is_current BOOLEAN,
          change_hash STRING
        )
        USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

def with_change_hash(df, tracked_cols):
    # Always (re)derive change_hash on the staged dataframe
    if "change_hash" in df.columns:
        df = df.drop("change_hash")
    return df.withColumn(
        "change_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_cols]), 256)
    )

def scd2_merge(full_name, staged_df, business_keys, tracked_cols):
    base = (staged_df
            .select(*(business_keys + tracked_cols))
            .dropDuplicates(business_keys))

    # Ensure target table exists BEFORE adding change_hash to the sample schema
    ensure_dim_table(full_name, base.limit(1))

    staged = (with_change_hash(base, tracked_cols)
              .withColumn("start_ts", F.current_timestamp())
              .withColumn("end_ts", F.lit(None).cast("timestamp"))
              .withColumn("is_current", F.lit(True)))

    dt = DeltaTable.forName(spark, full_name)
    on = " AND ".join([f"t.`{k}` = s.`{k}`" for k in business_keys])

    (dt.alias("t").merge(staged.alias("s"), on)
        .whenMatchedUpdate(
            condition="t.is_current = true AND t.change_hash <> s.change_hash",
            set={"end_ts": "current_timestamp()", "is_current": "false"}
        )
        .whenNotMatchedInsertAll()
        .execute())
