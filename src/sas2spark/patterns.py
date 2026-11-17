"""PySpark equivalents of the programs in /sas. Each function documents the SAS behavior it matches."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# SAS sorts BY variables; Spark has no implicit order, so every window states its ordering explicitly.
def acct_order():
    return Window.partitionBy("account_id").orderBy("txn_date", "txn_id")


def derive_flags(txn: DataFrame) -> DataFrame:
    """01_derive_flags.sas. SAS WEEKDAY(): 1=Sunday..7=Saturday, same as Spark dayofweek()."""
    band = (F.when(F.col("amount") >= 1000, "HIGH")
             .when(F.col("amount") >= 100, "MEDIUM")
             .otherwise("LOW"))
    return (txn.withColumn("amount_band", band)
               .withColumn("is_weekend", F.dayofweek("txn_date").isin(1, 7).cast("int")))


def running_total(txn: DataFrame) -> DataFrame:
    """02_retain_running_total.sas. RETAIN + FIRST. reset -> cumulative window; LAST. -> row_number desc."""
    cum = acct_order().rowsBetween(Window.unboundedPreceding, Window.currentRow)
    last = Window.partitionBy("account_id").orderBy(F.col("txn_date").desc(), F.col("txn_id").desc())
    return (txn.withColumn("running_amt", F.sum("amount").over(cum))
               .withColumn("txn_seq", F.row_number().over(acct_order()))
               .withColumn("is_last_txn", (F.row_number().over(last) == 1).cast("int")))


def first_per_key(txn: DataFrame) -> DataFrame:
    """03_nodupkey.sas. dropDuplicates() keeps an arbitrary row; row_number makes it deterministic."""
    return (txn.withColumn("_rn", F.row_number().over(acct_order()))
               .filter("_rn = 1").drop("_rn"))


def proc_means(txn: DataFrame) -> DataFrame:
    """04_proc_means.sas. SAS STD is the sample standard deviation (n-1) -> stddev_samp."""
    return (txn.groupBy("region")
               .agg(F.count("amount").alias("n"), F.avg("amount").alias("mean_amt"),
                    F.stddev_samp("amount").alias("std_amt"), F.min("amount").alias("min_amt"),
                    F.max("amount").alias("max_amt")))


def proc_freq(txn: DataFrame) -> DataFrame:
    """05_proc_freq.sas. PERCENT is 0-100 and excludes missing levels by default."""
    non_missing = txn.filter(F.col("channel").isNotNull())
    total = non_missing.count()
    return (non_missing.groupBy("channel").agg(F.count("*").alias("count"))
                       .withColumn("percent", F.col("count") * 100.0 / F.lit(total)))


def merge_in_flags(accounts: DataFrame, totals: DataFrame) -> DataFrame:
    """06_merge_in_flags.sas. A full outer join with explicit IN= style indicators.
    Caveat: SAS match-merge on many-to-many keys behaves differently from a SQL join; this assumes unique keys."""
    a = accounts.withColumn("_in_a", F.lit(True))
    t = totals.withColumn("_in_t", F.lit(True))
    j = a.join(t, "account_id", "full_outer")
    status = (F.when(F.col("_in_a") & F.col("_in_t"), "BOTH")
               .when(F.col("_in_a"), "ACCOUNT_ONLY").otherwise("TXN_ONLY"))
    return j.withColumn("match_status", status).drop("_in_a", "_in_t")


def lag_gap(txn: DataFrame) -> DataFrame:
    """07_lag.sas. LAG() resets at FIRST. -> lag over a partitioned window gives null for the first row."""
    return (txn.withColumn("prev_date", F.lag("txn_date").over(acct_order()))
               .withColumn("days_since_prev", F.datediff("txn_date", "prev_date")))
