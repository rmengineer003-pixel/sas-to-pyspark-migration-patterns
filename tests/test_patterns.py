"""Parity tests: each PySpark pattern vs. a plain-Python reference that encodes the SAS semantics."""
import random
import statistics
from collections import Counter, defaultdict
from datetime import date, timedelta

import pytest
from pyspark.sql import Row

from sas2spark import patterns as p
from sas2spark.reconcile import reconcile


def synthetic_txn(n=300, seed=3):
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        rows.append(Row(txn_id=i, account_id=f"A{rng.randint(1, 25):03d}",
                        txn_date=date(2025, 1, 1) + timedelta(days=rng.randint(0, 90)),
                        amount=round(rng.uniform(5, 2500), 2), region=rng.choice(["EAST", "WEST", "SOUTH"]),
                        channel=rng.choice(["WEB", "BRANCH", "MOBILE", None])))
    return rows


@pytest.fixture(scope="module")
def data(spark):
    rows = synthetic_txn()
    return rows, spark.createDataFrame(rows)


def sas_sorted(rows):
    return sorted(rows, key=lambda r: (r.account_id, r.txn_date, r.txn_id))


def test_derive_flags(data):
    rows, df = data
    got = {r.txn_id: (r.amount_band, r.is_weekend) for r in p.derive_flags(df).collect()}
    for r in rows:
        band = "HIGH" if r.amount >= 1000 else "MEDIUM" if r.amount >= 100 else "LOW"
        weekend = int(r.txn_date.isoweekday() in (6, 7))
        assert got[r.txn_id] == (band, weekend)


def test_running_total_matches_retain(data):
    rows, df = data
    got = {r.txn_id: (round(r.running_amt, 2), r.txn_seq, r.is_last_txn) for r in p.running_total(df).collect()}
    expected, by_acct = {}, defaultdict(list)
    for r in sas_sorted(rows):
        by_acct[r.account_id].append(r)
    for recs in by_acct.values():
        total = 0.0
        for i, r in enumerate(recs, 1):
            total += r.amount
            expected[r.txn_id] = (round(total, 2), i, int(i == len(recs)))
    assert got == expected


def test_first_per_key_matches_nodupkey(data):
    rows, df = data
    seen, expected = set(), set()
    for r in sas_sorted(rows):
        if r.account_id not in seen:
            seen.add(r.account_id)
            expected.add(r.txn_id)
    assert {r.txn_id for r in p.first_per_key(df).collect()} == expected


def test_proc_means_uses_sample_std(data):
    rows, df = data
    got = {r.region: r for r in p.proc_means(df).collect()}
    for region in {r.region for r in rows}:
        vals = [r.amount for r in rows if r.region == region]
        g = got[region]
        assert g.n == len(vals)
        assert g.mean_amt == pytest.approx(statistics.mean(vals))
        assert g.std_amt == pytest.approx(statistics.stdev(vals))  # n-1, like SAS
        assert (g.min_amt, g.max_amt) == (min(vals), max(vals))


def test_proc_freq_excludes_missing(data):
    rows, df = data
    counts = Counter(r.channel for r in rows if r.channel is not None)
    total = sum(counts.values())
    got = {r.channel: (r["count"], r.percent) for r in p.proc_freq(df).collect()}
    assert set(got) == set(counts)
    for k, c in counts.items():
        assert got[k][0] == c and got[k][1] == pytest.approx(100 * c / total)


def test_merge_in_flags(spark):
    acc = spark.createDataFrame([Row(account_id="A1", name="x"), Row(account_id="A2", name="y")])
    tot = spark.createDataFrame([Row(account_id="A2", total=5.0), Row(account_id="A3", total=7.0)])
    got = {r.account_id: r.match_status for r in p.merge_in_flags(acc, tot).collect()}
    assert got == {"A1": "ACCOUNT_ONLY", "A2": "BOTH", "A3": "TXN_ONLY"}


def test_lag_resets_per_account(data):
    rows, df = data
    got = {r.txn_id: r.days_since_prev for r in p.lag_gap(df).collect()}
    prev = {}
    for r in sas_sorted(rows):
        exp = (r.txn_date - prev[r.account_id]).days if r.account_id in prev else None
        assert got[r.txn_id] == exp
        prev[r.account_id] = r.txn_date


def test_reconcile_detects_differences(spark, data):
    _, df = data
    ref = p.proc_means(df)
    assert reconcile(ref, ref, ["region"]).passed
    from pyspark.sql import functions as F
    broken = ref.withColumn("mean_amt", F.when(F.col("region") == "EAST", F.col("mean_amt") + 1)
                            .otherwise(F.col("mean_amt"))).filter("region != 'WEST'")
    res = reconcile(ref, broken, ["region"])
    assert not res.passed
    assert res.missing_in_target == 1 and res.column_mismatches["mean_amt"] == 1
