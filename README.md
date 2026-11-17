# SAS to PySpark Migration Patterns

> **Representative portfolio project.** The SAS programs in `/sas` are short, generic examples written for this repository, and all data is synthetic. Nothing here comes from an employer or client codebase.

Seven SAS DATA step and PROC patterns that show up in almost every legacy analytics estate, each re-implemented in PySpark and proven equal with parity tests. Includes a reconciliation helper for comparing migrated output to a legacy extract.

## Business problem

Teams moving off SAS to Spark (Databricks, EMR, Synapse) need more than a line-by-line translation. Subtle semantic differences (row order, sample vs population standard deviation, missing values, LAG resets) cause silent number changes that surface in reports weeks later. Every pattern here is backed by a test that encodes the SAS behavior.

## Patterns

| # | SAS program | PySpark function | Key semantic handled |
|---|---|---|---|
| 1 | `sas/01_derive_flags.sas` | `derive_flags` | IF/ELSE bands, WEEKDAY numbering |
| 2 | `sas/02_retain_running_total.sas` | `running_total` | RETAIN + FIRST./LAST. → ordered windows |
| 3 | `sas/03_nodupkey.sas` | `first_per_key` | deterministic NODUPKEY |
| 4 | `sas/04_proc_means.sas` | `proc_means` | STD = sample standard deviation |
| 5 | `sas/05_proc_freq.sas` | `proc_freq` | PERCENT excludes missing |
| 6 | `sas/06_merge_in_flags.sas` | `merge_in_flags` | IN= flags on a match-merge |
| 7 | `sas/07_lag.sas` | `lag_gap` | LAG reset per BY group |

See `docs/mapping.md` for a one-page cheat sheet.

## Reconciliation helper

`sas2spark.reconcile.reconcile(reference_df, migrated_df, keys)` returns row counts, keys missing or extra on either side, and per-column mismatch counts with a numeric tolerance. Use it to sign off each migrated job against the legacy output.

```python
res = reconcile(legacy_extract_df, migrated_df, keys=["region"])
assert res.passed, res
```

## Run it

```bash
pip install -e ".[dev]"     # Java 17 required for local Spark
pytest -q
```

## Tests

`tests/test_patterns.py` generates 300 synthetic transactions and compares each PySpark function to a plain-Python reference implementation of the SAS logic. It also checks that the reconciliation helper catches a missing key and a changed value.

## Limitations and next steps

- SAS formats, informats, and macro language are out of scope.
- Many-to-many match-merge semantics are documented, not emulated.
- Next: add PROC TRANSPOSE, PROC SUMMARY with multiple CLASS combinations (TYPES/WAYS), and hash-object lookups.

## License

MIT
