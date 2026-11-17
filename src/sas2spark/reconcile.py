"""Reconcile a migrated Spark output against a reference extract (for example, the legacy SAS dataset
exported to CSV/Parquet). Reports row-count, missing/extra keys, and per-column mismatches."""
from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class ReconResult:
    source_rows: int
    target_rows: int
    missing_in_target: int
    extra_in_target: int
    column_mismatches: dict[str, int] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return (self.source_rows == self.target_rows and not self.missing_in_target
                and not self.extra_in_target and not any(self.column_mismatches.values()))


def reconcile(reference: DataFrame, migrated: DataFrame, keys: list[str], tolerance: float = 1e-6) -> ReconResult:
    cols = [c for c in reference.columns if c not in keys and c in migrated.columns]
    r = reference.select(*keys, *[F.col(c).alias(f"r_{c}") for c in cols]).withColumn("_r", F.lit(1))
    m = migrated.select(*keys, *[F.col(c).alias(f"m_{c}") for c in cols]).withColumn("_m", F.lit(1))
    j = r.join(m, keys, "full_outer")
    both = j.filter("_r = 1 AND _m = 1")
    mismatches = {}
    dtypes = dict(reference.dtypes)
    for c in cols:
        a, b = F.col(f"r_{c}"), F.col(f"m_{c}")
        if dtypes[c] in ("double", "float") or dtypes[c].startswith("decimal"):
            diff = (F.abs(a - b) > tolerance) | (a.isNull() != b.isNull())
        else:
            diff = ~a.eqNullSafe(b)
        mismatches[c] = both.filter(diff).count()
    return ReconResult(reference.count(), migrated.count(), j.filter("_m IS NULL").count(),
                       j.filter("_r IS NULL").count(), mismatches)
