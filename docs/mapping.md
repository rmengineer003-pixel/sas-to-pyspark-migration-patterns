# SAS to PySpark quick mapping

| SAS | PySpark | Gotcha |
|---|---|---|
| `IF / ELSE IF / ELSE` in DATA step | `F.when(...).when(...).otherwise(...)` | Character length truncation in SAS (`LENGTH $6`) has no Spark equivalent; check widths in reports |
| `RETAIN x; x + amount;` with `FIRST.` reset | `F.sum().over(Window.partitionBy().orderBy().rowsBetween(unboundedPreceding, currentRow))` | Spark has no implicit row order; always order the window |
| `FIRST.var` / `LAST.var` | `row_number()` ascending / descending | |
| `PROC SORT NODUPKEY` | `row_number() == 1` | `dropDuplicates()` keeps an arbitrary row |
| `PROC MEANS ... STD=` | `stddev_samp` | SAS STD is sample (n-1), not population |
| `PROC FREQ ... OUT=` PERCENT | `count * 100 / total` | Missing levels are excluded unless `/ MISSING` |
| `MERGE a(in=x) b(in=y); BY k;` | full outer join + indicator columns | Many-to-many BY merges behave differently from SQL joins |
| `LAG(x)` with `FIRST.` reset | `F.lag(x).over(partitioned window)` | SAS LAG inside conditional logic is a classic bug source; Spark lag is evaluated per row |
| `WEEKDAY(date)` | `F.dayofweek(date)` | Both use 1 = Sunday |
| SAS missing (`.`) | `null` | SAS treats missing as smallest value in sorts and comparisons; Spark nulls do not compare |
