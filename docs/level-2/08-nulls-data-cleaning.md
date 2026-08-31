# 08 · Nulls Data Cleaning

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Real data is messy: missing values, duplicate rows, inconsistent types,
outliers from bad upstream input. This module covers PySpark's toolkit for
finding and fixing these problems before they corrupt downstream
aggregations and joins.

```python
data = [
    (1, "Alice", 34, "US", 55000.0),
    (2, "Bob", None, "IN", 42000.0),
    (3, None, 29, "US", None),
    (4, "Deepak", 41, None, 61000.0),
    (5, "Alice", 34, "US", 55000.0),  # exact duplicate of row 1
    (6, "Elena", -5, "DE", 30000.0),  # invalid age
]
df = spark.createDataFrame(data, ["id", "name", "age", "country", "salary"])
```

## Finding nulls: isNull, isNotNull, per-column counts

```python
from pyspark.sql.functions import col, isnull, isnan, when, count

df.filter(col("name").isNull()).show()
# id=3 row only

df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df.columns
]).show()

# +---+----+---+-------+------+
# | id|name|age|country|salary|
# +---+----+---+-------+------+
# |  0|   1|  1|      1|     1|
# +---+----+---+-------+------+
```

That last pattern — a list comprehension over `df.columns`, each producing
a per-column null count — is the standard "null profile" check to run on
any new dataset before building a pipeline against it.

## isnan vs. isNull: a common trap

`isNull()` and `isnan()` are **not the same check**. `isNull()` detects
SQL `NULL`; `isnan()` detects the floating-point `NaN` value, which is a
valid (non-null) numeric value that can appear from certain numeric
operations (like `0.0 / 0.0`) or from reading source data with literal
`"NaN"` strings.

```python
import math
nan_df = spark.createDataFrame([(1, float("nan")), (2, None), (3, 5.0)], ["id", "val"])

nan_df.filter(col("val").isNull()).show()   # only id=2
nan_df.filter(isnan(col("val"))).show()      # only id=1
```

Use `col("val").isNull() | isnan(col("val"))` to catch both if you need a
combined "not a usable number" check — checking only one is a frequent,
subtle bug source.

## Dropping rows: dropna / na.drop

```python
df.na.drop().show()          # drops any row with ANY null column
df.na.drop(how="all").show() # drops only rows where EVERY column is null
df.na.drop(subset=["name", "age"]).show()  # drops rows null in name OR age
df.na.drop(thresh=4).show()  # keeps rows with at least 4 non-null columns
```

`thresh` is useful for wide tables where you want to tolerate a few
missing optional fields but drop rows that are mostly empty.

## Filling nulls: fillna / na.fill

```python
df.na.fill({"age": 0, "country": "UNKNOWN", "salary": 0.0}).show()

# +---+------+---+-------+-------+
# | id|  name|age|country| salary|
# +---+------+---+-------+-------+
# |  1| Alice| 34|     US|55000.0|
# |  2|   Bob|  0|     IN|42000.0|
# |  3|  null| 29|     US|    0.0|
# |  4|Deepak| 41|UNKNOWN|61000.0|
# |  5| Alice| 34|     US|55000.0|
# |  6| Elena| -5|     DE|30000.0|
# +---+------+---+-------+-------+
```

A dict passed to `.fill()` lets you specify a different default per
column, and only fills columns whose type matches the value given (a
string default won't accidentally get applied to a numeric column). Note
`name` (id=3) is *not* filled here because it wasn't in the dict — filling
a name with a placeholder is often the wrong choice; dropping or flagging
is usually better than fabricating a name.

Filling with an aggregate (e.g. mean imputation) requires computing the
value first, since `.fill()` only accepts literals:

```python
from pyspark.sql.functions import mean

mean_age = df.select(mean("age")).first()[0]
df.na.fill({"age": mean_age}).show()
```

## Deduplication: dropDuplicates

```python
df.dropDuplicates().show()
# removes id=5 (exact duplicate of id=1 across ALL columns)

df.dropDuplicates(["name", "age"]).show()
# removes any row sharing BOTH name and age with an earlier-kept row —
# more aggressive; keeps only the first-encountered row per (name, age)
```

`dropDuplicates()` with no arguments compares every column; passing a
subset of columns treats those columns as the identity key and arbitrarily
keeps one row per key (which row is kept is not guaranteed unless you
control ordering first, e.g. with a window `row_number()` ranked by a
tiebreaker column like `id`).

```python
# Deterministic dedup: keep the row with the LOWEST id per (name, age) key
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("name", "age").orderBy("id")
deduped = df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")
```

## Validating value ranges: catching bad data

```python
df.filter((col("age") < 0) | (col("age") > 120)).show()
# id=6, age=-5 — clearly invalid

# Clean invalid ages to null rather than silently keeping a bad value:
cleaned = df.withColumn(
    "age",
    when((col("age") >= 0) & (col("age") <= 120), col("age")).otherwise(None)
)
```

Turning an invalid value into `null` (rather than dropping the whole row,
or leaving the bad value in place) is often the right call: it preserves
the row for other valid columns while flagging that this one field
couldn't be trusted.

## Standardizing inconsistent values

```python
from pyspark.sql.functions import upper, trim, initcap

# Country codes: normalize casing/whitespace inconsistencies
messy_countries = spark.createDataFrame(
    [(1, " us"), (2, "US"), (3, "u.s."), (4, "United States")], ["id", "country"]
)

normalized = messy_countries.withColumn(
    "country_clean",
    when(upper(trim(col("country"))).isin("US", "U.S.", "UNITED STATES"), "US")
    .otherwise(upper(trim(col("country"))))
)
normalized.show()
```

Real-world "standardize this categorical column" logic almost always ends
up as a `when/otherwise` chain (or a join against a reference/lookup
table for large numbers of variants) — there's no single built-in that
handles arbitrary business-specific normalization rules.

## Worked example: a full cleaning pipeline

Task: from `df`, produce a cleaned dataset — invalid ages nulled, missing
`country` filled with `"UNKNOWN"`, exact duplicate rows removed, and rows
missing `name` dropped entirely (can't analyze a customer with no
identity).

```python
clean = (
    df.dropDuplicates()
      .na.drop(subset=["name"])
      .withColumn("age", when((col("age") >= 0) & (col("age") <= 120), col("age")).otherwise(None))
      .na.fill({"country": "UNKNOWN"})
)
clean.orderBy("id").show()

# +---+------+----+-------+-------+
# | id|  name| age|country| salary|
# +---+------+----+-------+-------+
# |  1| Alice|  34|     US|55000.0|
# |  2|   Bob|null|     IN|42000.0|
# |  4|Deepak|  41|UNKNOWN|61000.0|
# |  6| Elena|null|     DE|30000.0|
# +---+------+----+-------+-------+
```

Row 3 (`name=null`) is gone (dropped for missing identity); row 5 (exact
duplicate of row 1) is gone (deduplicated); row 6's `age=-5` became `null`
rather than being dropped outright, since the rest of that row is usable.

## Exercise

Using `df` from the top of this module:

1. Produce a per-column null count for every column, using the list
   comprehension pattern shown above.
2. Write a filter that would catch both `NULL` and `NaN` values in a
   hypothetical float column, using `|`.
3. Deduplicate `df` on `(name, age)`, keeping the row with the **highest**
   `salary` per key (hint: `row_number()` over a window ordered by
   `salary` descending).
4. Build the full cleaning pipeline from the worked example, but change
   the "missing name" policy to fill with `"UNKNOWN_CUSTOMER"` instead of
   dropping, and explain in a comment which choice you think is more
   appropriate for a real billing dataset and why.
