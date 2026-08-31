# 07 · SQL And Dataframe API

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Module 06 introduced `spark.sql(...)`; this module goes deeper into
*translating fluently between the two styles* — reading a DataFrame chain
and writing its SQL equivalent (and back), and recognizing where each
style is the clearer choice in real pipeline code.

```python
events = spark.createDataFrame(
    [
        (1, "click", "home", "2024-01-01 10:00:00"),
        (2, "view", "home", "2024-01-01 10:01:00"),
        (3, "click", "cart", "2024-01-01 10:05:00"),
        (4, "purchase", "cart", "2024-01-01 10:06:00"),
        (5, "click", "home", "2024-01-02 09:00:00"),
        (6, "purchase", "cart", "2024-01-02 09:10:00"),
    ],
    ["event_id", "event_type", "page", "event_time"],
)
events.createOrReplaceTempView("events")
```

## Side-by-side translation table

| DataFrame API | Spark SQL |
|---|---|
| `df.select("a", "b")` | `SELECT a, b FROM t` |
| `df.filter(col("a") > 5)` | `WHERE a > 5` |
| `df.groupBy("a").agg(sum("b"))` | `GROUP BY a` + `SUM(b)` |
| `df.orderBy(col("a").desc())` | `ORDER BY a DESC` |
| `df.join(other, "key")` | `JOIN other USING (key)` |
| `df.withColumn("c", expr)` | `SELECT *, expr AS c` |
| `df.withColumnRenamed("a", "b")` | `SELECT a AS b` |
| `df.distinct()` | `SELECT DISTINCT ...` |
| `df.limit(n)` | `LIMIT n` |
| `Window.partitionBy(...).orderBy(...)` | `OVER (PARTITION BY ... ORDER BY ...)` |

## Worked translation: DataFrame chain to SQL

DataFrame version — count events per page, keep pages with more than one
event type represented:

```python
from pyspark.sql.functions import countDistinct, count

df_version = (
    events.groupBy("page")
          .agg(
              count("*").alias("total_events"),
              countDistinct("event_type").alias("distinct_types"),
          )
          .filter("distinct_types > 1")
          .orderBy("page")
)
df_version.show()

# +----+------------+--------------+
# |page|total_events|distinct_types|
# +----+------------+--------------+
# |cart|           3|             2|
# |home|           3|             2|
# +----+------------+--------------+
```

Equivalent SQL — note `HAVING`, which is the SQL-native way to filter on
an aggregate, versus the DataFrame API's "just `.filter()` again after
`.agg()`" (there's no separate `HAVING` DataFrame method):

```python
sql_version = spark.sql("""
    SELECT page,
           COUNT(*) AS total_events,
           COUNT(DISTINCT event_type) AS distinct_types
    FROM events
    GROUP BY page
    HAVING distinct_types > 1
    ORDER BY page
""")
sql_version.show()  # identical output to df_version
```

Both produce the exact same physical plan — verify with `.explain()` on
each and compare, or check `df_version.exceptAll(sql_version).count() == 0`
plus the reverse to confirm row-for-row equivalence.

## Window functions: DataFrame vs. SQL syntax

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

w = Window.partitionBy("page").orderBy(col("event_time"))
events.withColumn("seq", rank().over(w)).orderBy("page", "seq").show()
```

```sql
SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time) AS seq
FROM events
ORDER BY page, seq
```

```python
spark.sql("""
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time) AS seq
    FROM events
    ORDER BY page, seq
""").show()
```

The `Window` object's `partitionBy`/`orderBy` map directly onto SQL's
`OVER (PARTITION BY ... ORDER BY ...)` clause — this is worth memorizing
since window logic is common to both styles in almost every non-trivial
pipeline.

## When to prefer DataFrame API

- **Parameterized/programmatic pipelines** — building a query whose
  columns or filters are decided at runtime from a config dict or loop is
  far more natural as chained method calls than as dynamically
  string-built SQL text.
- **Reusable transformation functions** — a Python function taking a
  DataFrame and returning a transformed DataFrame composes cleanly
  (`df.transform(my_step)`), and integrates with normal Python testing.
- **Type-sensitive column construction** — building complex nested
  `StructType` literals or working closely with UDFs reads more naturally
  as DataFrame code.

```python
def add_event_date(df):
    from pyspark.sql.functions import to_date
    return df.withColumn("event_date", to_date("event_time"))

def filter_purchases(df):
    return df.filter("event_type = 'purchase'")

# Composable pipeline via `.transform`:
result = events.transform(add_event_date).transform(filter_purchases)
result.show()
```

## When to prefer SQL

- **Analyst-facing / cross-team logic** — SQL is the shared language
  between engineers, analysts, and BI tools; a SQL string in a config or
  file is directly reviewable/editable by non-Python-fluent teammates.
- **Complex multi-CTE aggregation logic** — deeply nested subqueries and
  CTEs are frequently more readable as SQL than as an equally deep chain
  of `.groupBy().agg().filter()` calls with intermediate variables.
- **Migrating existing SQL** — porting a query from a traditional
  warehouse to Spark is often fastest verbatim as `spark.sql(...)`, with
  Spark-SQL-specific syntax adjustments as needed.

## Worked example: same result, two implementations, one test

Task: compute each page's most recent event, expressed both ways, and
prove they match.

```python
w_latest = Window.partitionBy("page").orderBy(col("event_time").desc())

df_latest = (
    events.withColumn("rn", rank().over(w_latest))
          .filter("rn = 1")
          .select("page", "event_id", "event_type", "event_time")
          .orderBy("page")
)

sql_latest = spark.sql("""
    SELECT page, event_id, event_type, event_time FROM (
        SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS rn
        FROM events
    ) ranked
    WHERE rn = 1
    ORDER BY page
""")

assert df_latest.exceptAll(sql_latest).count() == 0
assert sql_latest.exceptAll(df_latest).count() == 0
df_latest.show()

# +----+--------+----------+-------------------+
# |page|event_id|event_type|         event_time|
# +----+--------+----------+-------------------+
# |cart|       6|  purchase|2024-01-02 09:10:00|
# |home|       5|     click|2024-01-02 09:00:00|
# +----+--------+----------+-------------------+
```

`exceptAll` in both directions is the standard "these two DataFrames have
identical rows" check — a single `exceptAll` alone only proves one side
has no extra rows, not that the sets are equal (it wouldn't catch a
missing row on the same side, for instance if `sql_latest` had fewer
rows).

## Exercise

Using `events` from the top of this module:

1. Write a DataFrame-API chain and an equivalent SQL query that both
   compute the total number of events per `event_type`, and verify they
   match with `exceptAll` in both directions.
2. Rewrite the `HAVING`-based worked example from earlier as pure
   DataFrame API code using `.filter()` after `.agg()`, and confirm it
   matches the SQL version.
3. Write a `.transform()`-composed pipeline of at least two functions that
   filters to `page = 'cart'` events and adds an `event_date` column.
4. In a short comment, describe one situation from your own experience (or
   a hypothetical one) where you'd choose SQL over the DataFrame API, and
   one where you'd choose the opposite.
