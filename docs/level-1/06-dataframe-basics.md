# 06 · DataFrame Basics (select, filter, withColumn)

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

We'll work with this sample DataFrame throughout:

```python
data = [
    (1, "Alice", "US", 120.50, 2),
    (2, "Bob", "IN", 45.00, 1),
    (3, "Carla", "US", 300.25, 5),
    (4, "Deepak", "DE", 60.00, 1),
    (5, "Elena", "US", 15.00, 1),
]
columns = ["order_id", "customer", "country", "amount", "quantity"]
df = spark.createDataFrame(data, columns)
```

## select — choosing and computing columns

`select` returns a new DataFrame with only the columns (or computed
expressions) you name — it never mutates `df` in place, since DataFrames are
immutable.

```python
df.select("customer", "amount").show()
# +--------+------+
# |customer|amount|
# +--------+------+
# |   Alice| 120.5|
# |     Bob|  45.0|
# |   Carla|300.25|
# |  Deepak|  60.0|
# |   Elena|  15.0|
# +--------+------+

# Three equivalent ways to reference a column:
df.select(df.customer, df["amount"]).show()
from pyspark.sql.functions import col
df.select(col("customer"), col("amount")).show()

# select can also compute new expressions inline:
df.select("customer", (df.amount * df.quantity).alias("line_total")).show()
# +--------+----------+
# |customer|line_total|
# +--------+----------+
# |   Alice|     241.0|
# |     Bob|      45.0|
# |   Carla|   1501.25|
# |  Deepak|      60.0|
# |   Elena|      15.0|
# +--------+----------+
```

`col(...)` becomes essential once you use functions from
`pyspark.sql.functions` (covered more in Level 2) — it lets you refer to a
column by name in any context, not just when you already have a `df`
variable in scope (useful inside functions that receive an arbitrary
DataFrame).

## filter / where — keeping matching rows

`filter` and `where` are exact synonyms in PySpark — pick whichever reads
better to you; this course uses `filter`.

```python
df.filter(df.country == "US").show()
# rows for Alice, Carla, Elena

df.filter(df.amount > 50).show()
# rows for Alice, Carla, Deepak

# Combine conditions with & (and), | (or), ~ (not) — NOT Python's and/or/not,
# and each condition needs parentheses because of operator precedence:
df.filter((df.country == "US") & (df.amount > 50)).show()
# rows for Alice, Carla

df.filter((df.country == "US") | (df.quantity > 3)).show()
# rows for Alice, Carla, Elena (US) -- Carla also matches quantity > 3

df.filter(~(df.country == "US")).show()
# rows for Bob, Deepak

# String-expression form (handy for dynamic/SQL-like conditions):
df.filter("amount > 50 AND country = 'US'").show()
```

The most common early mistake: writing `df.filter(df.country == "US" and
df.amount > 50)` using Python's `and`. That fails (or silently misbehaves)
because Python's `and`/`or` operate on the *Column objects themselves*, not
on the per-row boolean results Spark needs — always use `&`, `|`, `~` with
parentheses around each side for DataFrame conditions.

## withColumn — adding or replacing a column

```python
df2 = df.withColumn("line_total", df.amount * df.quantity)
df2.show()
# adds a new "line_total" column to every row

# Reusing an existing column name REPLACES it (common for cleaning/casting):
df3 = df.withColumn("amount", df.amount.cast("float"))
df3.printSchema()   # amount is now float instead of double

# Conditional columns use when/otherwise (Spark's if/else expression):
from pyspark.sql.functions import when

df4 = df.withColumn(
    "size_tier",
    when(df.amount >= 200, "large")
    .when(df.amount >= 50, "medium")
    .otherwise("small"),
)
df4.select("customer", "amount", "size_tier").show()
# +--------+------+---------+
# |customer|amount|size_tier|
# +--------+------+---------+
# |   Alice| 120.5|   medium|
# |     Bob|  45.0|    small|
# |   Carla|300.25|    large|
# |  Deepak|  60.0|   medium|
# |   Elena|  15.0|    small|
# +--------+------+---------+
```

Remember: like every DataFrame operation, `withColumn` returns a **new**
DataFrame — `df` itself is untouched. This trips people up coming from
`pandas`, where `df["col"] = ...` mutates in place. In PySpark you always
reassign: `df = df.withColumn(...)`.

!!! warning "Avoid calling withColumn in a loop many times"
    Calling `.withColumn()` repeatedly in a Python `for` loop (e.g. once per
    column, hundreds of times) can noticeably slow down query planning,
    because each call adds a new node to the logical plan Catalyst has to
    analyze. Prefer a single `.select()` with all the expressions you need
    computed at once when you're adding many columns.

## Chaining operations together

Because every transformation returns a new DataFrame, they chain naturally —
this is the idiomatic PySpark style you'll see throughout this course:

```python
result = (
    df.filter(df.country == "US")
      .withColumn("line_total", df.amount * df.quantity)
      .select("customer", "line_total")
      .filter(col("line_total") > 100)
)
result.show()
# +--------+----------+
# |customer|line_total|
# +--------+----------+
# |   Alice|     241.0|
# |   Carla|   1501.25|
# +--------+----------+
```

Remember from Module 2: none of this executes until an action (`.show()`
here) is called — Spark sees the whole chain at once and optimizes it as a
unit.

## Renaming and dropping columns

```python
df.withColumnRenamed("customer", "customer_name").show()
df.drop("quantity").show()   # returns a DataFrame without that column
```

## Worked example: building a small report

Task: from the `df` above, produce customer name, order total (amount ×
quantity), and a flag for whether the order qualifies for free shipping
(line total over 100), for US orders only.

```python
report = (
    df.filter(df.country == "US")
      .withColumn("line_total", df.amount * df.quantity)
      .withColumn(
          "free_shipping",
          when(col("line_total") > 100, True).otherwise(False),
      )
      .select("customer", "line_total", "free_shipping")
)
report.show()
# +--------+----------+-------------+
# |customer|line_total|free_shipping|
# +--------+----------+-------------+
# |   Alice|     241.0|         true|
# |   Carla|   1501.25|         true|
# |   Elena|      15.0|        false|
# +--------+----------+-------------+
```

## Exercise

Using the same `df` from the top of this module:

1. Select just `customer` and `country`, but rename `country` to `country_code`.
2. Filter to orders where `quantity` is greater than 1 **and** `amount` is
   less than 200.
3. Add a column `discounted_amount` equal to `amount * 0.9` for orders from
   `"DE"`, and equal to `amount` (unchanged) for everyone else, using
   `when`/`otherwise`.
4. Chain all three ideas into one expression that filters to non-US orders,
   computes `discounted_amount` as in step 3, and selects only `customer`
   and `discounted_amount`.

Expected result for step 4 (only Bob and Deepak are non-US; Deepak gets the
10% DE discount): Bob → 45.0 unchanged, Deepak → 60.0 × 0.9 = 54.0.
