# 06 · Spark SQL Basics

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Every PySpark DataFrame operation ultimately compiles down to the same
Catalyst logical plan that a raw SQL query would produce — the DataFrame
API and Spark SQL are two syntaxes for the same engine. This module covers
running actual SQL strings against DataFrames via temporary views.

```python
orders = spark.createDataFrame(
    [
        (1, 101, "electronics", 250.0),
        (2, 102, "books", 40.0),
        (3, 101, "books", 15.0),
        (4, 103, "electronics", 600.0),
        (5, 102, "electronics", 80.0),
    ],
    ["order_id", "customer_id", "category", "amount"],
)
```

## Registering a temporary view

A DataFrame isn't queryable by name in SQL until you register it as a
view. Temp views are scoped to the current `SparkSession`.

```python
orders.createOrReplaceTempView("orders")

spark.sql("SELECT * FROM orders WHERE amount > 100").show()

# +--------+-----------+-----------+------+
# |order_id|customer_id|   category|amount|
# +--------+-----------+-----------+------+
# |       1|        101|electronics| 250.0|
# |       4|        103|electronics| 600.0|
# +--------+-----------+-----------+------+
```

`createOrReplaceTempView` overwrites any existing view of the same name in
this session — safe to call repeatedly, e.g. across notebook re-runs.
`createTempView` (without `OrReplace`) raises an error if the name is
already taken.

## Global temp views: sharing across sessions

```python
orders.createOrReplaceGlobalTempView("orders_global")

# Must be queried with the `global_temp` database prefix:
spark.sql("SELECT * FROM global_temp.orders_global").show()
```

A regular temp view dies with the `SparkSession` that created it; a
*global* temp view is tied to the Spark *application* and is visible to
other sessions created from the same `SparkContext` (rare in typical
scripts, more relevant in notebook environments with multiple sessions).

## SELECT, WHERE, GROUP BY — SQL you already know

```python
spark.sql("""
    SELECT category, COUNT(*) AS order_count, SUM(amount) AS total_revenue
    FROM orders
    WHERE amount > 10
    GROUP BY category
    ORDER BY total_revenue DESC
""").show()

# +-----------+-----------+-------------+
# |   category|order_count|total_revenue|
# +-----------+-----------+-------------+
# |electronics|          3|        930.0|
# |      books|          2|         55.0|
# +-----------+-----------+-------------+
```

Every clause here (`WHERE`, `GROUP BY`, `ORDER BY`, aggregate functions)
maps directly onto the DataFrame methods from Level 1 and this module's
own `.filter()`/`.groupBy()`/`.agg()`/`.orderBy()` — this is the same
Catalyst optimizer processing the same logical operations either way.

## Multi-table SQL: joins in plain SQL

```python
customers = spark.createDataFrame(
    [(101, "Alice"), (102, "Bob"), (103, "Carla")],
    ["customer_id", "name"],
)
customers.createOrReplaceTempView("customers")

spark.sql("""
    SELECT c.name, o.category, o.amount
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    ORDER BY o.order_id
""").show()

# +-----+-----------+------+
# | name|   category|amount|
# +-----+-----------+------+
# |Alice|electronics| 250.0|
# |  Bob|      books|  40.0|
# |Alice|      books|  15.0|
# |Carla|electronics| 600.0|
# |  Bob|electronics|  80.0|
# +-----+-----------+------+
```

Aliasing tables (`orders o`, `customers c`) works exactly as in any SQL
dialect. Under the hood, this produces the same `SortMergeJoin` /
`BroadcastHashJoin` physical plan choices covered in this level's joins
module — `.explain()` on the result of `spark.sql(...)` works identically
to `.explain()` on a DataFrame API chain.

## Common Table Expressions (CTEs) with WITH

```python
spark.sql("""
    WITH category_totals AS (
        SELECT category, SUM(amount) AS total
        FROM orders
        GROUP BY category
    )
    SELECT category, total
    FROM category_totals
    WHERE total > 100
""").show()
```

CTEs are a clean way to break a complex query into named, readable steps
— Spark SQL supports the standard `WITH name AS (...)` syntax, including
multiple CTEs separated by commas and CTEs referencing earlier CTEs.

## Mixing SQL and the DataFrame API

Because `spark.sql(...)` returns an ordinary DataFrame, you can freely
chain DataFrame methods onto a SQL query's result, or feed a
DataFrame-built result back into another SQL query via a fresh temp view:

```python
sql_result = spark.sql("SELECT category, SUM(amount) AS total FROM orders GROUP BY category")

# Continue with the DataFrame API:
from pyspark.sql.functions import round as spark_round
sql_result.withColumn("total_rounded", spark_round("total", 0)).show()

# Feed it back into another SQL query:
sql_result.createOrReplaceTempView("category_totals")
spark.sql("SELECT * FROM category_totals ORDER BY total DESC").show()
```

There is no performance penalty for switching between the two styles —
choose whichever is more readable for a given step. Analysts and
SQL-first teams often prefer `spark.sql(...)` for aggregation-heavy logic
and the DataFrame API for programmatic/parameterized pipeline code.

## Listing and inspecting views/tables

```python
spark.catalog.listTables()
# [Table(name='orders', database=None, description=None, tableType='TEMPORARY', isTemporary=True), ...]

spark.sql("DESCRIBE orders").show()
# +-----------+---------+-------+
# |   col_name|data_type|comment|
# +-----------+---------+-------+
# |   order_id|      int|   null|
# |customer_id|      int|   null|
# |   category|   string|   null|
# |     amount|   double|   null|
# +-----------+---------+-------+

spark.catalog.dropTempView("orders")
```

`spark.catalog` is the programmatic API for the same metadata SQL's
`SHOW TABLES` / `DESCRIBE` expose — useful for pipeline code that needs to
check what's registered before running dynamically-built queries.

## Worked example: parameterized SQL query

Task: build a query filtering by a threshold that comes from a Python
variable — never use raw Python string formatting/f-strings to inject
values into SQL (SQL-injection-shaped bug risk, and it defeats query plan
caching); use Spark SQL's parameter markers instead.

```python
# Spark 3.4+ style named parameters:
threshold = 100.0
spark.sql(
    "SELECT * FROM orders WHERE amount > :min_amount ORDER BY amount DESC",
    args={"min_amount": threshold},
).show()

# +--------+-----------+-----------+------+
# |order_id|customer_id|   category|amount|
# +--------+-----------+-----------+------+
# |       4|        103|electronics| 600.0|
# |       1|        101|electronics| 250.0|
# +--------+-----------+-----------+------+
```

`args={...}` binds Python values safely into the query without string
interpolation — prefer this pattern any time a query's filter values come
from outside the literal SQL text.

## Exercise

Using `orders` and `customers` from the top of this module:

1. Register both as temp views and write a SQL query returning each
   customer's total spend, ordered descending.
2. Write the same query (1) using the DataFrame API instead, and confirm
   both approaches give identical results.
3. Write a CTE-based SQL query that first computes per-category totals,
   then filters to categories above the overall average category total
   (hint: a second CTE or a subquery for the average).
4. Use `args={...}` parameter binding to filter `orders` by a
   `category` value supplied as a Python variable, rather than
   string-formatting it into the query text.
