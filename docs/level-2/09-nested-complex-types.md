# 09 · Nested Complex Types

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Real-world data — JSON APIs, event logs, semi-structured exports — is
rarely flat. PySpark models this with three complex types: `ArrayType`,
`MapType`, and `StructType`. This module covers constructing, querying,
and flattening all three.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, DoubleType

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer", StructType([
        StructField("name", StringType()),
        StructField("country", StringType()),
    ])),
    StructField("items", ArrayType(StructType([
        StructField("sku", StringType()),
        StructField("qty", IntegerType()),
        StructField("price", DoubleType()),
    ]))),
    StructField("tags", ArrayType(StringType())),
    StructField("metadata", MapType(StringType(), StringType())),
])

data = [
    (1, ("Alice", "US"),
        [("SKU-1", 2, 9.99), ("SKU-2", 1, 24.50)],
        ["priority", "gift"],
        {"channel": "web", "campaign": "summer24"}),
    (2, ("Bob", "IN"),
        [("SKU-3", 5, 3.00)],
        [],
        {"channel": "mobile"}),
]
df = spark.createDataFrame(data, schema)
df.printSchema()

# root
#  |-- order_id: integer (nullable = true)
#  |-- customer: struct (nullable = true)
#  |    |-- name: string (nullable = true)
#  |    |-- country: string (nullable = true)
#  |-- items: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- sku: string (nullable = true)
#  |    |    |-- qty: integer (nullable = true)
#  |    |    |-- price: double (nullable = true)
#  |-- tags: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- metadata: map (nullable = true)
#  |    |-- value: string (valueContainsNull = true)
```

## StructType: dot-access into nested fields

```python
df.select("order_id", "customer.name", "customer.country").show()

# +--------+-----+-------+
# |order_id| name|country|
# +--------+-----+-------+
# |       1|Alice|     US|
# |       2|  Bob|     IN|
# +--------+-----+-------+
```

`"customer.name"` dotted-path syntax works directly in `.select()` and
SQL. You can also use `col("customer.name")` or `col("customer")["name"]`
(bracket access) interchangeably.

## Flattening a struct into top-level columns

```python
flat = df.select(
    "order_id",
    "customer.name",
    "customer.country",
    "tags",
    "metadata",
)
```

For a struct with many fields, `.select("order_id", "customer.*")` expands
every field of `customer` as its own top-level column without naming each
one:

```python
df.select("order_id", "customer.*").show()
# +--------+-----+-------+
# |order_id| name|country|
# +--------+-----+-------+
# |       1|Alice|     US|
# |       2|  Bob|     IN|
# +--------+-----+-------+
```

## ArrayType: indexing, size, and element functions

```python
from pyspark.sql.functions import size, array_contains, element_at

df.select(
    "order_id",
    size("tags").alias("tag_count"),
    array_contains("tags", "gift").alias("is_gift"),
    element_at("tags", 1).alias("first_tag"),
).show()

# +--------+---------+-------+---------+
# |order_id|tag_count|is_gift|first_tag|
# +--------+---------+-------+---------+
# |       1|        2|   true| priority|
# |       2|        0|  false|     null|
# +--------+---------+-------+---------+
```

`element_at(arr, 1)` uses **1-based** indexing (unlike Python's 0-based
indexing) and returns `null` for an out-of-range index rather than
raising an error — important when arrays can be empty, as `tags` is for
order 2.

## explode: array rows to multiple output rows

The most important array operation: `explode()` turns each element of an
array into its own output row, duplicating the other columns.

```python
from pyspark.sql.functions import explode

df.select("order_id", explode("items").alias("item")).select(
    "order_id", "item.sku", "item.qty", "item.price"
).show()

# +--------+-----+---+-----+
# |order_id|  sku|qty|price|
# +--------+-----+---+-----+
# |       1|SKU-1|  2| 9.99|
# |       1|SKU-2|  1| 24.5|
# |       2|SKU-3|  5|  3.0|
# +--------+-----+---+-----+
```

Order 1's two items became two rows, both carrying `order_id=1` — this is
the standard "line items" flattening pattern for any order/basket-shaped
event data. Note: `explode()` on an **empty** array drops the row entirely
(order 2's empty `tags` array, if exploded, would produce zero rows for
that order) — use `explode_outer()` instead if you need to keep rows with
empty/null arrays (producing a row with `null` in the exploded column).

```python
from pyspark.sql.functions import explode_outer

df.select("order_id", explode_outer("tags").alias("tag")).show()
# order_id=2's empty tags array now produces ONE row with tag=null,
# rather than being dropped
```

## MapType: keys, values, and lookups

```python
from pyspark.sql.functions import map_keys, map_values

df.select(
    "order_id",
    map_keys("metadata").alias("meta_keys"),
    map_values("metadata").alias("meta_values"),
    df.metadata["channel"].alias("channel"),
).show()

# +--------+--------------------+----------------+-------+
# |order_id|           meta_keys|     meta_values|channel|
# +--------+--------------------+----------------+-------+
# |       1|[channel, campaign]|[web, summer24] |    web|
# |       2|           [channel]|        [mobile]| mobile|
# +--------+--------------------+----------------+-------+
```

`df.metadata["channel"]` (or `col("metadata")["channel"]`, or
`col("metadata").getItem("channel")`) looks up a specific key, returning
`null` if that key isn't present in a given row's map (as would happen for
order 2 if it lacked a `"campaign"` key, which it does).

## Building nested structures: struct() and array()

Going the other direction — constructing complex types from flat columns:

```python
from pyspark.sql.functions import struct, array, create_map, lit

flat_orders = spark.createDataFrame(
    [(1, "Alice", "US"), (2, "Bob", "IN")], ["order_id", "name", "country"]
)

nested = flat_orders.select(
    "order_id",
    struct("name", "country").alias("customer"),
    array(lit("web")).alias("tags"),
    create_map(lit("source"), lit("api")).alias("metadata"),
)
nested.printSchema()
```

`struct(...)` groups columns into a single struct-typed column;
`array(...)` builds a literal array; `create_map(k1, v1, k2, v2, ...)`
builds a map from alternating key/value expressions.

## Worked example: line-item revenue report

Task: from `df`, compute total revenue per order (summing `qty * price`
across `items`), and separately, a flat "one row per line item" table with
customer info attached.

```python
from pyspark.sql.functions import sum as spark_sum, expr

# Per-order total using higher-order array function (no explode needed):
order_totals = df.select(
    "order_id",
    expr("aggregate(items, 0D, (acc, x) -> acc + x.qty * x.price)").alias("order_total"),
)
order_totals.show()

# +--------+-----------+
# |order_id|order_total|
# +--------+-----------+
# |       1|      44.48|
# |       2|       15.0|
# +--------+-----------+

# Flat line-item table with customer info:
line_items = (
    df.select("order_id", "customer.name", explode("items").alias("item"))
      .select("order_id", "name", "item.sku", "item.qty", "item.price")
)
line_items.show()
```

`expr("aggregate(items, 0D, (acc, x) -> acc + x.qty * x.price)")` uses
Spark SQL's higher-order `aggregate` function directly on the array,
avoiding an `explode` + `groupBy` round trip when you only need a
per-row scalar summary rather than a flattened table.

## Exercise

Using `df` from the top of this module:

1. Flatten `customer` into top-level `name`/`country` columns using the
   `.*` shorthand.
2. Explode `items` and compute total quantity sold per `sku` across both
   orders (hint: explode, then `groupBy("item.sku")`).
3. Use `map_keys`/`map_values` to find which orders have a `"campaign"`
   key in `metadata` (hint: `array_contains(map_keys("metadata"),
   "campaign")`).
4. Build a brand-new DataFrame from flat columns (`order_id`, `sku`,
   `qty`, `price`) and reconstruct it into the original nested
   `items: array<struct<...>>` shape per `order_id` (hint: `groupBy` +
   `collect_list(struct(...))`).
