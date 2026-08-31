# 10 · Capstone Multi Source Join

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

This capstone pulls together everything from Level 2: joins, window
functions, UDFs (and avoiding them), partitioning, caching, Spark SQL, null
handling, and nested types. You'll build a small pipeline that combines
three independent data sources — orders, customers, and a product catalog
with nested pricing — into one enriched, analysis-ready table.

## The sources

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)

spark = SparkSession.builder.appName("multi-source-capstone").getOrCreate()

orders = spark.createDataFrame(
    [
        (1, 101, "P1", 2, "2024-01-05"),
        (2, 102, "P2", 1, "2024-01-05"),
        (3, 101, "P1", 1, "2024-01-06"),
        (4, 103, "P3", 5, None),
        (5, 104, "P2", 3, "2024-01-07"),
        (6, 101, "P4", 1, "2024-01-08"),
    ],
    ["order_id", "customer_id", "product_id", "qty", "order_date"],
)

customers = spark.createDataFrame(
    [
        (101, "Alice", "US"),
        (102, "Bob", "IN"),
        (103, "Carla", "DE"),
    ],
    ["customer_id", "name", "country"],
)

# Product catalog has a nested "pricing" struct and a "tags" array —
# a realistic shape for a document-store export landing as JSON/Parquet.
products_schema = StructType([
    StructField("product_id", StringType()),
    StructField("title", StringType()),
    StructField("pricing", StructType([
        StructField("unit_price", DoubleType()),
        StructField("currency", StringType()),
    ])),
    StructField("tags", ArrayType(StringType())),
])

products = spark.createDataFrame(
    [
        ("P1", "Wireless Mouse", (19.99, "USD"), ["electronics", "accessory"]),
        ("P2", "USB-C Hub", (34.50, "USD"), ["electronics"]),
        ("P3", "Standing Desk", (299.00, "USD"), ["furniture", "office"]),
    ],
    schema=products_schema,
)
```

Note the deliberate messiness: `orders.order_date` has a null, `P4` has no
catalog entry, and `customer_id=104` has no matching customer.

## Step 1 — cache the small dimension, broadcast it into the join

`customers` and `products` are both small dimension tables reused across
multiple joins, so we cache them once and broadcast at join time.

```python
from pyspark.sql.functions import broadcast

customers.cache()
products.cache()
customers.count()   # materialize the cache
products.count()

enriched = (
    orders
    .join(broadcast(customers), on="customer_id", how="left")
    .join(broadcast(products), on="product_id", how="left")
)
```

## Step 2 — flatten the nested pricing struct and clean nulls

```python
from pyspark.sql.functions import col, coalesce, lit, round as spark_round

flat = (
    enriched
    .withColumn("unit_price", col("pricing.unit_price"))
    .withColumn("currency", coalesce(col("pricing.currency"), lit("USD")))
    .drop("pricing")
    .withColumn("name", coalesce(col("name"), lit("UNKNOWN")))
    .withColumn("country", coalesce(col("country"), lit("UNKNOWN")))
    .withColumn("title", coalesce(col("title"), lit("UNKNOWN PRODUCT")))
    .withColumn("unit_price", coalesce(col("unit_price"), lit(0.0)))
    .withColumn("line_total", spark_round(col("unit_price") * col("qty"), 2))
)
```

`order_id=6` (product `P4`, no catalog match) now shows `title=UNKNOWN
PRODUCT`, `unit_price=0.0`, `line_total=0.0` instead of nulls propagating
silently through downstream aggregates.

## Step 3 — window function: rank each customer's orders by spend

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

customer_window = Window.partitionBy("customer_id").orderBy(col("line_total").desc())

ranked = flat.withColumn("spend_rank", rank().over(customer_window))
ranked.select("customer_id", "name", "order_id", "line_total", "spend_rank").orderBy("customer_id", "spend_rank").show()

# +-----------+-----+--------+----------+----------+
# |customer_id| name|order_id|line_total|spend_rank|
# +-----------+-----+--------+----------+----------+
# |        101|Alice|       1|     39.98|         1|
# |        101|Alice|       3|     19.99|         2|
# |        101|Alice|       6|       0.0|         3|
# |        102|  Bob|       2|      34.5|         1|
# |        103|Carla|       4|       0.0|         1|
# |        104|UNKNOWN|     5|       0.0|         1|
# +-----------+-----+--------+----------+----------+
```

## Step 4 — a small UDF only where the DataFrame API can't help, wrapped carefully

Tagging an order as "premium" combines multiple array-membership and
threshold checks that would be awkward as native expressions here, but we
prefer `array_contains` (native) over a Python UDF wherever possible —
only the final composite label uses a UDF, and even then a `pandas_udf`
for vectorization:

```python
from pyspark.sql.functions import array_contains, pandas_udf
import pandas as pd

has_electronics = array_contains(col("tags"), "electronics")

@pandas_udf("string")
def spend_tier(line_total: pd.Series) -> pd.Series:
    return pd.cut(
        line_total.fillna(0.0),
        bins=[-0.01, 0, 25, 1000],
        labels=["none", "standard", "premium"],
    ).astype(str)

tiered = (
    ranked
    .withColumn("is_electronics", coalesce(has_electronics, lit(False)))
    .withColumn("spend_tier", spend_tier(col("line_total")))
)
```

## Step 5 — register as a temp view and finish the aggregation in SQL

```python
tiered.createOrReplaceTempView("enriched_orders")

summary = spark.sql("""
    SELECT
        customer_id,
        name,
        country,
        COUNT(*)                    AS order_count,
        ROUND(SUM(line_total), 2)   AS total_spend,
        SUM(CASE WHEN spend_tier = 'premium' THEN 1 ELSE 0 END) AS premium_orders
    FROM enriched_orders
    GROUP BY customer_id, name, country
    ORDER BY total_spend DESC
""")
summary.show()

# +-----------+-------+-------+-----------+-----------+--------------+
# |customer_id|   name|country|order_count|total_spend|premium_orders|
# +-----------+-------+-------+-----------+-----------+--------------+
# |        101|  Alice|     US|          3|      59.97|             1|
# |        102|    Bob|     IN|          1|       34.5|             1|
# |        103|  Carla|     DE|          1|        0.0|             0|
# |        104|UNKNOWN|UNKNOWN|          1|        0.0|             0|
# +-----------+-------+-------+-----------+-----------+--------------+
```

## Step 6 — partition the write for downstream consumers

Since `country` is a low-cardinality, frequently-filtered column, we
partition the final write on it:

```python
(
    tiered
    .repartition("country")
    .write
    .mode("overwrite")
    .partitionBy("country")
    .parquet("/data/warehouse/enriched_orders")
)

customers.unpersist()
products.unpersist()
```

Partitioning by `country` means a downstream query filtering
`WHERE country = 'US'` prunes to a single partition directory instead of
scanning the whole dataset — the payoff for the extra `repartition` here.

## Exercise

1. Add a fourth dimension source — a `promotions` DataFrame keyed by
   `product_id` with a `discount_pct` column — and join it in, applying
   the discount to `line_total` only where a promotion exists.
2. Replace the `pandas_udf` spend-tier logic with an equivalent
   `CASE WHEN` expression using native `when()/otherwise()` calls, and
   compare `.explain()` output between the two versions.
3. Re-run Step 6 but partition by `country` **and** the month extracted
   from `order_date` — handle the row with a null `order_date` explicitly
   rather than letting it silently create a `null` partition folder.
