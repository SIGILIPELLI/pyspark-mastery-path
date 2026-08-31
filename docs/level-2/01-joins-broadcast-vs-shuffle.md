# 01 · Joins Broadcast Vs Shuffle

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Joins are the single most expensive operation you'll write in Spark, and
the difference between a broadcast join and a shuffle join is often the
difference between a job that finishes in seconds and one that spills to
disk for an hour. This module covers how Spark actually executes a join,
and how to steer it toward the cheap plan.

We'll use two small DataFrames to keep the traced output readable:

```python
orders = spark.createDataFrame(
    [
        (1, 101, 250.00),
        (2, 102, 75.50),
        (3, 101, 40.00),
        (4, 103, 500.00),
        (5, 104, 12.25),
    ],
    ["order_id", "customer_id", "amount"],
)

customers = spark.createDataFrame(
    [
        (101, "Alice", "US"),
        (102, "Bob", "IN"),
        (103, "Carla", "DE"),
    ],
    ["customer_id", "name", "country"],
)
```

Note `customer_id=104` has no match in `customers` — we'll use this to talk
about join types below.

## Why joins are expensive: the shuffle

A join needs every row from one side that shares a key to land on the same
executor as the matching rows from the other side. If the two DataFrames
are partitioned arbitrarily (which they are, right after a `read`), Spark
has to **shuffle** — repartition both sides by the join key across the
network — before it can compare rows partition-by-partition. This is the
default strategy, called a **sort-merge join** (or, for small hash tables,
a **shuffle hash join**), and it's expensive: network I/O, disk spill for
large partitions, and a stage boundary that blocks pipelining.

```python
result = orders.join(customers, on="customer_id", how="inner")
result.explain()

# == Physical Plan ==
# *(5) SortMergeJoin [customer_id#10], [customer_id#20], Inner
# :- *(2) Sort [customer_id#10 ASC NULLS FIRST], false, 0
# :  +- Exchange hashpartitioning(customer_id#10, 200), ...
# :     +- *(1) Filter isnotnull(customer_id#10)
# +- *(4) Sort [customer_id#20 ASC NULLS FIRST], false, 0
#    +- Exchange hashpartitioning(customer_id#20, 200), ...
#       +- *(3) Filter isnotnull(customer_id#20)
```

The two `Exchange hashpartitioning(...)` nodes are the shuffles — one per
side of the join. `explain()` is your primary tool for seeing whether Spark
chose a shuffle or a broadcast; we'll return to it in depth in Level 3.

## Broadcast joins: skip the shuffle entirely

If one side of the join is small enough to fit comfortably in each
executor's memory, Spark can instead **send a full copy of the small
DataFrame to every executor**, and each executor does a local hash-join
against its partition of the large side. No shuffle of the large side is
needed at all.

```python
from pyspark.sql.functions import broadcast

result = orders.join(broadcast(customers), on="customer_id", how="inner")
result.explain()

# == Physical Plan ==
# *(2) BroadcastHashJoin [customer_id#10], [customer_id#20], Inner, BuildRight
# :- *(2) Filter isnotnull(customer_id#10)
# +- BroadcastExchange HashedRelationBroadcastMode(...)
#    +- *(1) Filter isnotnull(customer_id#20)
```

`BroadcastHashJoin` replaces `SortMergeJoin`, and there's only one
`BroadcastExchange` (sending `customers` to every executor) instead of two
full shuffles. This is dramatically cheaper when `customers` is small — a
dimension table of a few thousand rows against a fact table of billions,
for example.

## Automatic broadcast: `spark.sql.autoBroadcastJoinThreshold`

You don't always have to call `broadcast()` explicitly. Spark's optimizer
will automatically pick a broadcast join if it estimates a table's size is
under a threshold, controlled by:

```python
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
# '10485760'   (10 MB, the default)

# Raise it if you know a larger dimension table is still safe to broadcast:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)  # 100 MB

# Disable auto-broadcast entirely (forces shuffle joins unless you call broadcast() manually):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

The size estimate comes from file statistics or a prior `.cache()`, and it
can be wrong — especially after several transformations. When in doubt,
call `broadcast()` explicitly rather than trusting the automatic threshold.

## Danger: broadcasting something too large

Broadcasting sends the *entire* DataFrame to *every* executor. If you
broadcast something too big, you can blow past executor memory and fail
the job with an `OutOfMemoryError`, or silently degrade performance as
executors spend most of their time serializing/deserializing a huge
broadcast var. Never wrap the large ("fact") side of a join in
`broadcast()` — only the small ("dimension") side.

```python
# WRONG — orders is the large fact table here, don't broadcast it
result = broadcast(orders).join(customers, on="customer_id")
```

## Join types recap, with `how=`

```python
orders.join(customers, on="customer_id", how="inner").show()
# +-----------+--------+------+-----+-------+
# |customer_id|order_id|amount| name|country|
# +-----------+--------+------+-----+-------+
# |        101|       1|250.00|Alice|     US|
# |        101|       3| 40.00|Alice|     US|
# |        102|       2| 75.50|  Bob|     IN|
# |        103|       4|500.00|Carla|     DE|
# +-----------+--------+------+-----+-------+
# (customer_id=104 dropped — no match)

orders.join(customers, on="customer_id", how="left").show()
# same 4 rows as inner, PLUS:
# |        104|       5| 12.25| null|   null|

customers.join(orders, on="customer_id", how="right").show()
# equivalent semantics to orders.join(customers, how="left") but with
# columns reordered per the base DataFrame

orders.join(customers, on="customer_id", how="left_anti").show()
# rows in orders with NO match in customers:
# +-----------+--------+------+
# |customer_id|order_id|amount|
# +-----------+--------+------+
# |        104|       5| 12.25|
# +-----------+--------+------+

orders.join(customers, on="customer_id", how="left_semi").show()
# rows in orders that DO have a match, but only orders' own columns —
# useful as an existence filter without duplicating matched rows
```

`left_semi` and `left_anti` are worth remembering: they answer "does a
match exist?" without the row-duplication risk of a regular join when the
right side has multiple matches per key.

## Worked example: enrich orders, flag orphans, pick the cheap plan

Task: join `orders` to `customers` to attach `name`/`country`, keep orders
without a matching customer (labelled `"UNKNOWN"`), and force a broadcast
since `customers` is a small dimension table.

```python
from pyspark.sql.functions import coalesce, lit, broadcast

enriched = (
    orders.join(broadcast(customers), on="customer_id", how="left")
          .withColumn("name", coalesce("name", lit("UNKNOWN")))
          .withColumn("country", coalesce("country", lit("UNKNOWN")))
          .orderBy("order_id")
)
enriched.show()

# +-----------+--------+------+-------+-------+
# |customer_id|order_id|amount|   name|country|
# +-----------+--------+------+-------+-------+
# |        101|       1|250.00|  Alice|     US|
# |        102|       2| 75.50|    Bob|     IN|
# |        101|       3| 40.00|  Alice|     US|
# |        103|       4|500.00|  Carla|     DE|
# |        104|       5| 12.25|UNKNOWN|UNKNOWN|
# +-----------+--------+------+-------+-------+
```

`enriched.explain()` would show `BroadcastHashJoin` here since we forced
it — confirm this is the plan you expect any time join performance matters.

## Exercise

Using `orders` and `customers` from the top of this module:

1. Write an inner join and count how many `orders` rows survive (expect 4).
2. Write a `left_anti` join to list orders with no matching customer
   (expect the `customer_id=104` row).
3. Explicitly broadcast `customers` and call `.explain()` on the result —
   confirm you see `BroadcastHashJoin` rather than `SortMergeJoin`.
4. Set `spark.sql.autoBroadcastJoinThreshold` to `-1`, re-run a plain
   (non-explicit-broadcast) join, and explain why the plan now falls back
   to a shuffle-based join even though `customers` is tiny.
