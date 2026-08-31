# 01 · Execution Plan Explain

!!! note "Not executed against a live cluster in this environment"
    Code and printed outputs below are hand-traced against documented PySpark
    behavior, not run against a live cluster here.

Every transformation you write is lazy — Spark builds up a logical plan and
only executes when an action (`.show()`, `.collect()`, `.write()`, ...)
forces it. `.explain()` is how you look inside that plan before you pay to
run it. This module is about reading those plans fluently: knowing which
line tells you about a shuffle, which tells you about a skipped scan, and
which tells you the optimizer made a choice you didn't expect.

## Setup

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.appName("explain-plans").getOrCreate()

orders = spark.createDataFrame(
    [(1, 101, "P1", 2), (2, 102, "P2", 1), (3, 101, "P1", 1), (4, 103, "P3", 5)],
    ["order_id", "customer_id", "product_id", "qty"],
)
customers = spark.createDataFrame(
    [(101, "Alice", "US"), (102, "Bob", "IN"), (103, "Carla", "DE")],
    ["customer_id", "name", "country"],
)
```

## The four plan stages

`.explain(True)` (or `mode="extended"`) prints all four stages the Catalyst
optimizer moves through:

```python
query = orders.filter(col("qty") > 1).join(customers, "customer_id")
query.explain(mode="extended")

# == Parsed Logical Plan ==
# Join Inner, (customer_id#10 = customer_id#20)
# :- Filter (qty#3 > 1)
# :  +- LogicalRDD [order_id#0, customer_id#10, product_id#2, qty#3]
# +- LogicalRDD [customer_id#20, name#21, country#22]
#
# == Analyzed Logical Plan ==
# customer_id: bigint, order_id: bigint, product_id: string, qty: bigint, name: string, country: string
# Join Inner, (customer_id#10 = customer_id#20)
# ... (types resolved, columns bound)
#
# == Optimized Logical Plan ==
# Join Inner, (customer_id#10 = customer_id#20)
# :- Filter (isnotnull(qty#3) AND (qty#3 > 1))
# :  +- LogicalRDD [...]
# +- Filter isnotnull(customer_id#20)
#    +- LogicalRDD [...]
#
# == Physical Plan ==
# *(2) BroadcastHashJoin [customer_id#10], [customer_id#20], Inner, BuildRight
# :- *(2) Filter (isnotnull(qty#3) AND (qty#3 > 1) AND isnotnull(customer_id#10))
# :  +- *(2) Scan ExistingRDD[...]
# +- BroadcastExchange HashedRelationBroadcastMode(...)
#    +- *(1) Filter isnotnull(customer_id#20)
#       +- *(1) Scan ExistingRDD[...]
```

Read these in order:

- **Parsed** — a literal translation of your DataFrame calls, unresolved
  (column names not yet checked against the schema).
- **Analyzed** — column references resolved and typed against the actual
  schema; this is where a typo in a column name would surface as an
  `AnalysisException`.
- **Optimized** — Catalyst has applied rule-based rewrites: predicate
  pushdown (`isnotnull` added automatically before the join), constant
  folding, and filter reordering.
- **Physical** — the actual execution strategy: note `BroadcastHashJoin`
  was chosen automatically here because `customers` is tiny, with no
  explicit `broadcast()` call needed.

## Reading a physical plan bottom-up

Physical plans execute bottom-up: the deepest node runs first. Star
prefixes like `*(2)` indicate whole-stage codegen — Spark fused several
operators into one compiled function for that stage number.

```python
agg = (
    orders.join(customers, "customer_id")
    .groupBy("country")
    .agg(spark_sum("qty").alias("total_qty"))
)
agg.explain()

# == Physical Plan ==
# *(3) HashAggregate(keys=[country#22], functions=[sum(qty#3)])
# +- Exchange hashpartitioning(country#22, 200), ...
#    +- *(2) HashAggregate(keys=[country#22], functions=[partial_sum(qty#3)])
#       +- *(2) BroadcastHashJoin [customer_id#10], [customer_id#20], Inner, BuildRight
#          :- *(2) Filter isnotnull(customer_id#10)
#          :  +- *(2) Scan ExistingRDD[...]
#          +- BroadcastExchange HashedRelationBroadcastMode(...)
#             +- *(1) Filter isnotnull(customer_id#20)
#                +- *(1) Scan ExistingRDD[...]
```

Note the **partial aggregation pattern**: `partial_sum` runs per-partition
before the `Exchange` (shuffle), then the final `HashAggregate` combines
the partial sums after data lands grouped by `country`. This halves the
data volume that has to cross the network compared to shuffling raw rows
first — Catalyst inserts this automatically, you don't write it yourself.

## `explain()` modes

```python
df = orders.join(customers, "customer_id")

df.explain()                    # physical plan only (default)
df.explain(mode="simple")       # same as default
df.explain(mode="extended")     # all four stages
df.explain(mode="codegen")      # generated Java source per WholeStageCodegen block
df.explain(mode="cost")         # physical plan annotated with estimated sizeInBytes/rowCount
df.explain(mode="formatted")    # physical plan with a separate numbered node index — most readable for wide plans
```

`mode="formatted"` is usually the best choice for a wide, deeply nested
plan — it splits the tree from the per-node detail so you aren't
scrolling past repeated boilerplate.

## Reading file-scan pruning

For file-backed sources, the scan node tells you whether partition
pruning and predicate pushdown actually engaged:

```python
events = spark.read.parquet("/data/events")  # partitioned by event_date
events.filter(col("event_date") == "2024-01-05").explain()

# == Physical Plan ==
# *(1) ColumnarToRow
# +- FileScan parquet [event_id#..,user_id#..,event_date#..] Batched: true,
#    DataFilters: [], Format: Parquet,
#    PartitionFilters: [isnotnull(event_date#..), (event_date#.. = 2024-01-05)],
#    PushedFilters: [], ReadSchema: struct<event_id:bigint,user_id:bigint>
```

`PartitionFilters` populated (not `DataFilters`) confirms Spark pruned to
only the `event_date=2024-01-05` partition directory on disk — it never
opened the other partitions' files at all.

## Worked example: spotting an accidental shuffle join

```python
big_orders = orders.withColumnRenamed("customer_id", "cust_id")
suspect = big_orders.join(customers, big_orders.cust_id == customers.customer_id)
suspect.explain()

# == Physical Plan ==
# *(2) BroadcastHashJoin [cust_id#..], [customer_id#20], Inner, BuildRight
```

Even after a rename, Spark still recognizes `customers` as broadcastable —
the join key naming doesn't matter to the optimizer, only estimated size
does. If this instead showed `SortMergeJoin`, that would be the signal to
check `spark.sql.autoBroadcastJoinThreshold` or force `broadcast()`
explicitly (Level 2, module 1).

## Exercise

1. Build a three-way join (`orders` → `customers` → a third small
   DataFrame of your own) and run `.explain(mode="formatted")`. Identify
   every `Exchange` node and explain in one sentence why each one exists.
2. Take a `.filter()` placed *after* a `.groupBy().agg()` and compare its
   `Optimized Logical Plan` against the same filter placed *before* the
   aggregation — confirm whether Catalyst pushed the filter down for you.
3. Using a partitioned Parquet directory (real or hypothetical), write a
   query whose `explain()` output shows `PartitionFilters` populated, and
   one whose filter is on a non-partition column, showing up instead
   under `PushedFilters`/`DataFilters`.
