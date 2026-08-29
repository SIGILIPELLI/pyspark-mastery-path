# 09 · Writing Data Out

!!! note "Not executed against a live cluster in this environment"
    Code below is hand-traced against documented PySpark reader/writer
    behavior, not run against a live cluster here.

Writing is the mirror image of reading (Module 5): `df.write` gives you a
`DataFrameWriter`, on which you configure format and options, then call
`.save(path)` or a format shortcut like `.parquet(path)` / `.csv(path)`.

## Basic write: Parquet

```python
df.write.parquet("output/orders_parquet")
```

Unlike `pandas.to_csv`, which writes exactly one file, this call writes a
**directory** named `output/orders_parquet/` containing one file per
partition (e.g. `part-00000-....snappy.parquet`, `part-00001-....parquet`,
...) plus a `_SUCCESS` marker file Spark writes when the job completes
without error. This is normal, expected Spark behavior — a Spark "output" is
almost always a directory of part-files that downstream Spark jobs read back
as a single logical DataFrame, not a single file meant for opening directly
in a spreadsheet.

## Save modes: what happens if the path already exists

By default, writing to a path that already exists **raises an error** — this
is a safety net against accidentally overwriting data. Control this with
`.mode(...)`:

```python
df.write.mode("overwrite").parquet("output/orders_parquet")   # replace existing data
df.write.mode("append").parquet("output/orders_parquet")      # add to existing data
df.write.mode("ignore").parquet("output/orders_parquet")       # skip silently if path exists
df.write.mode("error").parquet("output/orders_parquet")        # default: fail if path exists
```

| Mode | Behavior if the target already has data |
|---|---|
| `"error"` / `"errorifexists"` | Raise an exception (this is the default if `.mode()` isn't called) |
| `"overwrite"` | Delete existing data at the path first, then write |
| `"append"` | Add new files alongside existing ones |
| `"ignore"` | Do nothing, silently, if the path already exists |

`"overwrite"` is what most ETL scripts use for a full-refresh pattern;
`"append"` is what incremental/daily-batch pipelines use — Level 2 builds on
this with partition-aware incremental writes.

## Writing CSV and JSON

```python
df.write.mode("overwrite").csv("output/orders_csv", header=True)
df.write.mode("overwrite").json("output/orders_json")
```

Same directory-of-part-files behavior as Parquet. CSV output loses type
information the way CSV input does on read (Module 5/7) — everything comes
back out as untyped text if you read it again — which is one more reason
Parquet is generally preferred as an *intermediate* storage format between
pipeline stages, reserving CSV/JSON for final exports meant for humans or
external systems that specifically need those formats.

## Controlling the number of output files: repartition / coalesce

The number of files written equals the DataFrame's current number of
partitions. Too many small files (a common "small files problem" in Spark)
hurts downstream read performance, since each file has overhead to open and
read; too few large files limits write/read parallelism.

```python
# Reduce to a specific number of output files:
df.coalesce(1).write.mode("overwrite").parquet("output/single_file_dir")
# coalesce(1) merges everything into 1 partition -> 1 output file
# (only reasonable for small-to-moderate result sets; for a huge DataFrame
# this forces all data through one task, which can be slow or exhaust memory)

df.repartition(4).write.mode("overwrite").parquet("output/four_files")
# repartition(4) reshuffles data into exactly 4 partitions -> 4 output files
# (more expensive than coalesce because it always triggers a full shuffle,
# but can *increase* partition count too, unlike coalesce)
```

Rule of thumb introduced here (expanded in Level 2's partitioning module):
use `coalesce` to *reduce* partitions cheaply when you don't need an even
redistribution; use `repartition` when you need an even, specific number of
partitions, including increasing the count, accepting the shuffle cost.

## Partitioned writes: partitionBy

For large datasets queried by a recurring filter (very commonly a date),
writing with `partitionBy` creates a directory structure that lets future
reads skip irrelevant data entirely:

```python
df.write.mode("overwrite").partitionBy("country").parquet("output/orders_by_country")
```

This produces a directory layout like:

```text
output/orders_by_country/
├── country=US/
│   └── part-00000-....parquet
├── country=IN/
│   └── part-00000-....parquet
└── country=DE/
    └── part-00000-....parquet
```

Reading it back and filtering on the partition column lets Spark skip
reading the other directories entirely ("partition pruning"):

```python
df2 = spark.read.parquet("output/orders_by_country")
df2.filter(df2.country == "US").show()   # Spark only reads the country=US/ folder
```

Choose partition columns with moderate cardinality (few-to-moderate distinct
values, like `country` or a `date` truncated to day) — partitioning by a
high-cardinality column (like `customer_id` with millions of distinct
values) creates an enormous number of tiny directories/files, which hurts
performance instead of helping it.

## Worked example: a full read-transform-write cycle

```python
df = spark.read.csv("orders.csv", header=True, inferSchema=True)

transformed = (
    df.filter(df.amount > 0)                                    # drop bad/zero rows
      .withColumn("line_total", df.amount * df.quantity)
      .select("order_id", "customer", "country", "line_total")
)

(
    transformed
    .repartition(4)
    .write
    .mode("overwrite")
    .partitionBy("country")
    .parquet("output/clean_orders")
)

# Verify the write by reading it back
check = spark.read.parquet("output/clean_orders")
print(check.count())   # should match transformed.count()
```

Reading the output back and comparing row counts (as the last two lines do)
is a cheap, worthwhile sanity check any time you write a new pipeline —
it catches an entire class of silent write bugs immediately.

## Exercise

Given a DataFrame `sales_df` with columns `sale_id`, `region`, `product`,
`amount`, `sale_date`:

1. Write it to `"output/sales_full"` as Parquet, overwriting any existing
   data, partitioned by `region`.
2. Suppose the DataFrame currently has 400 tiny partitions from an earlier
   `groupBy`. Rewrite the write call to first reduce it to 8 output files
   per partition-folder, using the cheaper of `coalesce`/`repartition`
   (justify your choice in a sentence).
3. Write the same data as CSV with headers to `"output/sales_csv"`, using
   `"append"` mode instead of overwrite.
4. Explain, in one sentence, why reading `"output/sales_full"` back and
   filtering on `region` afterward is cheaper than filtering on `product`.

Answer for (2): `coalesce(8)` is the right choice, because you're only
*reducing* the partition count (400 → 8) and don't need a perfectly even
redistribution — `coalesce` avoids a full shuffle by merging existing
partitions locally, whereas `repartition(8)` would force an unnecessary full
shuffle to achieve the same reduction.

Answer for (4): filtering on `region` benefits from partition pruning — since
the data was written with `partitionBy("region")`, Spark can skip reading
entire folders for regions that don't match the filter, whereas `product`
isn't a partition column, so filtering on it still requires scanning every
partition's files.
